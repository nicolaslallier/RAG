"""API HTTP : /sante (sans jeton) et /chercher (jeton → niveaux de visibilité)."""

import hmac
import json
from dataclasses import asdict
from datetime import date
from typing import Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from rag.aiguillage import aiguiller
from rag.config import PUBLIC, TOUT, Reglages, charger_config, empreinte_indexation
from rag.db import connexion
from rag.entretien import doublons, liens_entrants, orphelines
from rag.graphe import chemin, expliquer
from rag.lister import lister, taxonomie
from rag.modeles import ErreurModele, modeles_depuis
from rag.recherche import Filtres, chercher
from rag.reponse import repondre, repondre_flux
from rag.rerank import reranker_depuis


class FiltresEntree(BaseModel):
    model_config = ConfigDict(extra="forbid")
    sources: list[str] | None = None
    dossier: str | None = None
    tags: list[str] | None = None
    type: str | None = None
    depuis: date | None = None
    jusqua: date | None = None


class DemandeChemin(BaseModel):
    model_config = ConfigDict(extra="forbid")
    de: str = Field(max_length=500)
    vers: str = Field(max_length=500)
    max_sauts: int = Field(6, ge=1, le=10)


class DemandeExpliquer(BaseModel):
    model_config = ConfigDict(extra="forbid")
    nom: str = Field(max_length=500)
    limite: int = Field(20, ge=1, le=100)


class Demande(BaseModel):
    question: str = Field(max_length=2000)
    filtres: FiltresEntree = FiltresEntree()
    limite: int = Field(10, ge=1, le=50)


class DemandeReindexer(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source: str | None = Field(None, max_length=200)


class DemandeQuestion(BaseModel):
    model_config = ConfigDict(extra="forbid")
    question: str = Field(max_length=2000)
    mode: Literal["lister", "repondre"] | None = None
    filtres: FiltresEntree | None = None
    flux: bool = False


class DemandeLister(BaseModel):
    model_config = ConfigDict(extra="forbid")
    question: str = Field(max_length=2000)
    filtres: FiltresEntree = FiltresEntree()


class DemandeRepondre(DemandeLister):
    flux: bool = False


def _sse(evenements):
    for e in evenements:
        yield f"data: {json.dumps(e, ensure_ascii=False, default=str)}\n\n"


def version_id(conn, empreinte: str) -> int | None:
    r = conn.execute("SELECT id FROM index_versions WHERE empreinte = %s", (empreinte,)).fetchone()
    return r["id"] if r else None


def create_app(reglages: Reglages | None = None, modeles=None, reranker=None) -> FastAPI:
    reglages = reglages or Reglages()
    if not reglages.jeton:
        raise RuntimeError("RAG_JETON est vide : l'API refuse de démarrer sans jeton")
    config = charger_config(reglages.config)
    empreinte = empreinte_indexation(config.indexation)
    modeles = modeles or modeles_depuis(reglages)
    reranker = reranker or reranker_depuis(config, reglages)  # chargé au 1er appel, dans un fil
    app = FastAPI(title="RAG maître")

    @app.exception_handler(ErreurModele)
    def _modele_indisponible(request: Request, e: ErreurModele):
        return JSONResponse({"detail": "modèle indisponible"}, status_code=503)

    def niveaux_du_jeton(authorization: str = Header(default="")) -> frozenset[str]:
        if not authorization.startswith("Bearer "):
            raise HTTPException(401, "jeton invalide")
        jeton = authorization.removeprefix("Bearer ").strip().encode()
        if jeton and hmac.compare_digest(jeton, reglages.jeton.encode()):
            return PUBLIC
        if jeton and reglages.jeton_prive and hmac.compare_digest(
                jeton, reglages.jeton_prive.encode()):
            return TOUT
        raise HTTPException(401, "jeton invalide")

    def jeton_reindexer(authorization: str = Header(default="")) -> None:
        if not reglages.jeton_reindexer:
            raise HTTPException(503, "réindexation non configurée")
        if not authorization.startswith("Bearer "):
            raise HTTPException(401, "jeton invalide")
        jeton = authorization.removeprefix("Bearer ").strip().encode()
        if jeton and hmac.compare_digest(jeton, reglages.jeton_reindexer.encode()):
            return
        niveaux_du_jeton(authorization)  # jeton de lecture valide : 403, sinon 401
        raise HTTPException(403, "ce jeton ne permet pas de réindexer")

    @app.post("/reindexer", status_code=202, dependencies=[Depends(jeton_reindexer)])
    def route_reindexer(d: DemandeReindexer):
        with connexion(reglages.database_url) as conn:
            noms = [r["nom"] for r in conn.execute(
                "UPDATE sources SET demande_le = now() WHERE %(s)s::text IS NULL OR nom = %(s)s "
                "RETURNING nom", {"s": d.source})]
            if d.source and not noms:
                raise HTTPException(404, "source inconnue")
            conn.execute("NOTIFY rag_reindexer")
            conn.commit()
        return {"demande": sorted(noms)}

    @app.get("/etat")
    def route_etat(niveaux: frozenset[str] = Depends(niveaux_du_jeton)):
        with connexion(reglages.database_url) as conn:
            sources = conn.execute(
                """SELECT s.nom, s.type, s.revision, s.indexe_le,
                          (SELECT count(*) FROM documents d WHERE d.source_id = s.id) AS documents,
                          s.demande_le IS NOT NULL AS demande_en_attente
                     FROM sources s WHERE s.visibilite = ANY(%s) ORDER BY s.nom""",
                (sorted(niveaux),)).fetchall()
            en_cours = conn.execute("SELECT genre, debut FROM passes WHERE fin IS NULL "
                                    "ORDER BY debut DESC LIMIT 1").fetchone()
            nuit = conn.execute("SELECT debut, fin, ok, bilan FROM passes WHERE genre = "
                                "'nocturne' AND fin IS NOT NULL ORDER BY debut DESC LIMIT 1"
                                ).fetchone()
            caches = {r["nom"] for r in conn.execute(
                "SELECT nom FROM sources WHERE NOT visibilite = ANY(%s)", (sorted(niveaux),))}
        if nuit:  # le bilan ne nomme pas une source que ce jeton ne voit pas
            nuit["bilan"] = {k: v for k, v in nuit["bilan"].items() if k not in caches}
        return {"sources": sources, "en_cours": en_cours, "derniere_nocturne": nuit}

    @app.get("/sante")
    def sante():
        try:
            with connexion(reglages.database_url) as conn:
                n = conn.execute("SELECT count(*) n FROM documents").fetchone()["n"]
        except Exception:  # noqa: BLE001 - la santé répond 503, sans détail
            return JSONResponse({"ok": False}, status_code=503)
        return {"ok": True, "documents": n, "version": empreinte}

    @app.post("/chercher")
    def route_chercher(d: Demande, niveaux: frozenset[str] = Depends(niveaux_du_jeton)):
        with connexion(reglages.database_url) as conn:
            v = version_id(conn, empreinte)
            if v is None:
                return {"resultats": []}
            r = chercher(conn, d.question, config.recherche, modeles, v,
                         config.indexation.modele_embedding, niveaux=niveaux,
                         filtres=Filtres(**d.filtres.model_dump()), limite=d.limite,
                         reranker=reranker)
        return {"resultats": r}

    def _lister(conn, question: str, f: Filtres, niveaux) -> dict:
        v = version_id(conn, empreinte)
        if v is None:
            return {"mode": "regroupement", "filtres": asdict(f), "elements": [], "total": 0}
        return lister(conn, question, f, config, modeles, v, niveaux=niveaux, reranker=reranker)

    def _repondre(question: str, f: Filtres, niveaux, flux: bool, avant=()):
        if flux:
            def evenements():
                yield from avant
                with connexion(reglages.database_url) as conn:
                    v = version_id(conn, empreinte) or 0
                    try:
                        yield from repondre_flux(conn, question, config, modeles, v,
                                                 niveaux=niveaux, filtres=f, reranker=reranker)
                    except ErreurModele:  # en plein flux : plus de code HTTP possible
                        yield {"type": "erreur", "detail": "modèle indisponible"}
            return StreamingResponse(_sse(evenements()), media_type="text/event-stream")
        with connexion(reglages.database_url) as conn:
            v = version_id(conn, empreinte) or 0
            return asdict(repondre(conn, question, config, modeles, v, niveaux=niveaux,
                                   filtres=f, reranker=reranker))

    @app.post("/lister")
    def route_lister(d: DemandeLister, niveaux: frozenset[str] = Depends(niveaux_du_jeton)):
        with connexion(reglages.database_url) as conn:
            return _lister(conn, d.question, Filtres(**d.filtres.model_dump()), niveaux)

    @app.post("/repondre")
    def route_repondre(d: DemandeRepondre, niveaux: frozenset[str] = Depends(niveaux_du_jeton)):
        return _repondre(d.question, Filtres(**d.filtres.model_dump()), niveaux, d.flux)

    @app.post("/question")
    def route_question(d: DemandeQuestion, niveaux: frozenset[str] = Depends(niveaux_du_jeton)):
        with connexion(reglages.database_url) as conn:
            dec = aiguiller(d.question, taxonomie(conn, niveaux=niveaux), modeles,
                            config.reponse.modele, d.mode,
                            Filtres(**d.filtres.model_dump()) if d.filtres else None)
            tete = {"mode": dec.mode, "aiguillage": {"par": dec.par,
                                                     "filtres": asdict(dec.filtres)}}
            if dec.mode == "lister":  # le « mode » de lister (metadonnees…) devient mode_liste
                corps = _lister(conn, d.question, dec.filtres, niveaux)
                return tete | {"mode_liste": corps.pop("mode")} | corps
        if d.flux:
            return _repondre(d.question, dec.filtres, niveaux, True,
                             [{"type": "aiguillage"} | tete])
        return tete | _repondre(d.question, dec.filtres, niveaux, False)

    @app.post("/chemin")
    def route_chemin(d: DemandeChemin, niveaux: frozenset[str] = Depends(niveaux_du_jeton)):
        with connexion(reglages.database_url) as conn:
            return chemin(conn, d.de, d.vers, d.max_sauts, niveaux=niveaux)

    @app.post("/expliquer")
    def route_expliquer(d: DemandeExpliquer, niveaux: frozenset[str] = Depends(niveaux_du_jeton)):
        with connexion(reglages.database_url) as conn:
            return expliquer(conn, d.nom, d.limite, niveaux=niveaux)

    @app.get("/doublons")
    def route_doublons(seuil: float = Query(0.92, ge=0.5, le=1.0),
                       limite: int = Query(50, ge=1, le=200),
                       niveaux: frozenset[str] = Depends(niveaux_du_jeton)):
        with connexion(reglages.database_url) as conn:
            v = version_id(conn, empreinte)
            return {"doublons": [] if v is None else
                    doublons(conn, v, seuil, limite, niveaux=niveaux)}

    @app.get("/orphelines")
    def route_orphelines(niveaux: frozenset[str] = Depends(niveaux_du_jeton)):
        with connexion(reglages.database_url) as conn:
            return {"orphelines": orphelines(conn, niveaux=niveaux)}

    @app.get("/liens-entrants")
    def route_liens_entrants(cle: str = Query(max_length=1000),
                             niveaux: frozenset[str] = Depends(niveaux_du_jeton)):
        with connexion(reglages.database_url) as conn:
            return liens_entrants(conn, cle, niveaux=niveaux)

    return app
