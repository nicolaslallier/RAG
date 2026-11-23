"""API HTTP : /sante (sans jeton) et /chercher (jeton → niveaux de visibilité)."""

import asyncio
import hmac
import json
import logging
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import date
from types import SimpleNamespace
from typing import Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field
from starlette.routing import Route

from rag import gestion
from rag.aiguillage import aiguiller, sources_citees
from rag.config import (
    LIMITE_MAX,
    NOM_MAX,
    PUBLIC,
    QUESTION_MAX,
    TOUT,
    Reglages,
    charger_config,
    empreinte_indexation,
)
from rag.db import connexion
from rag.entretien import doublons, liens_entrants, orphelines
from rag.graphe import chemin, expliquer
from rag.lister import lister, taxonomie
from rag.mcp_serveur import creer_mcp
from rag.modeles import ErreurModele, modeles_depuis
from rag.recherche import Filtres, chercher
from rag.reponse import repondre, repondre_flux
from rag.rerank import reranker_depuis
from rag.sources_gerees import coffret_depuis
from rag.ui import STATIQUE, routeur_ui, validation_sans_cache

log = logging.getLogger("rag.api")


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
    de: str = Field(max_length=NOM_MAX)
    vers: str = Field(max_length=NOM_MAX)
    max_sauts: int = Field(6, ge=1, le=10)


class DemandeExpliquer(BaseModel):
    model_config = ConfigDict(extra="forbid")
    nom: str = Field(max_length=NOM_MAX)
    limite: int = Field(20, ge=1, le=100)


class Demande(BaseModel):
    question: str = Field(max_length=QUESTION_MAX)
    filtres: FiltresEntree = FiltresEntree()
    limite: int = Field(10, ge=1, le=LIMITE_MAX)


class DemandeReindexer(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source: str | None = Field(None, max_length=200)


class DemandeQuestion(BaseModel):
    model_config = ConfigDict(extra="forbid")
    question: str = Field(max_length=QUESTION_MAX)
    mode: Literal["lister", "repondre"] | None = None
    filtres: FiltresEntree | None = None
    flux: bool = False


class DemandeLister(BaseModel):
    model_config = ConfigDict(extra="forbid")
    question: str = Field(max_length=QUESTION_MAX)
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
    mcp = creer_mcp(SimpleNamespace(
        niveaux=lambda a: niveaux_de(a) or frozenset(),
        chercher=lambda q, f, limite, n: _chercher(q, f, limite, n),
        lister=lambda q, f, n: _question_lister(q, f, n),
        repondre=lambda q, n: _repondre_simple(q, n),
        chemin=lambda de, vers, n: _avec_conn(chemin, de, vers, niveaux=n),
        expliquer=lambda nom, n: _avec_conn(expliquer, nom, niveaux=n),
        sources=lambda n: _avec_conn(lambda c: {"sources": gestion.sources(c, niveaux=n),
                                                "en_cours": gestion.passe_en_cours(c)}),
        fichiers=lambda source, e, contient, page, n: _avec_conn(
            gestion.fichiers, source, niveaux=n, etat=e, contient=contient, page=page)))
    mcp_asgi = mcp.streamable_http_app().routes[0].endpoint

    @asynccontextmanager
    async def cycle(app):
        nonlocal config, reranker
        async with mcp.session_manager.run():
            if reranker is not None:  # charge le modèle hors de la boucle d'événements
                try:
                    await asyncio.to_thread(reranker.noter, "x", ["x"])
                except Exception:  # téléchargement raté : l'API démarre, rerank désactivé
                    log.exception("rerank indisponible : désactivé (refus au cosinus)")
                    rr = config.recherche.rerank.model_copy(update={"actif": False})
                    config = config.model_copy(update={"recherche": config.recherche.model_copy(
                        update={"rerank": rr})})
                    reranker = None
            yield

    app = FastAPI(title="RAG maître", lifespan=cycle, docs_url=None, redoc_url=None,
                  openapi_url=None)

    class RouteMcp:  # application ASGI (une instance, pas une fonction) : 401 avant le MCP
        async def __call__(self, scope, receive, send):
            entetes = dict(scope["headers"])
            if niveaux_de(entetes.get(b"authorization", b"").decode("latin-1")) is None:
                await JSONResponse({"detail": "jeton invalide"}, status_code=401)(
                    scope, receive, send)
                return
            await mcp_asgi(scope, receive, send)

    app.router.routes.append(Route("/mcp", endpoint=RouteMcp(), methods=["GET", "POST", "DELETE"]))

    def _avec_conn(f, *a, **k):
        with connexion(reglages.database_url) as conn:
            return f(conn, *a, **k)

    def _chercher(question, f, limite, niveaux):
        with connexion(reglages.database_url) as conn:
            v = version_id(conn, empreinte)
            return [] if v is None else chercher(
                conn, question, config.recherche, modeles, v, config.indexation.modele_embedding,
                niveaux=niveaux, filtres=f, limite=limite, reranker=reranker)

    def _question_lister(question, f, niveaux):
        with connexion(reglages.database_url) as conn:
            if f is None:
                f = aiguiller(question, taxonomie(conn, niveaux=niveaux), modeles,
                              config.reponse.modele, "lister").filtres
            return _lister(conn, question, f, niveaux)

    def _repondre_simple(question, niveaux):
        with connexion(reglages.database_url) as conn:
            v = version_id(conn, empreinte) or 0
            return repondre(conn, question, config, modeles, v, niveaux=niveaux,
                            reranker=reranker, bonus_sources=_bonus(conn, question, niveaux))

    def _bonus(conn, question, niveaux):  # source nommée : bonus de score (config mesurée)
        return sources_citees(question, taxonomie(conn, niveaux=niveaux))

    @app.exception_handler(ErreurModele)
    def _modele_indisponible(request: Request, e: ErreurModele):
        return JSONResponse({"detail": "modèle indisponible"}, status_code=503)

    def niveaux_de(authorization: str) -> frozenset[str] | None:
        if not authorization.startswith("Bearer "):
            return None
        jeton = authorization.removeprefix("Bearer ").strip().encode()
        if jeton and hmac.compare_digest(jeton, reglages.jeton.encode()):
            return PUBLIC
        if jeton and reglages.jeton_prive and hmac.compare_digest(
                jeton, reglages.jeton_prive.encode()):
            return TOUT
        return None

    def niveaux_du_jeton(authorization: str = Header(default="")) -> frozenset[str]:
        if (n := niveaux_de(authorization)) is None:
            raise HTTPException(401, "jeton invalide")
        return n

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
            # Un compte seulement, et pas de 404 : ni liste ni oracle des noms de sources.
            return {"demande": gestion.demander_reindexation(conn, d.source)}

    @app.get("/etat")
    def route_etat(niveaux: frozenset[str] = Depends(niveaux_du_jeton)):
        with connexion(reglages.database_url) as conn:
            sources = conn.execute(
                """SELECT s.nom, s.type, s.revision, s.indexe_le,
                          (SELECT count(*) FROM documents d WHERE d.source_id = s.id) AS documents,
                          s.demande_le IS NOT NULL AS demande_en_attente
                     FROM sources s WHERE s.visibilite = ANY(%s) ORDER BY s.nom""",
                (sorted(niveaux),)).fetchall()
            en_cours = gestion.passe_en_cours(conn)
            nuit = conn.execute("SELECT debut, fin, ok, bilan FROM passes WHERE genre = "
                                "'nocturne' AND fin IS NOT NULL ORDER BY debut DESC LIMIT 1"
                                ).fetchone()
            caches = {r["nom"] for r in conn.execute(
                "SELECT nom FROM sources WHERE NOT visibilite = ANY(%s)", (sorted(niveaux),))}
        if nuit:  # le bilan ne nomme pas une source que ce jeton ne voit pas
            nuit["bilan"] = {k: ({"erreur": "echec"} if isinstance(v, dict) and "erreur" in v
                                 else v)
                             for k, v in nuit["bilan"].items() if k not in caches}
        return {"sources": sources, "en_cours": en_cours, "derniere_nocturne": nuit}

    app.include_router(routeur_ui(
        niveaux_de=niveaux_de, ecriture_possible=bool(reglages.jeton_prive),
        database_url=reglages.database_url,
        version_id=lambda: _avec_conn(version_id, empreinte),
        coffret=coffret_depuis(reglages), github_jeton=reglages.github_jeton,
        noms_reserves={s.nom for s in config.sources}))
    app.add_exception_handler(RequestValidationError, validation_sans_cache)
    app.mount("/ui/statique", StaticFiles(directory=STATIQUE), name="ui_statique")

    @app.get("/", include_in_schema=False)
    def racine():
        return RedirectResponse("/ui/", status_code=307)

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
            return {"mode": "regroupement", "filtres": asdict(f), "elements": [], "total": 0,
                    "tronque": False}
        return lister(conn, question, f, config, modeles, v, niveaux=niveaux, reranker=reranker)

    def _repondre(question: str, f: Filtres, niveaux, flux: bool, avant=(), bonus=None):
        if flux:
            def evenements():
                yield from avant
                with connexion(reglages.database_url) as conn:
                    v = version_id(conn, empreinte) or 0
                    try:
                        yield from repondre_flux(conn, question, config, modeles, v,
                                                 niveaux=niveaux, filtres=f, reranker=reranker,
                                                 bonus_sources=bonus)
                    except ErreurModele:  # en plein flux : plus de code HTTP possible
                        yield {"type": "erreur", "detail": "modèle indisponible"}
            return StreamingResponse(_sse(evenements()), media_type="text/event-stream")
        with connexion(reglages.database_url) as conn:
            v = version_id(conn, empreinte) or 0
            return asdict(repondre(conn, question, config, modeles, v, niveaux=niveaux,
                                   filtres=f, reranker=reranker, bonus_sources=bonus))

    @app.post("/lister")
    def route_lister(d: DemandeLister, niveaux: frozenset[str] = Depends(niveaux_du_jeton)):
        with connexion(reglages.database_url) as conn:
            return _lister(conn, d.question, Filtres(**d.filtres.model_dump()), niveaux)

    @app.post("/repondre")
    def route_repondre(d: DemandeRepondre, niveaux: frozenset[str] = Depends(niveaux_du_jeton)):
        with connexion(reglages.database_url) as conn:
            bonus = _bonus(conn, d.question, niveaux)
        return _repondre(d.question, Filtres(**d.filtres.model_dump()), niveaux, d.flux,
                         bonus=bonus)

    @app.post("/question")
    def route_question(d: DemandeQuestion, niveaux: frozenset[str] = Depends(niveaux_du_jeton)):
        with connexion(reglages.database_url) as conn:
            dec = aiguiller(d.question, taxonomie(conn, niveaux=niveaux), modeles,
                            config.reponse.modele, d.mode,
                            Filtres(**d.filtres.model_dump()) if d.filtres else None)
            tete = {"mode": dec.mode, "aiguillage": {"par": dec.par,
                                                     "filtres": asdict(dec.filtres),
                                                     "bonus_sources": dec.bonus_sources}}
            if dec.mode == "lister":  # le « mode » de lister (metadonnees…) devient mode_liste
                corps = _lister(conn, d.question, dec.filtres, niveaux)
                return tete | {"mode_liste": corps.pop("mode")} | corps
        if d.flux:
            return _repondre(d.question, dec.filtres, niveaux, True,
                             [{"type": "aiguillage"} | tete], dec.bonus_sources)
        return tete | _repondre(d.question, dec.filtres, niveaux, False,
                                bonus=dec.bonus_sources)

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
