"""API HTTP : /sante (sans jeton) et /chercher (jeton → niveaux de visibilité)."""

import hmac
from datetime import date

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from rag.config import PUBLIC, TOUT, Reglages, charger_config, empreinte_indexation
from rag.db import connexion
from rag.modeles import modeles_depuis
from rag.recherche import Filtres, chercher


class FiltresEntree(BaseModel):
    model_config = ConfigDict(extra="forbid")
    sources: list[str] | None = None
    dossier: str | None = None
    tags: list[str] | None = None
    type: str | None = None
    depuis: date | None = None
    jusqua: date | None = None


class Demande(BaseModel):
    question: str = Field(max_length=2000)
    filtres: FiltresEntree = FiltresEntree()
    limite: int = Field(10, ge=1, le=50)


def version_id(conn, empreinte: str) -> int | None:
    r = conn.execute("SELECT id FROM index_versions WHERE empreinte = %s", (empreinte,)).fetchone()
    return r["id"] if r else None


def create_app(reglages: Reglages | None = None, modeles=None) -> FastAPI:
    reglages = reglages or Reglages()
    if not reglages.jeton:
        raise RuntimeError("RAG_JETON est vide : l'API refuse de démarrer sans jeton")
    config = charger_config(reglages.config)
    empreinte = empreinte_indexation(config.indexation)
    modeles = modeles or modeles_depuis(reglages)
    app = FastAPI(title="RAG maître")

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
                         filtres=Filtres(**d.filtres.model_dump()), limite=d.limite)
        return {"resultats": r}

    return app
