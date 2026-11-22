"""Interface de gestion sous /ui/ : connexion par jeton (cookie), écran Sources.

Le cookie `rag_session` contient le jeton lui-même ; il n'est jamais affiché ni journalisé.
Les écritures exigent le jeton privé, l'en-tête `HX-Request` et une `Origin` absente ou locale.
"""

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from rag.config import TOUT
from rag.db import connexion
from rag.gestion import demander_reindexation
from rag.gestion import sources as lister_sources

log = logging.getLogger("rag.ui")

DOSSIER = Path(__file__).parent
STATIQUE = DOSSIER / "statique"
COOKIE = "rag_session"
_COOKIE_ATTRS = {"path": "/ui", "httponly": True, "samesite": "strict", "secure": False}


def routeur_ui(*, niveaux_de: Callable[[str], frozenset[str] | None], ecriture_possible: bool,
               base_url: str, version_id: Callable[[], int | None]) -> APIRouter:
    gabarits = Jinja2Templates(directory=DOSSIER / "gabarits")  # autoescape actif (.html)
    routeur = APIRouter(prefix="/ui")

    def session(request: Request) -> frozenset[str]:
        jeton = request.cookies.get(COOKIE, "")
        if not jeton or (niveaux := niveaux_de("Bearer " + jeton)) is None:
            raise HTTPException(303, headers={"Location": "/ui/connexion"})
        return niveaux

    def peut_ecrire(niveaux: frozenset[str]) -> bool:
        return ecriture_possible and niveaux == TOUT

    def ecrivain(request: Request, niveaux: Annotated[frozenset[str], Depends(session)]) -> None:
        origine = request.headers.get("origin")
        attendue = f"{request.url.scheme}://{request.headers.get('host', '')}"
        if (not peut_ecrire(niveaux) or request.headers.get("hx-request") != "true"
                or (origine is not None and origine != attendue)):
            raise HTTPException(403, "écriture refusée")

    def page(request: Request, nom: str, statut: int = 200, **contexte) -> HTMLResponse:
        return gabarits.TemplateResponse(request, nom, contexte, status_code=statut)

    @routeur.get("/connexion", response_class=HTMLResponse)
    def connexion_page(request: Request):
        return page(request, "connexion.html", erreur=None)

    @routeur.post("/connexion")
    def connexion_envoi(request: Request, jeton: Annotated[str, Form()] = ""):
        jeton = jeton.strip()
        if not jeton or niveaux_de("Bearer " + jeton) is None:
            log.warning("connexion à l'interface refusée")
            return page(request, "connexion.html", 401, erreur="Jeton refusé.")
        log.info("connexion à l'interface acceptée")
        r = RedirectResponse("/ui/", status_code=303)
        r.set_cookie(COOKIE, jeton, **_COOKIE_ATTRS)
        return r

    @routeur.post("/deconnexion")
    def deconnexion():
        r = RedirectResponse("/ui/connexion", status_code=303)
        r.delete_cookie(COOKIE, **_COOKIE_ATTRS)
        return r

    @routeur.get("/", response_class=HTMLResponse)
    def ecran_sources(request: Request, niveaux: Annotated[frozenset[str], Depends(session)]):
        with connexion(base_url) as conn:
            lignes = lister_sources(conn, niveaux=niveaux)
        return page(request, "sources.html", sources=lignes, ecrivain=peut_ecrire(niveaux),
                    version=version_id())

    @routeur.post("/sources/{nom}/reindexer", response_class=HTMLResponse,
                  dependencies=[Depends(ecrivain)])
    def reindexer(nom: str, niveaux: Annotated[frozenset[str], Depends(session)]):
        with connexion(base_url) as conn:
            ligne = next((s for s in lister_sources(conn, niveaux=niveaux) if s["nom"] == nom),
                         None)
            if ligne is None:
                raise HTTPException(404, "source inconnue")
            demander_reindexation(conn, nom)
            ligne["demande_en_attente"] = True
        macro = gabarits.get_template("_macros.html").module.ligne_source
        return HTMLResponse(macro(ligne, True))

    return routeur
