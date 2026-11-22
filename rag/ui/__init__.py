"""Interface de gestion sous /ui/ : connexion par jeton (cookie), écran Sources.

Le cookie `rag_session` contient le jeton lui-même ; il n'est jamais affiché ni journalisé.
Les écritures exigent le jeton privé, l'en-tête `HX-Request` et une `Origin` absente ou locale.
"""

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Annotated
from urllib.parse import quote, urlencode

from fastapi import APIRouter, Depends, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from rag.config import TOUT
from rag.db import connexion
from rag.gestion import SourceInconnue, demander_reindexation
from rag.gestion import fichier as lire_fichier
from rag.gestion import fichiers as lister_fichiers
from rag.gestion import sources as lister_sources

log = logging.getLogger("rag.ui")

DOSSIER = Path(__file__).parent
STATIQUE = DOSSIER / "statique"
COOKIE = "rag_session"
_COOKIE_ATTRS = {"path": "/ui", "httponly": True, "samesite": "strict", "secure": False}
SANS_CACHE = {"Cache-Control": "no-store"}
PAR_PAGE = 50
ETATS = ("indexe", "exclu_auto", "exclu_manuel")
INTROUVABLE = "Source ou fichier introuvable."


def _sans_cache[R: Response](reponse: R) -> R:
    reponse.headers.update(SANS_CACHE)
    return reponse


def routeur_ui(*, niveaux_de: Callable[[str], frozenset[str] | None], ecriture_possible: bool,
               database_url: str, version_id: Callable[[], int | None]) -> APIRouter:
    gabarits = Jinja2Templates(directory=DOSSIER / "gabarits")  # autoescape actif (.html)
    gabarits.env.filters["en_requete"] = lambda v: quote(str(v), safe="")  # tout encodé, « / » aussi
    routeur = APIRouter(prefix="/ui")

    def session(request: Request) -> frozenset[str]:
        jeton = request.cookies.get(COOKIE, "")
        if not jeton or (niveaux := niveaux_de("Bearer " + jeton)) is None:
            if request.headers.get("hx-request") == "true":  # htmx : pas de page de connexion dans un tableau
                raise HTTPException(401, "session expirée",
                                    headers=SANS_CACHE | {"HX-Redirect": "/ui/connexion"})
            raise HTTPException(303, headers=SANS_CACHE | {"Location": "/ui/connexion"})
        return niveaux

    def peut_ecrire(niveaux: frozenset[str]) -> bool:
        return ecriture_possible and niveaux == TOUT

    def ecrivain(request: Request, niveaux: Annotated[frozenset[str], Depends(session)]) -> None:
        origine = request.headers.get("origin")
        attendue = f"{request.url.scheme}://{request.headers.get('host', '')}"
        if (not peut_ecrire(niveaux) or request.headers.get("hx-request") != "true"
                or (origine is not None and origine != attendue)):
            raise HTTPException(403, "écriture refusée", headers=SANS_CACHE)

    def page_ui(request: Request, nom: str, statut: int = 200, **contexte) -> HTMLResponse:
        return _sans_cache(gabarits.TemplateResponse(request, nom, contexte, status_code=statut))

    @routeur.get("/connexion", response_class=HTMLResponse)
    def connexion_page(request: Request):
        return page_ui(request, "connexion.html", erreur=None)

    @routeur.post("/connexion")
    def connexion_envoi(request: Request, jeton: Annotated[str, Form()] = ""):
        jeton = jeton.strip()
        if not jeton or niveaux_de("Bearer " + jeton) is None:
            log.warning("connexion à l'interface refusée")
            return page_ui(request, "connexion.html", 401, erreur="Jeton refusé.")
        log.info("connexion à l'interface acceptée")
        r = RedirectResponse("/ui/", status_code=303)
        r.set_cookie(COOKIE, jeton, **_COOKIE_ATTRS)
        return _sans_cache(r)

    @routeur.post("/deconnexion")
    def deconnexion():
        r = RedirectResponse("/ui/connexion", status_code=303)
        r.delete_cookie(COOKIE, **_COOKIE_ATTRS)
        return _sans_cache(r)

    @routeur.get("/", response_class=HTMLResponse)
    def ecran_sources(request: Request, niveaux: Annotated[frozenset[str], Depends(session)]):
        with connexion(database_url) as conn:
            lignes = lister_sources(conn, niveaux=niveaux)
        return page_ui(request, "sources.html", sources=lignes, ecrivain=peut_ecrire(niveaux),
                    version=version_id())

    @routeur.get("/sources/{nom}", response_class=HTMLResponse)
    def ecran_fichiers(request: Request, nom: str,
                       niveaux: Annotated[frozenset[str], Depends(session)],
                       etat: str = "", contient: str = "", page: int = 1):
        etat_ok = etat if etat in ETATS else None
        contient = contient.strip()
        try:
            with connexion(database_url) as conn:
                liste = lister_fichiers(conn, nom, niveaux=niveaux, etat=etat_ok,
                                        contient=contient or None, page=page, par_page=PAR_PAGE)
        except SourceInconnue:
            raise HTTPException(404, INTROUVABLE, headers=SANS_CACHE) from None
        requete = urlencode({k: v for k, v in (("etat", etat_ok), ("contient", contient)) if v})
        gabarit = "_lignes_fichiers.html" if request.headers.get("hx-request") == "true" \
            else "fichiers.html"
        return page_ui(request, gabarit, liste=liste, etat=etat_ok, contient=contient,
                       requete=requete, ecrivain=peut_ecrire(niveaux))

    @routeur.get("/sources/{nom}/fichier", response_class=HTMLResponse)
    def ecran_fichier(request: Request, nom: str, chemin: str,
                      niveaux: Annotated[frozenset[str], Depends(session)]):
        try:
            with connexion(database_url) as conn:
                doc = lire_fichier(conn, nom, chemin, niveaux=niveaux, version_id=version_id())
        except SourceInconnue:
            raise HTTPException(404, INTROUVABLE, headers=SANS_CACHE) from None
        return page_ui(request, "fichier.html", doc=doc)

    @routeur.post("/sources/{nom}/reindexer", response_class=HTMLResponse,
                  dependencies=[Depends(ecrivain)])
    def reindexer(nom: str, niveaux: Annotated[frozenset[str], Depends(session)]):
        with connexion(database_url) as conn:
            ligne = next((s for s in lister_sources(conn, niveaux=niveaux) if s["nom"] == nom),
                         None)
            if ligne is None:
                raise HTTPException(404, "source inconnue", headers=SANS_CACHE)
            demander_reindexation(conn, nom)
            ligne["demande_en_attente"] = True
        macro = gabarits.get_template("_macros.html").module.ligne_source
        return _sans_cache(HTMLResponse(macro(ligne, True)))

    return routeur
