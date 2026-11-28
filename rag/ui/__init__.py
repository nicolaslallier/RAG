"""Interface de gestion sous /ui/ : connexion par jeton (cookie), Sources, Fichiers, Exclusions.

Le cookie `rag_session` contient le jeton lui-même ; il n'est jamais affiché ni journalisé.
Les écritures exigent le jeton privé, l'en-tête `HX-Request` et une `Origin` absente ou locale.
"""

import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Annotated
from urllib.parse import quote, urlencode

from fastapi import APIRouter, Depends, Form, HTTPException, Request, Response
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import UploadFile
from starlette.exceptions import HTTPException as StarletteHTTPException

from rag.config import TOUT
from rag.db import connexion
from rag.gestion import (
    PAGE_MAX,
    RAISON_MAX,
    MotifInvalide,
    SourceInconnue,
    annuler,
    apercu,
    demander_reindexation,
    exclure,
    exporter,
    importer,
    passe_en_cours,
)
from rag.gestion import exclusions as lister_exclusions
from rag.gestion import fichier as lire_fichier
from rag.gestion import fichiers as lister_fichiers
from rag.gestion import sources as lister_sources
from rag.sources import ErreurSource, jeton_general, verifier_depot
from rag.sources_gerees import (
    Coffret,
    JetonIllisible,
    JetonImpossible,
    SourceInvalide,
    _origine,
    ajouter,
    bilan_suppression,
    modifier,
    supprimer,
    valider,
)
from rag.sources_gerees import exporter as exporter_sources
from rag.sources_gerees import importer as importer_sources

log = logging.getLogger("rag.ui")

DOSSIER = Path(__file__).parent
STATIQUE = DOSSIER / "statique"
COOKIE = "rag_session"
_COOKIE_ATTRS = {"path": "/ui", "httponly": True, "samesite": "strict", "secure": False}
SANS_CACHE = {"Cache-Control": "no-store"}
PAR_PAGE = 50
ETATS = ("indexe", "exclu_auto", "exclu_manuel")
INTROUVABLE = "Source ou fichier introuvable."
GROS_APERCU = 100  # au-delà, seconde confirmation (case à cocher)
ORIGINE_FICHIERS = "fichiers"  # aperçu lancé depuis l'écran des fichiers
IMPORT_MAX = 1024 * 1024  # octets du fichier importé
IMPORT_MARGE = 16 * 1024  # enveloppe multipart tolérée au-delà, pour refuser tôt sur Content-Length
CHEMINS_IMPORT = ("/ui/exclusions/import", "/ui/sources-git/import")  # plafond propre (rag.garde)


def _sans_cache[R: Response](reponse: R) -> R:
    reponse.headers.update(SANS_CACHE)
    return reponse


async def validation_sans_cache(request: Request, exc: RequestValidationError) -> Response:
    """422 de FastAPI, avec `Cache-Control: no-store` sous /ui/ ; à enregistrer sur l'app."""
    reponse = await request_validation_exception_handler(request, exc)
    return _sans_cache(reponse) if request.url.path.startswith("/ui/") else reponse


def _en_texte(valeur) -> str:
    """Valeur de frontmatter lisible : texte tel quel, liste simple jointe, sinon JSON."""
    if isinstance(valeur, str):
        return valeur
    if isinstance(valeur, list) and all(isinstance(v, str | int | float) for v in valeur):
        return ", ".join(map(str, valeur))
    return json.dumps(valeur, ensure_ascii=False)


def _motifs(texte: str) -> list[str]:
    """Un motif d'exclusion par ligne ; lignes vides ignorées."""
    return [m.strip() for m in texte.splitlines() if m.strip()]


async def _lire_fichier_import(request: Request) -> bytes | tuple[str, int]:
    """Contenu du fichier téléversé, ou `(message, statut)` si refusé. Corps lu après la garde."""
    trop_gros = f"Fichier trop gros : {IMPORT_MAX // 1024} Ko au plus."
    longueur = request.headers.get("content-length", "")
    if longueur.isdigit() and int(longueur) > IMPORT_MAX + IMPORT_MARGE:
        return trop_gros, 413
    try:
        async with request.form(max_files=1, max_fields=1) as formulaire:
            fichier = formulaire.get("fichier")
            if not isinstance(fichier, UploadFile):
                return "Choisissez un fichier YAML.", 400
            contenu = await fichier.read(IMPORT_MAX + 1)  # l'en-tête seul ne suffit pas
    except StarletteHTTPException:  # formulaire multipart malformé
        return "Formulaire illisible.", 400
    if len(contenu) > IMPORT_MAX:
        return trop_gros, 413
    return contenu


def routeur_ui(*, niveaux_de: Callable[[str], frozenset[str] | None], ecriture_possible: bool,
               database_url: str, version_id: Callable[[], int | None],
               coffret: Coffret | None = None, github_jeton: str = "",
               noms_reserves: set[str] = frozenset()) -> APIRouter:
    gabarits = Jinja2Templates(directory=DOSSIER / "gabarits")  # autoescape actif (.html)
    gabarits.env.filters["en_requete"] = lambda v: quote(str(v), safe="")  # tout encodé, « / » aussi
    gabarits.env.filters["en_texte"] = _en_texte  # texte simple : échappé par le gabarit
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

    def origine_locale(request: Request) -> bool:
        origine = request.headers.get("origin")
        return origine is None or origine == f"{request.url.scheme}://{request.headers.get('host', '')}"

    def ecrivain(request: Request, niveaux: Annotated[frozenset[str], Depends(session)]) -> None:
        if (not peut_ecrire(niveaux) or request.headers.get("hx-request") != "true"
                or not origine_locale(request)):
            raise HTTPException(403, "écriture refusée", headers=SANS_CACHE)

    def page_ui(request: Request, nom: str, statut: int = 200, **contexte) -> HTMLResponse:
        return _sans_cache(gabarits.TemplateResponse(request, nom, contexte, status_code=statut))

    @routeur.get("/connexion", response_class=HTMLResponse)
    def connexion_page(request: Request):
        return page_ui(request, "connexion.html", erreur=None)

    @routeur.post("/connexion")
    def connexion_envoi(request: Request, jeton: Annotated[str, Form()] = ""):
        if not origine_locale(request):  # pas de connexion forcée depuis un site tiers
            raise HTTPException(403, "origine refusée", headers=SANS_CACHE)
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
            en_cours = passe_en_cours(conn)
        return page_ui(request, "sources.html", sources=lignes, ecrivain=peut_ecrire(niveaux),
                       en_cours=en_cours, version=version_id())

    @routeur.get("/sources/{nom}", response_class=HTMLResponse)
    def ecran_fichiers(request: Request, nom: str,
                       niveaux: Annotated[frozenset[str], Depends(session)],
                       etat: str = "", contient: str = "", page: int = 1):
        etat_ok = etat if etat in ETATS else None
        contient = contient.strip()
        filtres = {"niveaux": niveaux, "etat": etat_ok, "contient": contient or None,
                   "par_page": PAR_PAGE}
        try:
            with connexion(database_url) as conn:
                # page bornée avant la requête, puis ramenée à la dernière
                liste = lister_fichiers(conn, nom, page=min(page, PAGE_MAX), **filtres)
                if 1 <= liste["pages"] < liste["page"]:  # au-delà de la dernière : la dernière
                    liste = lister_fichiers(conn, nom, page=liste["pages"], **filtres)
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

    # --- Exclusions ---------------------------------------------------------------------------

    def exclusions_visibles(conn, niveaux: frozenset[str]) -> list[dict]:
        """Exclusions des sources visibles ; celles d'une source pas encore connue (importées
        pour plus tard) seulement pour l'écrivain."""
        visibles = {s["nom"] for s in lister_sources(conn, niveaux=niveaux)}
        connues = {r["nom"] for r in conn.execute("SELECT nom FROM sources")}
        ecrit = peut_ecrire(niveaux)
        return [e for e in lister_exclusions(conn)
                if e["source"] in visibles or (ecrit and e["source"] not in connues)]

    def message(request: Request, texte: str, statut: int = 200, *, erreur: bool = False,
                niveaux: frozenset[str] | None = None) -> HTMLResponse:
        """Fragment : un message et, après une écriture réussie, la liste à jour (hors bande)."""
        contexte = {"message": texte, "erreur": erreur, "oob": True, "ecrivain": True}
        if niveaux is not None:
            with connexion(database_url) as conn:
                contexte["exclusions"] = exclusions_visibles(conn, niveaux)
        return page_ui(request, "_message.html", statut, **contexte)

    def pluriel(n: int, mot: str) -> str:
        return f"{n} {mot}{'s' if n > 1 else ''}"

    @routeur.get("/exclusions", response_class=HTMLResponse)
    def ecran_exclusions(request: Request, niveaux: Annotated[frozenset[str], Depends(session)]):
        with connexion(database_url) as conn:
            lignes = exclusions_visibles(conn, niveaux)
            noms = [s["nom"] for s in lister_sources(conn, niveaux=niveaux)]
        return page_ui(request, "exclusions.html", exclusions=lignes, sources=noms,
                       ecrivain=peut_ecrire(niveaux), raison_max=RAISON_MAX)

    @routeur.post("/exclusions/apercu", response_class=HTMLResponse,
                  dependencies=[Depends(ecrivain)])
    def apercu_exclusion(request: Request, source: Annotated[str, Form()],
                         motif: Annotated[str, Form()], raison: Annotated[str, Form()] = "",
                         origine: Annotated[str, Form()] = "exclusions"):
        if len(raison.strip()) > RAISON_MAX:
            return message(request, f"La raison fait au plus {RAISON_MAX} caractères.", 400,
                           erreur=True)
        try:
            with connexion(database_url) as conn:
                a = apercu(conn, source, motif)
        except MotifInvalide as e:
            return message(request, f"Motif refusé : {e}.", 400, erreur=True)
        except SourceInconnue:
            raise HTTPException(404, "source inconnue", headers=SANS_CACHE) from None
        return page_ui(request, "_apercu.html", source=source, motif=motif.strip(),
                       raison=raison.strip(), origine=ORIGINE_FICHIERS if origine == ORIGINE_FICHIERS
                       else "exclusions", total=a["total"], chemins=a["chemins"],
                       gros=a["total"] > GROS_APERCU, raison_max=RAISON_MAX)

    @routeur.post("/exclusions", response_class=HTMLResponse, dependencies=[Depends(ecrivain)])
    def ajouter_exclusion(request: Request, niveaux: Annotated[frozenset[str], Depends(session)],
                          source: Annotated[str, Form()], motif: Annotated[str, Form()],
                          raison: Annotated[str, Form()] = "",
                          confirme_gros: Annotated[str, Form()] = "",
                          origine: Annotated[str, Form()] = "exclusions"):
        try:
            with connexion(database_url) as conn:
                total = apercu(conn, source, motif)["total"]
                if total > GROS_APERCU and not confirme_gros:
                    return message(request, f"Cochez « Je confirme retirer {total} documents » "
                                   "pour continuer.", 400, erreur=True)
                n = exclure(conn, source, motif, raison)
        except MotifInvalide as e:
            return message(request, f"Exclusion refusée : {e}.", 400, erreur=True)
        except SourceInconnue:
            raise HTTPException(404, "source inconnue", headers=SANS_CACHE) from None
        log.info("exclusion manuelle ajoutée : %s, %d documents retirés", source, n)
        texte = f"{pluriel(n, 'document')} retiré{'s' if n > 1 else ''}."
        if origine == ORIGINE_FICHIERS:  # pas de liste d'exclusions sur cet écran : on recharge
            reponse = message(request, texte)
            reponse.headers["HX-Refresh"] = "true"
            return reponse
        return message(request, texte, niveaux=niveaux)

    @routeur.post("/exclusions/annuler", response_class=HTMLResponse,
                  dependencies=[Depends(ecrivain)])
    def annuler_exclusion(request: Request, niveaux: Annotated[frozenset[str], Depends(session)],
                          source: Annotated[str, Form()], motif: Annotated[str, Form()]):
        try:
            with connexion(database_url) as conn:
                levee = annuler(conn, source, motif)
        except MotifInvalide as e:
            return message(request, f"Motif refusé : {e}.", 400, erreur=True)
        if levee:
            log.info("exclusion manuelle annulée : %s", source)
        texte = ("Exclusion annulée : le fichier reviendra à la prochaine passe." if levee
                 else "Cette exclusion était déjà levée.")
        return message(request, texte, niveaux=niveaux)

    @routeur.get("/exclusions/export")
    def exporter_exclusions(niveaux: Annotated[frozenset[str], Depends(session)]):
        if not peut_ecrire(niveaux):  # l'export nomme toutes les sources, privées comprises
            raise HTTPException(403, "export réservé à l'écrivain", headers=SANS_CACHE)
        with connexion(database_url) as conn:
            texte = exporter(conn)
        return _sans_cache(Response(texte, media_type="text/yaml", headers={
            "Content-Disposition": 'attachment; filename="exclusions-rag.yaml"'}))

    @routeur.post("/exclusions/import", response_class=HTMLResponse,
                  dependencies=[Depends(ecrivain)])
    async def importer_exclusions(request: Request,
                                  niveaux: Annotated[frozenset[str], Depends(session)]):
        # Corps lu ici, après `ecrivain` : un formulaire déclaré serait lu avant la garde.
        lu = await _lire_fichier_import(request)
        if isinstance(lu, tuple):
            return message(request, lu[0], lu[1], erreur=True)
        contenu = lu
        try:
            texte = contenu.decode("utf-8")
        except UnicodeDecodeError:
            return message(request, "Fichier illisible : UTF-8 attendu.", 400, erreur=True)

        def importer_en_base() -> dict:
            with connexion(database_url) as conn:
                return importer(conn, texte)

        try:
            bilan = await run_in_threadpool(importer_en_base)
        except MotifInvalide as e:
            return message(request, f"Import refusé : {e}.", 400, erreur=True)
        log.info("import d'exclusions : %d ajoutées, %d déjà présentes, %d documents retirés",
                 bilan["ajoutees"], bilan["ignorees"], bilan["retires"])
        n = bilan["retires"]
        texte = (f"{bilan['ajoutees']} ajoutées, {bilan['ignorees']} déjà présentes, "
                 f"{pluriel(n, 'document')} retiré{'s' if n > 1 else ''}.")
        return await run_in_threadpool(message, request, texte, niveaux=niveaux)

    # --- Sources git --------------------------------------------------------------------------

    def ecrivain_visible(niveaux: Annotated[frozenset[str], Depends(session)]) -> None:
        if not peut_ecrire(niveaux):  # GET des formulaires : l'écrivain seulement
            raise HTTPException(403, "réservé à l'écrivain", headers=SANS_CACHE)

    def source_geree(nom: str) -> dict:
        with connexion(database_url) as conn:
            r = conn.execute("SELECT * FROM sources WHERE nom = %s AND geree", (nom,)).fetchone()
        if r is None:
            raise HTTPException(404, INTROUVABLE, headers=SANS_CACHE)
        return r

    def redirection_sources() -> Response:
        return _sans_cache(Response(headers={"HX-Redirect": "/ui/"}))

    @routeur.get("/sources-git/nouvelle", response_class=HTMLResponse,
                 dependencies=[Depends(ecrivain_visible)])
    def formulaire_nouvelle(request: Request):
        source = {"nom": "", "emplacement": "", "branche": "main", "visibilite": "public-maison",
                  "exclure": [], "utilisateur": "x-access-token", "jeton_chiffre": None}
        return page_ui(request, "source_git.html", source=source, creation=True,
                       coffret=coffret is not None)

    @routeur.get("/sources-git/export", dependencies=[Depends(ecrivain_visible)])
    def exporter_les_sources():
        with connexion(database_url) as conn:
            texte = exporter_sources(conn)
        return _sans_cache(Response(texte, media_type="text/yaml", headers={
            "Content-Disposition": 'attachment; filename="sources-rag.yaml"'}))

    @routeur.post("/sources-git", response_class=HTMLResponse, dependencies=[Depends(ecrivain)])
    def creer_source(request: Request, nom: Annotated[str, Form()],
                     emplacement: Annotated[str, Form()], branche: Annotated[str, Form()] = "main",
                     visibilite: Annotated[str, Form()] = "public-maison",
                     exclure: Annotated[str, Form()] = "", utilisateur: Annotated[str, Form()] = "",
                     jeton: Annotated[str, Form()] = ""):
        utilisateur = utilisateur.strip() or "x-access-token"
        jeton = jeton.strip()
        try:
            conf = valider(nom.strip(), emplacement, branche.strip(), visibilite, _motifs(exclure),
                           utilisateur, noms_reserves=set(noms_reserves))
            if jeton and coffret is None:
                raise JetonImpossible("RAG_CLE_SECRETS absente : impossible d'enregistrer un jeton")
            verifier_depot(conf.emplacement, conf.branche,
                           jeton or jeton_general(conf.emplacement, github_jeton), utilisateur)
            with connexion(database_url) as conn:
                ajouter(conn, conf, utilisateur=utilisateur, jeton=jeton or None, coffret=coffret)
        except (SourceInvalide, ErreurSource) as e:
            return message(request, f"Source refusée : {e}.", 400, erreur=True)
        log.info("source git ajoutée : %s", conf.nom)
        return redirection_sources()

    @routeur.get("/sources-git/{nom}/modifier", response_class=HTMLResponse,
                 dependencies=[Depends(ecrivain_visible)])
    def formulaire_modifier(request: Request, nom: str):
        return page_ui(request, "source_git.html", source=source_geree(nom), creation=False,
                       coffret=coffret is not None)

    @routeur.post("/sources-git/{nom}/modifier", response_class=HTMLResponse,
                  dependencies=[Depends(ecrivain)])
    def modifier_source(request: Request, nom: str, emplacement: Annotated[str, Form()],
                        branche: Annotated[str, Form()] = "main",
                        visibilite: Annotated[str, Form()] = "public-maison",
                        exclure: Annotated[str, Form()] = "",
                        utilisateur: Annotated[str, Form()] = "",
                        jeton: Annotated[str, Form()] = "",
                        retirer_jeton: Annotated[str, Form()] = ""):
        actuelle = source_geree(nom)
        utilisateur = utilisateur.strip() or "x-access-token"
        jeton = jeton.strip()
        retirer = bool(retirer_jeton)
        try:
            conf = valider(nom, emplacement, branche.strip(), visibilite, _motifs(exclure),
                           utilisateur, noms_reserves=set(noms_reserves))
            if jeton and coffret is None:
                raise JetonImpossible("RAG_CLE_SECRETS absente : impossible d'enregistrer un jeton")
            stocke = actuelle["jeton_chiffre"] is not None
            reseau = (conf.emplacement != actuelle["emplacement"]
                      or conf.branche != (actuelle["branche"] or "main")
                      or utilisateur != actuelle["utilisateur"]
                      or bool(jeton) or (retirer and stocke))
            if reseau:
                verifier_depot(conf.emplacement, conf.branche,
                               _jeton_de_verification(actuelle, conf.emplacement, jeton, retirer),
                               utilisateur)
            with connexion(database_url) as conn:
                modifier(conn, nom, conf, utilisateur=utilisateur, jeton=jeton or None,
                         retirer_jeton=retirer, coffret=coffret)
        except SourceInconnue:
            raise HTTPException(404, INTROUVABLE, headers=SANS_CACHE) from None
        except (SourceInvalide, ErreurSource) as e:
            return message(request, f"Modification refusée : {e}.", 400, erreur=True)
        log.info("source git modifiée : %s", nom)
        return redirection_sources()

    def _jeton_de_verification(actuelle: dict, emplacement: str, jeton: str, retirer: bool) -> str:
        """Jeton pour joindre le dépôt, comme le fera le travailleur : le nouveau, sinon
        l'enregistré (même origine, non retiré), sinon le jeton général (github.com)."""
        if jeton:
            return jeton
        if (not retirer and coffret is not None and actuelle["jeton_chiffre"] is not None
                and _origine(emplacement) == _origine(actuelle["emplacement"])):
            try:
                return coffret.dechiffrer(actuelle["jeton_chiffre"])
            except JetonIllisible:
                pass
        return jeton_general(emplacement, github_jeton)

    @routeur.get("/sources-git/{nom}/supprimer", response_class=HTMLResponse,
                 dependencies=[Depends(ecrivain_visible)])
    def confirmer_suppression(request: Request, nom: str):
        try:
            with connexion(database_url) as conn:
                bilan = bilan_suppression(conn, nom)
        except SourceInconnue:
            raise HTTPException(404, INTROUVABLE, headers=SANS_CACHE) from None
        return page_ui(request, "supprimer_source.html", bilan=bilan)

    @routeur.post("/sources-git/{nom}/supprimer", response_class=HTMLResponse,
                  dependencies=[Depends(ecrivain)])
    def supprimer_source(nom: str):
        try:
            with connexion(database_url) as conn:
                supprimer(conn, nom)
        except SourceInconnue:
            raise HTTPException(404, INTROUVABLE, headers=SANS_CACHE) from None
        log.info("source git supprimée : %s", nom)
        return redirection_sources()

    @routeur.post("/sources-git/import", response_class=HTMLResponse,
                  dependencies=[Depends(ecrivain)])
    async def importer_les_sources(request: Request):
        lu = await _lire_fichier_import(request)
        if isinstance(lu, tuple):
            return message(request, lu[0], lu[1], erreur=True)
        try:
            texte = lu.decode("utf-8")
        except UnicodeDecodeError:
            return message(request, "Fichier illisible : UTF-8 attendu.", 400, erreur=True)

        def importer_en_base() -> dict:
            with connexion(database_url) as conn:
                return importer_sources(conn, texte, noms_reserves=set(noms_reserves))

        try:
            bilan = await run_in_threadpool(importer_en_base)
        except SourceInvalide as e:
            return message(request, f"Import refusé : {e}.", 400, erreur=True)
        log.info("import de sources git : %d ajoutées, %d modifiées, %d inchangées",
                 bilan["ajoutees"], bilan["modifiees"], bilan["inchangees"])
        a, m, i = bilan["ajoutees"], bilan["modifiees"], bilan["inchangees"]
        return message(request, f"{a} ajoutée{'s' if a > 1 else ''}, "
                       f"{m} modifiée{'s' if m > 1 else ''}, {i} inchangée{'s' if i > 1 else ''}. "
                       "Rechargez la page pour voir la liste.")

    return routeur
