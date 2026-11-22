from pathlib import Path

import pytest

from rag.config import PUBLIC, SourceConf, charger_config
from rag.indexeur import indexer_source, version_courante
from rag.recherche import chercher
from rag.sources import LecteurDossier
from tests.conftest import indexer_coffre

CONFIG = charger_config(Path("config/rag.yaml"))
R = CONFIG.recherche


def avec(**poids):
    return R.model_copy(update={"poids_source": poids})


def _code(bd, tmp_path, faux_modeles, fichiers):
    d = tmp_path / "code"
    for chemin, texte in fichiers.items():
        (d / chemin).parent.mkdir(parents=True, exist_ok=True)
        (d / chemin).write_text(texte)
    indexer_source(bd, SourceConf(visibilite="public-maison", nom="code", type="code",
                                  emplacement="x"), LecteurDossier(d), CONFIG.indexation,
                   faux_modeles)


def scores(bd, q, conf, faux_modeles):
    v = version_courante(bd, CONFIG.indexation)
    return {r.cle: r.score for r in chercher(bd, q, conf, faux_modeles, v, "m", niveaux=PUBLIC,
                                             limite=50)}


def test_poids_absent_vaut_un(bd, tmp_path, faux_modeles):
    assert charger_config(Path("config/rag.yaml")).recherche.model_copy(
        update={"poids_source": {}}).poids_source == {}
    indexer_coffre(bd, tmp_path, faux_modeles, {"a.md": "# Poulet\n\nRecette de poulet."})
    _code(bd, tmp_path, faux_modeles, {"poulet.py": "def poulet():\n    return 'poulet'\n"})
    assert scores(bd, "poulet", avec(), faux_modeles) == scores(
        bd, "poulet", avec(obsidian=1.0, code=1.0), faux_modeles)


def test_le_poids_du_code_multiplie_ses_scores(bd, tmp_path, faux_modeles):
    indexer_coffre(bd, tmp_path, faux_modeles, {"a.md": "# Poulet\n\nRecette de poulet."})
    _code(bd, tmp_path, faux_modeles, {"poulet.py": "def poulet():\n    return 'poulet'\n"})
    un, demi = (scores(bd, "poulet", avec(code=x), faux_modeles) for x in (1.0, 0.5))
    assert demi["coffre-maison:a.md"] == pytest.approx(un["coffre-maison:a.md"])
    assert demi["code:poulet.py"] == pytest.approx(un["code:poulet.py"] * 0.5)


def test_les_chemins_de_code_ne_remplissent_pas_la_liste_floue(bd, tmp_path, faux_modeles):
    _code(bd, tmp_path, faux_modeles, {f"{i}/Infra - Stack Keycloak.md": f"Texte {i}." for i in range(30)})
    indexer_coffre(bd, tmp_path, faux_modeles, {  # indexée après : perd les égalités par id
        "Infra - Stack Keycloak.md": "# Stack Keycloak\n\nSur le serveur principal."})
    sans_vecteur = R.model_copy(update={"top_vecteur": 0, "poids": R.poids.model_copy(
        update={"vecteur": 0.0})})
    assert "coffre-maison:Infra - Stack Keycloak.md" in scores(bd, "keycloack", sans_vecteur,
                                                               faux_modeles)


def test_type_inconnu_refuse():
    with pytest.raises(ValueError):
        R.model_validate(R.model_dump() | {"poids_source": {"autre": 1.0}})
