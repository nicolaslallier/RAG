from pathlib import Path

import pytest

from rag.config import PUBLIC, charger_config
from rag.graphe import charger_wikiliens
from rag.indexeur import version_courante
from rag.recherche import Filtres
from rag.recherche import chercher as _chercher
from tests.conftest import indexer_coffre

CONF = charger_config(Path("config/rag.yaml"))
# peu de candidats : seul le plein texte trouve les notes qui parlent de poulet
AVEC = CONF.recherche.model_copy(update={"top_vecteur": 1, "flou": False,
                                         "graphe": CONF.recherche.graphe.model_copy(
                                             update={"expansion": True})})
NOTES = {
    "Poulet.md": "---\ntype: recette\n---\n# Poulet au cidre\n\nRecette de poulet au cidre. "
                 "[[Cuisine]] " + " ".join(f"[[V{i}]]" for i in range(9)),
    "Cuisine.md": "---\ntype: ressource\n---\n# Cuisine\n\nLa pièce où l'on mijote. [[Loin]]",
    "Loin.md": "# Loin\n\nÀ deux sauts.",
    "Sauce.md": "---\ntype: recette\n---\n# Sauce\n\nSauce brune. [[Poulet]]",
    **{f"V{i}.md": f"# V{i}\n\nVoisin numéro {i}." for i in range(9)},
}


def chercher(bd, q, conf, modeles, v, limite=20, **k):
    return _chercher(bd, q, conf, modeles, v, "m", niveaux=k.pop("niveaux", PUBLIC),
                     limite=limite, **k)


@pytest.fixture
def index_lie(bd, tmp_path, faux_modeles):
    indexer_coffre(bd, tmp_path, faux_modeles, NOTES)
    charger_wikiliens(bd, "coffre-maison")
    return version_courante(bd, CONF.indexation)


def test_desactivee_par_defaut(bd, index_lie, faux_modeles):
    assert CONF.recherche.graphe.expansion is False
    assert all(r.via == "hybride" for r in chercher(bd, "poulet", CONF.recherche, faux_modeles,
                                                    index_lie))
    for f in ("config/rag.yaml", "config/dev.yaml"):
        assert charger_config(Path(f)).recherche.graphe.expansion is False


def test_ajoute_le_voisin_a_un_saut(bd, index_lie, faux_modeles):
    r = chercher(bd, "recette de poulet au cidre", AVEC, faux_modeles, index_lie, limite=30)
    v = next(x for x in r if x.chemin == "Cuisine.md")
    assert v.via == "graphe" and v.score == pytest.approx(r[0].score * 0.5)
    assert r == sorted(r, key=lambda x: -x.score)


def test_plafond_et_pas_de_doublon(bd, index_lie, faux_modeles):
    r = chercher(bd, "recette de poulet au cidre", AVEC, faux_modeles, index_lie, limite=30)
    assert len([x for x in r if x.via == "graphe"]) == 5
    assert len({x.document_id for x in r}) == len(r)


def test_deux_sauts_non_suivis(bd, index_lie, faux_modeles):
    seul = AVEC.model_copy(update={"graphe": AVEC.graphe.model_copy(update={"depuis": 1})})
    r = chercher(bd, "recette de poulet au cidre", seul, faux_modeles, index_lie, limite=30)
    assert r[0].chemin == "Poulet.md" and "Loin.md" not in {x.chemin for x in r if x.via == "graphe"}


def test_respecte_les_filtres(bd, index_lie, faux_modeles):
    r = chercher(bd, "recette de poulet au cidre", AVEC, faux_modeles, index_lie,
                 filtres=Filtres(type="recette"))
    assert {x.chemin for x in r} <= {"Poulet.md", "Sauce.md"}


def test_n_ajoute_rien_d_une_source_privee(bd, index_lie, faux_modeles, tmp_path):
    indexer_coffre(bd, tmp_path, faux_modeles, {"Secret.md": "# Secret\n\nCaché."}, nom="perso")
    bd.execute("UPDATE sources SET visibilite='prive' WHERE nom='perso'")
    charger_wikiliens(bd, "perso")
    bd.execute("""INSERT INTO liens (de, vers, type, confiance, origine)
                  SELECT a.id, b.id, 'lien', 'EXTRAIT', 'wikilien' FROM noeuds a, noeuds b
                   WHERE a.cle='Poulet.md' AND b.cle='Secret.md'""")
    bd.commit()
    r = chercher(bd, "recette de poulet au cidre", AVEC, faux_modeles, index_lie, limite=30)
    assert "Secret.md" not in {x.chemin for x in r}
