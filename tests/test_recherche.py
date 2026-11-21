from datetime import date
from functools import partial
from pathlib import Path

import pytest

from rag.config import PUBLIC, TOUT, Poids, charger_config
from rag.recherche import Filtres, chercher, lien_source

CONF = charger_config(Path("config/rag.yaml"))
M = CONF.indexation.modele_embedding
POULET = "02 - Domaines/Cuisine/Recettes/Poulet cidre et Boursin.md"


def ch(bd, q, index, fm, conf=None, filtres=None, limite=10, niveaux=PUBLIC):
    return chercher(bd, q, conf or CONF.recherche, fm, index.version, M, niveaux=niveaux,
                    filtres=filtres, limite=limite)


def test_trouve_par_le_sens_et_par_les_mots(bd, index, faux_modeles):
    r = ch(bd, "recette de poulet au cidre", index, faux_modeles)
    assert r[0].chemin == POULET and r[0].score > r[1].score
    assert r[0].cle == "coffre-maison:" + r[0].chemin
    assert r[0].lien.startswith("obsidian://open?vault=Maison&file=")
    assert r[0].origine == "claude" and 0 < r[0].cosinus <= 1


def test_sans_accents(bd, index, faux_modeles):
    assert ch(bd, "pate chinois", index, faux_modeles)[0].titre == "Pâté chinois"


def test_flou_sur_un_titre_mal_tape(bd, index, faux_modeles):
    assert ch(bd, "grafanna", index, faux_modeles)[0].titre == "Grafana"
    sans = CONF.recherche.model_copy(update={"flou": False,
                                             "poids": Poids(vecteur=0, texte=1, flou=0)})
    assert all(x.titre != "Grafana" for x in ch(bd, "grafanna", index, faux_modeles, sans))


@pytest.mark.parametrize("q", ["?", "c'est quoi ça", "a & b | c : d", "", "   ", "l'",
                               "\\ ' \" ;", "!!(&|:*)"])
def test_question_sans_mot_utile_ne_plante_pas(bd, index, faux_modeles, q):
    assert isinstance(ch(bd, q, index, faux_modeles), list)


def test_question_vide_n_appelle_pas_le_modele(bd, index, faux_modeles):
    n = faux_modeles.appels
    assert ch(bd, "  ", index, faux_modeles) == [] and faux_modeles.appels == n


def test_filtres(bd, index, faux_modeles):
    def f(**k):
        return {x.chemin for x in ch(bd, "note", index, faux_modeles, filtres=Filtres(**k),
                                     limite=50)}
    assert f(dossier="02 - Domaines/") and all(
        c.startswith("02 - Domaines/") for c in f(dossier="02 - Domaines/"))
    assert f(dossier="02 - Domaines/Cuisine/Recettes/P%") == set()  # pas de joker LIKE
    assert f(type="ressource") == {"03 - Ressources/Grafana.md"}
    assert f(tags=["poulet", "recette"]) == {POULET} and f(tags=["poulet", "absent"]) == set()
    assert f(depuis=date(2026, 1, 1)).isdisjoint({"03 - Ressources/Kubernetes.md"})
    assert "03 - Ressources/Kubernetes.md" in f(jusqua=date(2025, 12, 31))
    assert f(sources=["inconnue"]) == set() and f(sources=["coffre-maison"])


def test_source_privee_invisible_sans_le_niveau(bd, index, faux_modeles):
    bd.execute("UPDATE sources SET visibilite = 'prive'")
    q = partial(ch, bd, "recette de poulet", index, faux_modeles)
    assert q(niveaux=PUBLIC) == []
    assert q(niveaux=TOUT)
    assert q(niveaux=frozenset()) == []


def test_niveaux_est_obligatoire(bd, index, faux_modeles):
    with pytest.raises(TypeError):
        chercher(bd, "x", CONF.recherche, faux_modeles, index.version, M)


def test_poids_des_branches_reglables(bd, index, faux_modeles):
    seul_vecteur = CONF.recherche.model_copy(update={"flou": False,
                                                     "poids": Poids(vecteur=1, texte=0, flou=0)})
    r = ch(bd, "tableaux de bord", index, faux_modeles, seul_vecteur, limite=50)
    cos = [x.cosinus for x in r]
    assert cos == sorted(cos, reverse=True)


def test_version_isolee(bd, index, faux_modeles):
    from rag.config import SourceConf
    from rag.indexeur import indexer_source
    from rag.sources import LecteurDossier

    avant = [(x.morceau_id, x.score) for x in ch(bd, "poulet", index, faux_modeles)]
    conf = CONF.indexation.model_copy(update={"strategie": "recursif"})
    src = SourceConf(nom="coffre-maison", type="obsidian", emplacement=str(index.coffre))
    indexer_source(bd, src, LecteurDossier(index.coffre), conf, faux_modeles)
    assert [(x.morceau_id, x.score) for x in ch(bd, "poulet", index, faux_modeles)] == avant


def test_parent_enfant_rend_la_section_entiere(bd, index_pe, faux_modeles):
    long = "\n\n".join(f"Paragraphe {i} sur le poulet rôti. " + "mot " * 120 for i in range(4))
    (index_pe.coffre / "Long.md").write_text(f"# Section\n\n{long}")
    from rag.config import SourceConf
    from rag.indexeur import indexer_source
    from rag.sources import LecteurDossier

    conf = CONF.indexation.model_copy(update={"strategie": "parent_enfant"})
    src = SourceConf(nom="coffre-maison", type="obsidian", emplacement=str(index_pe.coffre))
    indexer_source(bd, src, LecteurDossier(index_pe.coffre), conf, faux_modeles)
    r = [x for x in ch(bd, "poulet rôti paragraphe", index_pe, faux_modeles, limite=50)
         if x.chemin == "Long.md"]
    assert len(r) == 1 and r[0].texte == long.strip()


def test_lien_code():
    assert lien_source("code", "Infra", "litellm/config.yaml", 12) == "Infra/litellm/config.yaml#L12"
    assert lien_source("code", "Infra", "a.py", None) == "Infra/a.py"
    assert lien_source("obsidian", "c", "03 - Ressources/Grafana.md", None) == (
        "obsidian://open?vault=Maison&file=03%20-%20Ressources/Grafana")


def test_ordre_reproductible_a_egalite(bd, index, faux_modeles):
    for _ in range(2):
        (index.coffre / f"Double{_}.md").write_text("# Pareil\n\nTexte identique.")
    from rag.config import SourceConf
    from rag.indexeur import indexer_source
    from rag.sources import LecteurDossier
    indexer_source(bd, SourceConf(nom="coffre-maison", type="obsidian", emplacement="x"),
                   LecteurDossier(index.coffre), CONF.indexation, faux_modeles)
    r = [ch(bd, "texte identique", index, faux_modeles) for _ in range(3)]
    ids = [[x.morceau_id for x in y] for y in r]
    assert ids[0] == ids[1] == ids[2]
    doubles = [x for x in r[0] if x.chemin.startswith("Double")]
    assert doubles[0].morceau_id < doubles[1].morceau_id   # égalité départagée par id
