from pathlib import Path

from rag.config import PUBLIC, TOUT, charger_config
from rag.lister import lister, taxonomie
from rag.recherche import Filtres
from tests.conftest import indexer_coffre

BASE = charger_config(Path("config/rag.yaml"))
CONFIG = BASE.model_copy(update={"reponse": BASE.reponse.model_copy(
    update={"seuil_refus_cosinus": None, "seuil_refus_rerank": None})})
Q = "quelles recettes j'ai pour le poulet"
POULET = Filtres(type="recette", tags=["poulet"])


def test_exhaustif_par_metadonnees(bd, index_recettes, faux_modeles):
    r = lister(bd, Q, POULET, CONFIG, faux_modeles, index_recettes, niveaux=PUBLIC)
    tagues = [e for e in r["elements"] if e["certitude"] == "tague"]
    assert r["mode"] == "metadonnees"
    assert {e["titre"] for e in tagues} == {"Poulet cidre et Boursin", "Poulet au beurre"}
    assert all(e["lien"].startswith("obsidian://") and "origine" in e for e in r["elements"])
    assert r["total"] == len(r["elements"]) and r["filtres"]["type"] == "recette"


def test_complement_probable_non_tague(bd, index_recettes, faux_modeles):
    r = lister(bd, Q, POULET, CONFIG, faux_modeles, index_recettes, niveaux=PUBLIC)
    prob = [e["titre"] for e in r["elements"] if e["certitude"] == "probable"]
    assert "Idées de soupers" in prob
    assert r["elements"][-1]["certitude"] == "probable"
    assert r["elements"][0]["certitude"] == "tague"


def test_pas_de_doublon_entre_tague_et_probable(bd, index_recettes, faux_modeles):
    r = lister(bd, Q, POULET, CONFIG, faux_modeles, index_recettes, niveaux=PUBLIC)
    cles = [e["cle"] for e in r["elements"]]
    assert len(cles) == len(set(cles))


def test_le_complement_respecte_le_refus(bd, index_recettes, faux_modeles):
    c = CONFIG.model_copy(update={"reponse": CONFIG.reponse.model_copy(
        update={"seuil_refus_cosinus": 0.999})})
    r = lister(bd, Q, POULET, c, faux_modeles, index_recettes, niveaux=PUBLIC)
    assert {e["certitude"] for e in r["elements"]} == {"tague"}


def test_sans_type_regroupe_par_document(bd, index_mixte, faux_modeles):
    r = lister(bd, "quelles sont mes dashboards Grafana", Filtres(), CONFIG, faux_modeles,
               index_mixte, niveaux=PUBLIC)
    assert r["mode"] == "regroupement"
    assert len({e["cle"] for e in r["elements"]}) == len(r["elements"])
    assert any(e["cle"].startswith("Infra:monitoring/grafana/") for e in r["elements"])
    assert all(e["certitude"] == "probable" and e["extrait"] for e in r["elements"])


def test_filtre_qui_ne_donne_rien(bd, index_recettes, faux_modeles):
    r = lister(bd, Q, Filtres(type="inexistant"), CONFIG, faux_modeles, index_recettes,
               niveaux=PUBLIC)
    assert r["mode"] == "metadonnees" and all(e["certitude"] == "probable" for e in r["elements"])


def test_plus_de_30_elements_tagues_tous_rendus(bd, tmp_path, faux_modeles):
    from rag.indexeur import version_courante
    notes = {f"R/Recette {i:02}.md": f"---\ntype: recette\n---\n# Recette {i:02}\n\nPlat {i}.\n"
             for i in range(40)}
    indexer_coffre(bd, tmp_path, faux_modeles, notes)
    v = version_courante(bd, CONFIG.indexation)
    r = lister(bd, "mes recettes", Filtres(type="recette"), CONFIG, faux_modeles, v,
               niveaux=PUBLIC)
    assert len([e for e in r["elements"] if e["certitude"] == "tague"]) == 40


def test_source_privee_invisible(bd, index_recettes, faux_modeles):
    bd.execute("UPDATE sources SET visibilite = 'prive'")
    r = lister(bd, Q, POULET, CONFIG, faux_modeles, index_recettes, niveaux=PUBLIC)
    assert r["elements"] == [] and r["total"] == 0
    assert lister(bd, Q, POULET, CONFIG, faux_modeles, index_recettes, niveaux=TOUT)["total"]
    assert taxonomie(bd, niveaux=PUBLIC) == {"types": {}, "tags": {}, "sources": []}


def test_taxonomie(bd, index_mixte):
    t = taxonomie(bd, niveaux=PUBLIC)
    assert t["types"]["recette"] == 3 and t["tags"]["poulet"] == 2
    assert sorted(t["sources"]) == ["Infra", "coffre-maison"]


def test_troncature_signalee(bd, tmp_path, faux_modeles, monkeypatch):
    from rag import lister as mod
    from rag.indexeur import version_courante
    monkeypatch.setattr(mod, "PLAFOND", 3)
    notes = {f"R/R{i}.md": f"---\ntype: recette\n---\n# R{i}\n\nPlat {i}.\n" for i in range(5)}
    indexer_coffre(bd, tmp_path, faux_modeles, notes)
    v = version_courante(bd, CONFIG.indexation)
    r = lister(bd, "x", Filtres(type="recette"), CONFIG, faux_modeles, v, niveaux=PUBLIC)
    assert r["tronque"] is True and [e["certitude"] for e in r["elements"]].count("tague") == 3
    r = lister(bd, "x", Filtres(), CONFIG, faux_modeles, v, niveaux=PUBLIC)
    assert r["tronque"] is False
