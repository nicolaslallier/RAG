from pathlib import Path

from rag.config import PUBLIC, SourceConf, charger_config
from rag.entretien import doublons, liens_entrants, orphelines
from rag.graphe import charger_wikiliens
from rag.indexeur import indexer_source, version_courante
from rag.sources import LecteurDossier
from tests.conftest import indexer_coffre

CONF = charger_config(Path("config/rag.yaml")).indexation
TEXTE = "# Bouillon\n\nBouillon de poulet, carottes, céleri, oignon, laurier et thym mijotés."


def v(bd):
    return version_courante(bd, CONF)


def test_doublons(bd, tmp_path, faux_modeles):
    indexer_coffre(bd, tmp_path, faux_modeles, {
        "a.md": TEXTE, "a-copie.md": TEXTE,
        "b.md": TEXTE.replace("thym", "persil"),
        "c.md": "# Kubernetes\n\nOrchestration de conteneurs en grappe."})
    d = doublons(bd, v(bd), niveaux=PUBLIC)
    assert {frozenset((x["a"]["cle"], x["b"]["cle"])) for x in d if x["similarite"] == 1.0} == {
        frozenset(("coffre-maison:a.md", "coffre-maison:a-copie.md"))}
    assert all(x["similarite"] >= 0.92 for x in d)
    assert all("c.md" not in (x["a"]["cle"] + x["b"]["cle"]) for x in d)
    assert len(doublons(bd, v(bd), seuil=0.5, niveaux=PUBLIC)) > len(d)
    assert d[0]["a"]["lien"].startswith("obsidian://")


def test_doublons_ignore_le_code(bd, tmp_path, faux_modeles):
    d = tmp_path / "code"
    d.mkdir()
    (d / "x.py").write_text("def f():\n    return 1\n")
    (d / "y.py").write_text("def f():\n    return 1\n")
    src = SourceConf(visibilite="public-maison", nom="code", type="code", emplacement="x")
    indexer_source(bd, src, LecteurDossier(d), CONF, faux_modeles)
    assert doublons(bd, v(bd), niveaux=PUBLIC) == []
    assert orphelines(bd, niveaux=PUBLIC) == []


def test_orphelines_et_liens_entrants(bd, tmp_path, faux_modeles):
    indexer_coffre(bd, tmp_path, faux_modeles, {"a.md": "# A\n\n[[b]]", "b.md": "# B\n\nx",
                                                "c.md": "# C\n\ny"})
    charger_wikiliens(bd, "coffre-maison")
    assert [o["cle"] for o in orphelines(bd, niveaux=PUBLIC)] == ["coffre-maison:c.md"]
    r = liens_entrants(bd, "coffre-maison:b.md", niveaux=PUBLIC)
    assert r["etat"] == "ok" and [e["cle"] for e in r["entrants"]] == ["coffre-maison:a.md"]
    assert liens_entrants(bd, "coffre-maison:a.md", niveaux=PUBLIC)["entrants"] == []
    assert liens_entrants(bd, "coffre-maison:absente.md", niveaux=PUBLIC)["etat"] == "introuvable"


def test_source_privee_invisible(bd, tmp_path, faux_modeles):
    indexer_coffre(bd, tmp_path, faux_modeles, {"a.md": TEXTE, "a2.md": TEXTE,
                                                "b.md": "# B\n\n[[a]]"})
    charger_wikiliens(bd, "coffre-maison")
    bd.execute("UPDATE sources SET visibilite='prive'")
    bd.commit()
    assert doublons(bd, v(bd), niveaux=PUBLIC) == [] and orphelines(bd, niveaux=PUBLIC) == []
    assert liens_entrants(bd, "coffre-maison:a.md", niveaux=PUBLIC)["etat"] == "introuvable"
