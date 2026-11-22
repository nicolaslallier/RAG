from rag.graphe import charger_wikiliens
from tests.conftest import indexer_coffre

NOTES = {"a.md": "# A\n\n[[B]] [[Mon alias]] [[dossier/c]] [[Absente]]",
         "b.md": "# B\n\nBêta.",
         "d.md": "---\naliases: [Mon alias]\n---\n# D\n\nDelta.",
         "dossier/c.md": "# C\n\nGamma."}


def liens(bd):
    return {(r["de"], r["vers"]) for r in bd.execute(
        "SELECT a.cle de, b.cle vers FROM liens l JOIN noeuds a ON a.id=l.de "
        "JOIN noeuds b ON b.id=l.vers WHERE l.origine='wikilien'")}


def test_liens_resolus_par_titre_alias_et_chemin(bd, tmp_path, faux_modeles):
    indexer_coffre(bd, tmp_path, faux_modeles, NOTES)
    assert charger_wikiliens(bd, "coffre-maison") == (4, 3, 1)
    assert liens(bd) == {("a.md", "b.md"), ("a.md", "d.md"), ("a.md", "dossier/c.md")}
    assert bd.execute("SELECT DISTINCT confiance, type FROM liens").fetchall() == [
        {"confiance": "EXTRAIT", "type": "lien"}]


def test_sans_accents_ni_casse(bd, tmp_path, faux_modeles):
    indexer_coffre(bd, tmp_path, faux_modeles, {"r/Pâté chinois.md": "# Pâté chinois\n\nx",
                                                "a.md": "# A\n\n[[pate CHINOIS]]"})
    assert charger_wikiliens(bd, "coffre-maison") == (2, 1, 0)


def test_homonymes_meme_dossier_d_abord(bd, tmp_path, faux_modeles):
    indexer_coffre(bd, tmp_path, faux_modeles, {
        "x/Index.md": "# Index\n\nx", "y/Index.md": "# Index\n\ny",
        "y/a.md": "# A\n\n[[Index]]", "z/b.md": "# B\n\n[[Index]]"})
    assert charger_wikiliens(bd, "coffre-maison")[1:] == (1, 1)
    assert liens(bd) == {("y/a.md", "y/Index.md")}


def test_note_supprimee_retire_son_noeud(bd, tmp_path, faux_modeles):
    coffre = indexer_coffre(bd, tmp_path, faux_modeles, NOTES)
    charger_wikiliens(bd, "coffre-maison")
    (coffre / "b.md").unlink()
    indexer_coffre(bd, tmp_path, faux_modeles, {})
    assert charger_wikiliens(bd, "coffre-maison") == (3, 2, 2)
    assert ("a.md", "b.md") not in liens(bd)
    assert bd.execute("SELECT count(*) n FROM noeuds").fetchone()["n"] == 3


def test_idempotent(bd, tmp_path, faux_modeles):
    indexer_coffre(bd, tmp_path, faux_modeles, NOTES)
    assert charger_wikiliens(bd, "coffre-maison") == charger_wikiliens(bd, "coffre-maison")
    assert len(liens(bd)) == 3
