import os
import unicodedata
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rag.config import SourceConf, charger_config
from rag.indexeur import CoffrePartielError, indexer_source
from rag.modeles import ErreurModele
from rag.sources import Entree, LecteurDossier
from tests.conftest import FauxModeles

CONF = charger_config(Path("config/rag.yaml")).indexation
SRC = SourceConf(nom="coffre-test", type="obsidian", emplacement="/tmp/x",
                 exclure=[".obsidian/**"])


@pytest.fixture
def coffre(tmp_path):
    d = tmp_path / "coffre"
    d.mkdir()
    (d / "a.md").write_text("# Alpha\n\nLa note alpha parle de [[b]] et de cuisine.")
    (d / "b.md").write_text("---\ntags: [beta]\n---\n# Bêta\n\nUne note sur le zèbre.")
    (d / "c.md").write_text("# Gamma\n\nRecette de pâté chinois.")
    (d / "d.md").write_text("Delta sans titre.")
    return d


def chemins(bd):
    return {r["chemin"] for r in bd.execute("SELECT chemin FROM documents")}


def ids_morceaux(bd, chemin):
    return {r["id"] for r in bd.execute(
        "SELECT m.id FROM morceaux m JOIN documents d ON d.id=m.document_id WHERE d.chemin=%s",
        (chemin,))}


class FauxModelesQuiTombe(FauxModeles):
    def __init__(self, apres):
        super().__init__()
        self.apres = apres

    def embed_documents(self, textes, modele):
        if self.appels >= self.apres:
            raise ErreurModele("panne")
        return super().embed_documents(textes, modele)


def test_premiere_passe_puis_rien_a_faire(bd, coffre, faux_modeles):
    b1 = indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    assert (b1.nouveaux, b1.modifies, b1.supprimes) == (4, 0, 0) and b1.morceaux >= 4
    n = faux_modeles.appels
    b2 = indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    assert b2.inchanges == 4 and faux_modeles.appels == n


def test_note_modifiee_seule_retraitee(bd, coffre, faux_modeles):
    indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    avant = {c: ids_morceaux(bd, c) for c in ("b.md", "c.md", "d.md")}
    (coffre / "a.md").write_text("# Alpha\n\nTout autre chose, plus long qu'avant.")
    b = indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    assert (b.modifies, b.inchanges) == (1, 3)
    assert avant == {c: ids_morceaux(bd, c) for c in avant}


def test_touch_sans_changement_ne_recalcule_rien(bd, coffre, faux_modeles):
    indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    n = faux_modeles.appels
    for f in coffre.iterdir():
        os.utime(f, (1e9, 1e9))
    b = indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    assert b.inchanges == 4 and faux_modeles.appels == n
    assert bd.execute("SELECT signature FROM documents WHERE chemin='a.md'").fetchone()[
        "signature"].startswith("1000000000")


def test_suppression_et_renommage(bd, coffre, faux_modeles):
    indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    (coffre / "a.md").rename(coffre / "z.md")
    (coffre / "b.md").unlink()
    b = indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    assert (b.nouveaux, b.supprimes) == (1, 2)
    assert chemins(bd) == {"c.md", "d.md", "z.md"}
    assert bd.execute("SELECT count(*) n FROM morceaux m JOIN documents d ON d.id=m.document_id "
                      "WHERE d.chemin IN ('a.md','b.md')").fetchone()["n"] == 0


def test_coffre_partiel_refuse(bd, coffre, faux_modeles):
    indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    for f in ("a.md", "b.md", "c.md"):
        (coffre / f).unlink()
    with pytest.raises(CoffrePartielError):
        indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    assert len(chemins(bd)) == 4
    b = indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles, forcer=True)
    assert b.supprimes == 3


def test_listage_vide_refuse(bd, coffre, faux_modeles):
    indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    for f in coffre.iterdir():
        f.unlink()
    with pytest.raises(CoffrePartielError):
        indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    assert len(chemins(bd)) == 4


def test_note_recente_reportee(bd, coffre, faux_modeles):
    src = SRC.model_copy(update={"repos_minutes": 10})
    b = indexer_source(bd, src, LecteurDossier(coffre), CONF, faux_modeles,
                       maintenant=datetime.now(UTC))
    assert b.reportes == 4 and b.nouveaux == 0
    b = indexer_source(bd, src, LecteurDossier(coffre), CONF, faux_modeles, sans_repos=True)
    assert b.nouveaux == 4


class LecteurFixe:
    def __init__(self, chemin):
        self.chemin = chemin

    def lister(self):
        return [Entree(self.chemin, "s1", datetime(2026, 1, 1, tzinfo=UTC), 5)]

    def lire(self, chemin):
        return b"# Modeles\n\ncontenu"

    def revision(self):
        return "r"


def test_chemin_nfd_puis_nfc_meme_document(bd, faux_modeles):
    nom = "Modèles/é.md"
    indexer_source(bd, SRC, LecteurFixe(unicodedata.normalize("NFD", nom)), CONF, faux_modeles)
    b = indexer_source(bd, SRC, LecteurFixe(unicodedata.normalize("NFC", nom)), CONF,
                       faux_modeles)
    assert b.inchanges == 1 and chemins(bd) == {unicodedata.normalize("NFC", nom)}


def test_panne_du_modele_ne_laisse_pas_de_demi_document(bd, coffre, faux_modeles):
    m = FauxModelesQuiTombe(apres=2)
    with pytest.raises(ErreurModele):
        indexer_source(bd, SRC, LecteurDossier(coffre), CONF, m)
    assert bd.execute("SELECT count(*) n FROM documents d WHERE NOT EXISTS "
                      "(SELECT 1 FROM morceaux WHERE document_id=d.id)").fetchone()["n"] == 0
    assert len(chemins(bd)) == 2
    assert indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles).nouveaux == 2


def test_deux_versions_cote_a_cote(bd, coffre, faux_modeles):
    indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    indexer_source(bd, SRC, LecteurDossier(coffre), CONF.model_copy(update={"strategie": "recursif"}),
                   faux_modeles)
    assert bd.execute("SELECT count(DISTINCT version_id) n FROM morceaux").fetchone()["n"] == 2
    assert len(chemins(bd)) == 4


def test_visibilite_suit_la_config(bd, coffre, faux_modeles):
    indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    indexer_source(bd, SRC.model_copy(update={"visibilite": "prive"}), LecteurDossier(coffre),
                   CONF, faux_modeles)
    assert bd.execute("SELECT visibilite FROM sources").fetchone()["visibilite"] == "prive"


def test_liens_sortants_stockes(bd, coffre, faux_modeles):
    indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    r = bd.execute("SELECT liens_sortants l FROM documents WHERE chemin='a.md'").fetchone()
    assert r["l"] == ["b"]


def test_secret_dans_une_note_exclu(bd, coffre, faux_modeles):
    (coffre / "e.md").write_text("cle: sk-abcdefghijklmnopqrstuvwx")
    b = indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    assert b.exclus == 1 and "e.md" not in chemins(bd)


def test_tsv_poids_a_sur_le_fil(bd, coffre, faux_modeles):
    (coffre / "Zèbre.md").write_text("# Savane\n\nUn animal rayé.")
    indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    r = bd.execute("""SELECT d.chemin FROM morceaux m JOIN documents d ON d.id=m.document_id
                      WHERE m.tsv @@ plainto_tsquery('fr','zebre')
                      ORDER BY ts_rank(m.tsv, plainto_tsquery('fr','zebre')) DESC""").fetchall()
    assert [x["chemin"] for x in r] == ["Zèbre.md", "b.md"]


def test_parent_sans_embedding(bd, coffre, faux_modeles):
    pe = CONF.model_copy(update={"strategie": "parent_enfant"})
    indexer_source(bd, SRC, LecteurDossier(coffre), pe, faux_modeles)
    r = bd.execute("""SELECT bool_and(embedding IS NULL AND tsv IS NULL) FILTER
                        (WHERE parent_id IS NULL) p,
                      bool_and(embedding IS NOT NULL) FILTER (WHERE parent_id IS NOT NULL) e,
                      count(*) FILTER (WHERE parent_id IS NOT NULL) n FROM morceaux""").fetchone()
    assert r["p"] and r["e"] and r["n"] >= 4


def test_cle_privee_loin_dans_le_fichier_exclue(bd, coffre, faux_modeles):
    (coffre / "f.md").write_text("texte " * 2000 + "\n-----BEGIN RSA PRIVATE KEY-----\nabc\n")
    b = indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    assert b.exclus == 1 and "f.md" not in chemins(bd)


def test_seules_les_notes_md_d_un_coffre(bd, coffre, faux_modeles):
    (coffre / ".gitignore").write_text("x")
    (coffre / "image.png").write_bytes(b"\x89PNG\x00")
    indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    assert chemins(bd) == {"a.md", "b.md", "c.md", "d.md"}


def test_note_devenue_secret_retiree_de_toutes_les_versions(bd, coffre, faux_modeles,
                                                            monkeypatch):
    import rag.config as rc
    import rag.indexeur as ri
    indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    indexer_source(bd, SRC, LecteurDossier(coffre), CONF.model_copy(update={"strategie": "recursif"}),
                   faux_modeles)
    # nouvelle règle d'exclusion qui juge c.md secret, avec la version d'analyseur incrémentée
    regle = ri.motif_exclusion
    monkeypatch.setattr(ri, "motif_exclusion",
                        lambda ch, d, ex: "secret" if ch == "c.md" and d else regle(ch, d, ex))
    monkeypatch.setattr(rc, "VERSION_ANALYSEUR", rc.VERSION_ANALYSEUR + 1)
    b = indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    assert b.exclus == 1 and "c.md" not in chemins(bd)
    assert bd.execute("SELECT count(*) n FROM morceaux m JOIN documents d ON d.id=m.document_id "
                      "WHERE d.chemin='c.md'").fetchone()["n"] == 0


def test_secret_journalise_et_pas_relu(bd, coffre, faux_modeles, caplog, monkeypatch):
    (coffre / "e.md").write_text("cle: sk-abcdefghijklmnopqrstuvwx")
    with caplog.at_level("WARNING"):
        indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    assert "e.md" in caplog.text and "sk-abc" not in caplog.text
    lus = []
    lec = LecteurDossier(coffre)
    lire = lec.lire
    monkeypatch.setattr(lec, "lire", lambda c: lus.append(c) or lire(c))
    b = indexer_source(bd, SRC, lec, CONF, faux_modeles)
    assert b.exclus == 1 and lus == []
    (coffre / "e.md").write_text("plus de secret")
    b = indexer_source(bd, SRC, lec, CONF, faux_modeles)
    assert b.nouveaux == 1 and "e.md" in chemins(bd)
