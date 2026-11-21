import base64

import pytest

from rag.sources import ErreurSource, LecteurGit
from tests.conftest import commit, git


def test_clone_puis_suit_origin_main(depot, tmp_path):
    lec = LecteurGit(str(depot), tmp_path / "clone")
    sha1 = lec.synchroniser()
    assert {e.chemin for e in lec.lister()} >= {"a.py", "docs/guide.md"}
    commit(depot, "b.py", "def b(): pass")
    l2 = LecteurGit(str(depot), tmp_path / "clone")
    assert l2.synchroniser() != sha1 and "b.py" in {e.chemin for e in l2.lister()}


def test_suit_une_reecriture_d_historique(depot, tmp_path):
    commit(depot, "b.py", "def b(): pass")
    LecteurGit(str(depot), tmp_path / "clone").synchroniser()
    (depot / "b.py").write_text("def c(): pass")
    git(depot, "commit", "-qa", "--amend", "-m", "réécrit")
    lec = LecteurGit(str(depot), tmp_path / "clone")
    assert lec.synchroniser() == git(depot, "rev-parse", "HEAD")
    assert lec.lire("b.py") == b"def c(): pass"


def test_une_autre_branche_n_est_jamais_lue(depot, tmp_path):
    git(depot, "switch", "-qc", "travail")
    commit(depot, "secret_de_travail.py", "x = 1")
    git(depot, "switch", "-q", "main")
    lec = LecteurGit(str(depot), tmp_path / "clone")
    assert "secret_de_travail.py" not in {e.chemin for e in lec.lister()}
    assert git(tmp_path / "clone", "branch", "-a").count("travail") == 0


def test_signature_est_le_sha_du_blob(depot, tmp_path):
    lec = LecteurGit(str(depot), tmp_path / "clone")
    e = next(e for e in lec.lister() if e.chemin == "a.py")
    assert e.signature == git(depot, "rev-parse", "HEAD:a.py") and e.taille > 0
    assert lec.revision() == git(depot, "rev-parse", "HEAD")


def test_sous_module_ignore(depot, tmp_path):
    sha = git(depot, "rev-parse", "HEAD")
    git(depot, "update-index", "--add", "--cacheinfo", f"160000,{sha},vendor/lib")
    git(depot, "commit", "-qm", "sous-module")
    assert "vendor/lib" not in {e.chemin for e in LecteurGit(str(depot), tmp_path / "c").lister()}


def test_le_jeton_ne_fuit_pas(tmp_path):
    with pytest.raises(ErreurSource) as e:
        LecteurGit("https://invalide.invalid/x.git", tmp_path / "c",
                   jeton="ghp_secret123").synchroniser()
    assert "ghp_secret123" not in str(e.value)
    assert base64.b64encode(b"x-access-token:ghp_secret123").decode() not in str(e.value)


def test_le_jeton_est_masque_dans_l_erreur(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", str(tmp_path))  # « git » factice qui répète son environnement
    lec = LecteurGit("x", tmp_path / "c", jeton="ghp_secret123")
    (tmp_path / "git").write_text("#!/bin/sh\necho \"$GIT_CONFIG_VALUE_0 ghp_secret123\" >&2\n"
                                  "exit 1\n")
    (tmp_path / "git").chmod(0o755)
    with pytest.raises(ErreurSource) as e:
        lec.synchroniser()
    assert "secret123" not in str(e.value) and "***" in str(e.value)


def test_un_lien_symbolique_n_est_ni_liste_ni_lu(depot, tmp_path):
    dehors = tmp_path / "dehors.txt"
    dehors.write_text("SECRET_HORS_DEPOT")
    (depot / "lien").symlink_to(dehors)
    git(depot, "add", "lien")
    git(depot, "commit", "-qm", "lien")
    lec = LecteurGit(str(depot), tmp_path / "clone")
    assert "lien" not in {e.chemin for e in lec.lister()}
    assert b"SECRET_HORS_DEPOT" not in lec.lire("lien")  # le blob : le texte du lien, rien de plus
    for c in ("../dehors.txt", "/etc/hosts", "a/../../x"):
        with pytest.raises(ErreurSource):
            lec.lire(c)


def test_lire_ignore_l_arbre_de_travail(depot, tmp_path):
    lec = LecteurGit(str(depot), tmp_path / "clone")
    lec.synchroniser()
    (tmp_path / "clone" / "a.py").write_text("modifié à la main")
    assert lec.lire("a.py") != b"modifi\xc3\xa9 \xc3\xa0 la main"
