import base64
import unicodedata
from pathlib import Path

import pytest

from rag.sources import ErreurSource, LecteurGit, lecteur_pour, verifier_depot
from tests.conftest import commit, git


def test_clone_puis_suit_origin_main(depot, tmp_path):
    lec = LecteurGit(str(depot), tmp_path / "clones" / "clone")
    sha1 = lec.synchroniser()
    assert {e.chemin for e in lec.lister()} >= {"a.py", "docs/guide.md"}
    commit(depot, "b.py", "def b(): pass")
    l2 = LecteurGit(str(depot), tmp_path / "clones" / "clone")
    assert l2.synchroniser() != sha1 and "b.py" in {e.chemin for e in l2.lister()}


def test_suit_une_reecriture_d_historique(depot, tmp_path):
    commit(depot, "b.py", "def b(): pass")
    LecteurGit(str(depot), tmp_path / "clones" / "clone").synchroniser()
    (depot / "b.py").write_text("def c(): pass")
    git(depot, "commit", "-qa", "--amend", "-m", "réécrit")
    lec = LecteurGit(str(depot), tmp_path / "clones" / "clone")
    assert lec.synchroniser() == git(depot, "rev-parse", "HEAD")
    assert lec.lire("b.py") == b"def c(): pass"


def test_une_autre_branche_n_est_jamais_lue(depot, tmp_path):
    git(depot, "switch", "-qc", "travail")
    commit(depot, "secret_de_travail.py", "x = 1")
    git(depot, "switch", "-q", "main")
    lec = LecteurGit(str(depot), tmp_path / "clones" / "clone")
    assert "secret_de_travail.py" not in {e.chemin for e in lec.lister()}
    assert git(tmp_path / "clones" / "clone", "branch", "-a").count("travail") == 0


def test_signature_est_le_sha_du_blob(depot, tmp_path):
    lec = LecteurGit(str(depot), tmp_path / "clones" / "clone")
    e = next(e for e in lec.lister() if e.chemin == "a.py")
    assert e.signature == git(depot, "rev-parse", "HEAD:a.py") and e.taille > 0
    assert lec.revision() == git(depot, "rev-parse", "HEAD")


def test_sous_module_ignore(depot, tmp_path):
    sha = git(depot, "rev-parse", "HEAD")
    git(depot, "update-index", "--add", "--cacheinfo", f"160000,{sha},vendor/lib")
    git(depot, "commit", "-qm", "sous-module")
    assert "vendor/lib" not in {e.chemin for e in LecteurGit(str(depot), tmp_path / "clones" / "c").lister()}


def test_le_jeton_ne_fuit_pas(tmp_path):
    with pytest.raises(ErreurSource) as e:
        LecteurGit("https://invalide.invalid/x.git", tmp_path / "clones" / "c",
                   jeton="ghp_secret123").synchroniser()
    assert "ghp_secret123" not in str(e.value)
    assert base64.b64encode(b"x-access-token:ghp_secret123").decode() not in str(e.value)


def test_le_jeton_est_masque_dans_l_erreur(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", str(tmp_path))  # « git » factice qui répète son environnement
    lec = LecteurGit("https://h.example/x.git", tmp_path / "clones" / "c", jeton="ghp_secret123")
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
    lec = LecteurGit(str(depot), tmp_path / "clones" / "clone")
    assert "lien" not in {e.chemin for e in lec.lister()}
    assert b"SECRET_HORS_DEPOT" not in lec.lire("lien")  # le blob : le texte du lien, rien de plus
    for c in ("../dehors.txt", "/etc/hosts", "a/../../x"):
        with pytest.raises(ErreurSource):
            lec.lire(c)


def test_lire_ignore_l_arbre_de_travail(depot, tmp_path):
    lec = LecteurGit(str(depot), tmp_path / "clones" / "clone")
    lec.synchroniser()
    (tmp_path / "clones" / "clone" / "a.py").write_text("modifié à la main")
    assert lec.lire("a.py") != b"modifi\xc3\xa9 \xc3\xa0 la main"


def test_destination_relative(depot, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    lec = LecteurGit(str(depot), Path("donnees/clones/x"))
    lec.synchroniser()
    assert (tmp_path / "donnees/clones/x/a.py").exists()


def test_jeton_pour_l_hote_de_l_url(tmp_path):
    lec = LecteurGit("https://gitea.maison/a/b.git", tmp_path / "clones" / "c", jeton="j",
                     utilisateur="nico")
    assert lec._env["GIT_CONFIG_KEY_0"] == "http.https://gitea.maison/.extraheader"
    assert base64.b64encode(b"nico:j").decode() in lec._env["GIT_CONFIG_VALUE_0"]


def test_url_changee_reclone(depot, tmp_path):
    LecteurGit(str(depot), tmp_path / "clones" / "c").synchroniser()
    depot2 = tmp_path / "autre"
    depot2.mkdir()
    git(depot2, "init", "-q", "-b", "main")
    git(depot2, "config", "user.email", "t@t")
    git(depot2, "config", "user.name", "t")
    commit(depot2, "seul.py", "x = 1")
    lec = LecteurGit(str(depot2), tmp_path / "clones" / "c")
    lec.synchroniser()
    assert [e.chemin for e in lec.lister()] == ["seul.py"]
    assert lec.lire("seul.py") == b"x = 1"


def test_protocole_https_seul(depot, tmp_path):
    with pytest.raises(ErreurSource):
        LecteurGit(str(depot), tmp_path / "clones" / "c", protocoles="https").synchroniser()


def test_verifier_depot_branche_absente(depot):
    with pytest.raises(ErreurSource, match="nexiste"):
        verifier_depot(str(depot), "nexiste", protocoles=None)


def test_verifier_depot_ok(depot):
    verifier_depot(str(depot), "main", protocoles=None)


def test_verifier_depot_refuse_un_chemin_local_par_defaut(depot):
    with pytest.raises(ErreurSource):
        verifier_depot(str(depot), "main")


def test_lecteur_pour_jeton_github_seulement(tmp_path):
    from rag.config import SourceConf

    class R:
        donnees_dir, github_jeton = tmp_path, "jg"
    def conf(u):
        return SourceConf(visibilite="public-maison", nom="x", type="code", emplacement=u)
    assert "GIT_CONFIG_KEY_0" not in lecteur_pour(conf("https://gitea.maison/a/b.git"), R)._env
    gh = lecteur_pour(conf("https://github.com/a/b.git"), R)
    assert gh._env["GIT_CONFIG_KEY_0"] == "http.https://github.com/.extraheader"
    propre = lecteur_pour(conf("https://gitea.maison/a/b.git"), R, "jp", "nico")
    assert base64.b64encode(b"nico:jp").decode() in propre._env["GIT_CONFIG_VALUE_0"]
    assert propre._env["GIT_ALLOW_PROTOCOL"] == "https"


def test_un_nom_non_normalise_est_ignore_seul(depot, tmp_path):
    blob = git(depot, "rev-parse", "HEAD:a.py")
    nfd = unicodedata.normalize("NFD", "pâté.py")
    git(depot, "config", "core.precomposeunicode", "false")  # macOS recompose sinon
    git(depot, "update-index", "--add", "--cacheinfo", f"100644,{blob},{nfd}")
    git(depot, "commit", "-qm", "noms bizarres")
    lec = LecteurGit(str(depot), tmp_path / "clones" / "c")
    lec.synchroniser()
    git(tmp_path / "clones" / "c", "config", "core.precomposeunicode", "false")
    brut = git(tmp_path / "clones" / "c", "ls-tree", "-r", "--name-only", "HEAD")
    assert "a.py" in {e.chemin for e in lec.lister()}
    assert len(lec.lister()) == len(brut.splitlines()) - 1


def test_clone_abime_est_reclone(depot, tmp_path):
    LecteurGit(str(depot), tmp_path / "clones" / "c").synchroniser()
    (tmp_path / "clones" / "c" / ".git" / "HEAD").write_text("n'importe quoi\n")
    commit(depot, "b.py", "x = 1")
    lec = LecteurGit(str(depot), tmp_path / "clones" / "c")
    assert lec.synchroniser() == git(depot, "rev-parse", "HEAD")


def test_jamais_de_suppression_hors_du_dossier_des_clones(tmp_path):
    (tmp_path / "precieux").mkdir()
    (tmp_path / "precieux" / "f").write_text("x")
    with pytest.raises(ErreurSource):
        LecteurGit("x", tmp_path / "precieux").synchroniser()
    assert (tmp_path / "precieux" / "f").exists()


def test_verifier_depot_ne_confond_pas_suffixe_ni_motif(depot):
    git(depot, "branch", "feature/main")
    git(depot, "branch", "-m", "main", "autre")
    for faux in ("main", "ma*", "*", "eature/main"):
        with pytest.raises(ErreurSource, match="introuvable"):
            verifier_depot(str(depot), faux, protocoles=None)
    verifier_depot(str(depot), "feature/main", protocoles=None)
