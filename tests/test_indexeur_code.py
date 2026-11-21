from pathlib import Path

from rag.config import PUBLIC, SourceConf, charger_config
from rag.exclusions import motif_exclusion
from rag.indexeur import indexer_source, version_courante
from rag.recherche import Filtres, chercher
from rag.sources import LecteurGit, lecteur_pour
from tests.conftest import commit

CONFIG = charger_config(Path("config/rag.yaml"))
CONF = CONFIG.indexation
SRC_CODE = SourceConf(visibilite="public-maison", nom="depot-test", type="code", emplacement="x")


def chemins(bd):
    return {r["chemin"] for r in bd.execute("SELECT chemin FROM documents")}


def test_indexe_le_code_par_symbole(bd, depot, tmp_path, faux_modeles):
    b = indexer_source(bd, SRC_CODE, LecteurGit(str(depot), tmp_path / "c"), CONF, faux_modeles)
    assert b.exclus == 3  # .env.example, uv.lock, logo.png
    assert chemins(bd) == {"a.py", "docs/guide.md", "gros.json"}
    r = bd.execute("SELECT m.symbole, m.ligne, m.fil, d.type FROM morceaux m JOIN documents d "
                   "ON d.id=m.document_id WHERE d.chemin='a.py' AND m.symbole IS NOT NULL"
                   ).fetchone()
    assert r["fil"].startswith("depot-test > a.py > ") and r["type"] == "python"
    assert r["ligne"] >= 1
    raisons = {r["chemin"]: r["raison"] for r in bd.execute("SELECT chemin, raison FROM exclus")}
    assert raisons == {"logo.png": "binaire"}  # les autres sont exclus sur leur nom, sans lecture


def test_un_secret_dans_le_contenu_n_entre_jamais(bd, depot, tmp_path, faux_modeles):
    commit(depot, "deploy/seal.key", "abc")
    commit(depot, "conf.yaml", "cle: sk-proj-" + "A" * 30 + "\n")
    commit(depot, "k.py", "K = '''-----BEGIN RSA PRIVATE KEY-----\nxx'''\n")
    indexer_source(bd, SRC_CODE, LecteurGit(str(depot), tmp_path / "c"), CONF, faux_modeles)
    assert not chemins(bd) & {"deploy/seal.key", "conf.yaml", "k.py"}
    assert bd.execute("SELECT count(*) n FROM morceaux WHERE texte ~ 'sk-proj-|PRIVATE KEY'"
                      ).fetchone()["n"] == 0


def test_gros_fichier_un_seul_morceau(bd, depot, tmp_path, faux_modeles):
    indexer_source(bd, SRC_CODE, LecteurGit(str(depot), tmp_path / "c"), CONF, faux_modeles)
    ms = bd.execute("SELECT m.texte FROM morceaux m JOIN documents d ON d.id=m.document_id "
                    "WHERE d.chemin='gros.json'").fetchall()
    assert len(ms) == 1 and len(ms[0]["texte"]) == 2000
    v = version_courante(bd, CONF)
    r = chercher(bd, "gros.json", CONFIG.recherche, faux_modeles, v, "m", niveaux=PUBLIC)
    assert "gros.json" in {x.chemin for x in r}


def test_fichier_inchange_jamais_relu(bd, depot, tmp_path, faux_modeles):
    indexer_source(bd, SRC_CODE, LecteurGit(str(depot), tmp_path / "c"), CONF, faux_modeles)
    lec = LecteurGit(str(depot), tmp_path / "c")
    lus = []
    lire = lec.lire
    lec.lire = lambda c: lus.append(c) or lire(c)
    b = indexer_source(bd, SRC_CODE, lec, CONF, faux_modeles)
    assert lus == [] and b.inchanges == 3 and b.exclus == 3


def test_recherche_filtre_par_depot(bd, depot, tmp_path, faux_modeles, index):
    indexer_source(bd, SRC_CODE, LecteurGit(str(depot), tmp_path / "c"), CONF, faux_modeles)
    v = version_courante(bd, CONF)
    r = chercher(bd, "aide lancer indexeur", CONFIG.recherche, faux_modeles, v, "m",
                 niveaux=PUBLIC, filtres=Filtres(sources=["depot-test"]))
    assert r and all(x.source == "depot-test" for x in r)
    a = next(x for x in r if x.chemin == "a.py" and x.symbole)
    assert a.lien == f"depot-test/a.py#L{a.ligne}"


def test_lecteur_pour_le_code(tmp_path):
    class R:
        donnees_dir, github_jeton = tmp_path, "j"
    lec = lecteur_pour(SourceConf(visibilite="public-maison", nom="AI", type="code", emplacement="u", branche="dev"), R)
    assert isinstance(lec, LecteurGit) and lec.dest == tmp_path / "clones" / "AI"
    assert lec.branche == "dev"


def test_la_config_de_prod_exclut_le_banc_du_depot_rag():
    for f in ("config/rag.yaml", "config/dev.yaml"):
        rag = next(s for s in charger_config(Path(f)).sources if s.nom == "RAG")
        assert motif_exclusion("banc/etalon.yaml", b"- id: x", rag.exclure) == "config"


def test_le_coffre_git_n_est_pas_une_source():
    for f in ("config/rag.yaml", "config/dev.yaml"):
        assert all("obsidian-maison" not in s.emplacement for s in charger_config(Path(f)).sources)


def test_blob_geant_exclu_sans_lecture(bd, depot, tmp_path, faux_modeles):
    commit(depot, "enorme.txt", "a" * (10 * CONF.taille_max_octets + 1))
    lec = LecteurGit(str(depot), tmp_path / "c")
    lus = []
    lire = lec.lire
    lec.lire = lambda c: lus.append(c) or lire(c)
    b = indexer_source(bd, SRC_CODE, lec, CONF, faux_modeles)
    assert "enorme.txt" not in lus and "enorme.txt" not in chemins(bd) and b.exclus == 4
