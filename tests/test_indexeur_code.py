from pathlib import Path

import pytest

from rag.config import PUBLIC, SourceConf, charger_config
from rag.exclusions import motif_exclusion
from rag.indexeur import indexer_source, version_courante
from rag.recherche import Filtres, chercher
from rag.sources import LecteurGit, lecteur_pour
from tests.conftest import commit, git, inscrire_source

CONFIG = charger_config(Path("config/rag.yaml"))
CONF = CONFIG.indexation
SRC_CODE = SourceConf(visibilite="public-maison", nom="depot-test", type="code", emplacement="x")


@pytest.fixture(autouse=True)
def source_en_base(request, bd):
    """Les sources code sont gérées en base : `indexer_source` ne les crée plus."""
    if request.node.name != "test_source_supprimee_n_est_pas_recreee":
        inscrire_source(bd, "depot-test")


def chemins(bd):
    return {r["chemin"] for r in bd.execute("SELECT chemin FROM documents")}


def test_indexe_le_code_par_symbole(bd, depot, tmp_path, faux_modeles):
    b = indexer_source(bd, SRC_CODE, LecteurGit(str(depot), tmp_path / "clones" / "c"), CONF, faux_modeles)
    assert b.exclus == 3  # .env.example, uv.lock, logo.png
    assert chemins(bd) == {"a.py", "docs/guide.md", "gros.json"}
    r = bd.execute("SELECT m.symbole, m.ligne, m.fil, d.type FROM morceaux m JOIN documents d "
                   "ON d.id=m.document_id WHERE d.chemin='a.py' AND m.symbole IS NOT NULL"
                   ).fetchone()
    assert r["fil"].startswith("depot-test > a.py > ") and r["type"] == "python"
    assert r["ligne"] >= 1
    raisons = {r["chemin"]: (r["raison"], r["lu"]) for r in bd.execute("SELECT chemin, raison, lu FROM exclus")}
    assert raisons == {"logo.png": ("binaire", True), ".env.example": ("secret", False),
                       "uv.lock": ("genere", False)}  # gros.json (400 Ko) reste sous 10 x taille_max


def test_un_secret_dans_le_contenu_n_entre_jamais(bd, depot, tmp_path, faux_modeles):
    commit(depot, "deploy/seal.key", "abc")
    commit(depot, "conf.yaml", "cle: sk-proj-" + "A" * 30 + "\n")
    commit(depot, "k.py", "K = '''-----BEGIN RSA PRIVATE KEY-----\nxx'''\n")
    indexer_source(bd, SRC_CODE, LecteurGit(str(depot), tmp_path / "clones" / "c"), CONF, faux_modeles)
    assert not chemins(bd) & {"deploy/seal.key", "conf.yaml", "k.py"}
    assert bd.execute("SELECT count(*) n FROM morceaux WHERE texte ~ 'sk-proj-|PRIVATE KEY'"
                      ).fetchone()["n"] == 0


def test_gros_fichier_un_seul_morceau(bd, depot, tmp_path, faux_modeles):
    indexer_source(bd, SRC_CODE, LecteurGit(str(depot), tmp_path / "clones" / "c"), CONF, faux_modeles)
    ms = bd.execute("SELECT m.texte FROM morceaux m JOIN documents d ON d.id=m.document_id "
                    "WHERE d.chemin='gros.json'").fetchall()
    assert len(ms) == 1 and len(ms[0]["texte"]) == 2000
    v = version_courante(bd, CONF)
    r = chercher(bd, "gros.json", CONFIG.recherche, faux_modeles, v, "m", niveaux=PUBLIC)
    assert "gros.json" in {x.chemin for x in r}


def test_fichier_inchange_jamais_relu(bd, depot, tmp_path, faux_modeles):
    indexer_source(bd, SRC_CODE, LecteurGit(str(depot), tmp_path / "clones" / "c"), CONF, faux_modeles)
    lec = LecteurGit(str(depot), tmp_path / "clones" / "c")
    lus = []
    lire = lec.lire
    lec.lire = lambda c: lus.append(c) or lire(c)
    b = indexer_source(bd, SRC_CODE, lec, CONF, faux_modeles)
    assert lus == [] and b.inchanges == 3 and b.exclus == 3


def test_recherche_filtre_par_depot(bd, depot, tmp_path, faux_modeles, index):
    indexer_source(bd, SRC_CODE, LecteurGit(str(depot), tmp_path / "clones" / "c"), CONF, faux_modeles)
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


def test_source_supprimee_n_est_pas_recreee(bd, depot, tmp_path, faux_modeles):
    from rag.indexeur import SourceSupprimee
    with pytest.raises(SourceSupprimee):
        indexer_source(bd, SRC_CODE, LecteurGit(str(depot), tmp_path / "clones" / "c"), CONF,
                       faux_modeles)
    assert bd.execute("SELECT count(*) n FROM sources").fetchone()["n"] == 0


def test_la_config_de_prod_exclut_le_banc_du_depot_rag():
    from rag.sources_gerees import lire_export
    rag = next(e for e in lire_export(Path("config/sources-maison.yaml").read_text())
               if e["nom"] == "RAG")
    assert motif_exclusion("banc/etalon.yaml", b"- id: x", rag["exclure"]) == "config"


def test_le_coffre_git_n_est_pas_une_source():
    for f in ("config/rag.yaml", "config/dev.yaml"):
        assert all("obsidian-maison" not in s.emplacement for s in charger_config(Path(f)).sources)


def test_blob_geant_exclu_sans_lecture(bd, depot, tmp_path, faux_modeles):
    commit(depot, "enorme.txt", "a" * (10 * CONF.taille_max_octets + 1))
    lec = LecteurGit(str(depot), tmp_path / "clones" / "c")
    lus = []
    lire = lec.lire
    lec.lire = lambda c: lus.append(c) or lire(c)
    b = indexer_source(bd, SRC_CODE, lec, CONF, faux_modeles)
    assert "enorme.txt" not in lus and "enorme.txt" not in chemins(bd) and b.exclus == 4


def test_blob_geant_est_dans_le_registre(bd, depot, tmp_path, faux_modeles):
    commit(depot, "enorme.txt", "a" * (10 * CONF.taille_max_octets + 1))
    indexer_source(bd, SRC_CODE, LecteurGit(str(depot), tmp_path / "clones" / "c"), CONF, faux_modeles)
    r = bd.execute("SELECT raison, lu FROM exclus WHERE chemin = 'enorme.txt'").fetchone()
    assert (r["raison"], r["lu"]) == ("trop_gros", False)


def test_exclu_sur_le_nom_puis_plus_exclu_est_reindexe(bd, depot, tmp_path, faux_modeles):
    lec = LecteurGit(str(depot), tmp_path / "clones" / "c")
    src = SourceConf(visibilite="public-maison", nom="depot-test", type="code", emplacement="x",
                     exclure=["docs/**"])
    indexer_source(bd, src, lec, CONF, faux_modeles)
    r = bd.execute("SELECT raison, lu FROM exclus WHERE chemin = 'docs/guide.md'").fetchone()
    assert (r["raison"], r["lu"]) == ("config", False)
    assert "docs/guide.md" not in chemins(bd)
    indexer_source(bd, SRC_CODE, lec, CONF, faux_modeles)
    assert "docs/guide.md" in chemins(bd)
    assert bd.execute("SELECT count(*) n FROM exclus WHERE chemin = 'docs/guide.md'").fetchone()["n"] == 0


def test_exclus_disparu_du_listage_est_oublie(bd, depot, tmp_path, faux_modeles):
    lec = LecteurGit(str(depot), tmp_path / "clones" / "c")
    indexer_source(bd, SRC_CODE, lec, CONF, faux_modeles)
    git(depot, "rm", "-q", "uv.lock")
    git(depot, "commit", "-qm", "rm lock")
    indexer_source(bd, SRC_CODE, LecteurGit(str(depot), tmp_path / "clones" / "c"), CONF,
                   faux_modeles)
    assert bd.execute("SELECT count(*) n FROM exclus WHERE chemin = 'uv.lock'").fetchone()["n"] == 0


def test_source_changee_pendant_la_passe_n_ecrit_rien(bd, depot, tmp_path, faux_modeles):
    from rag.indexeur import SourceSupprimee
    inscrire_source(bd, "depot-test", emplacement="https://x/nouveau.git")
    ancienne = SRC_CODE.model_copy(update={"emplacement": "https://x/ancien.git"})
    with pytest.raises(SourceSupprimee, match="changée ou supprimée"):
        indexer_source(bd, ancienne, LecteurGit(str(depot), tmp_path / "clones" / "c"), CONF,
                       faux_modeles)
    assert bd.execute("SELECT count(*) n FROM documents").fetchone()["n"] == 0
    autre_branche = SRC_CODE.model_copy(update={"emplacement": "https://x/nouveau.git",
                                                "branche": "dev"})
    with pytest.raises(SourceSupprimee):
        indexer_source(bd, autre_branche, LecteurGit(str(depot), tmp_path / "clones" / "c"), CONF,
                       faux_modeles)


def _changer_emplacement():
    from rag.db import connexion
    from tests.conftest import URL
    with connexion(URL) as autre:
        autre.execute("UPDATE sources SET emplacement = 'https://x/nouveau.git'"
                      " WHERE nom = 'depot-test'")
        autre.commit()


def test_source_changee_entre_listage_et_ecriture(bd, depot, tmp_path, faux_modeles):
    from rag.indexeur import SourceSupprimee

    class ModelesQuiChangentLaSource(type(faux_modeles)):
        def embed_documents(self, textes, modele):
            if not self.appels:
                _changer_emplacement()
            return super().embed_documents(textes, modele)

    with pytest.raises(SourceSupprimee):
        indexer_source(bd, SRC_CODE, LecteurGit(str(depot), tmp_path / "clones" / "c"), CONF,
                       ModelesQuiChangentLaSource())
    assert bd.execute("SELECT count(*) AS n FROM documents").fetchone()["n"] == 0
    assert bd.execute("SELECT revision FROM sources WHERE nom = 'depot-test'"
                      ).fetchone()["revision"] is None


def test_source_changee_pendant_le_listage(bd, depot, tmp_path, faux_modeles):
    from rag.indexeur import SourceSupprimee
    indexer_source(bd, SRC_CODE, LecteurGit(str(depot), tmp_path / "clones" / "c"), CONF,
                   faux_modeles)
    avant = bd.execute("SELECT revision, indexe_le FROM sources WHERE nom = 'depot-test'"
                       ).fetchone()
    bd.commit()
    commit(depot, "b.py", "def b():\n    return 2\n")
    lec = LecteurGit(str(depot), tmp_path / "clones" / "c")
    lister = lec.lister

    def lister_puis_changer():
        r = lister()
        _changer_emplacement()
        return r
    lec.lister = lister_puis_changer
    with pytest.raises(SourceSupprimee):
        indexer_source(bd, SRC_CODE, lec, CONF, faux_modeles)
    apres = bd.execute("SELECT revision, indexe_le FROM sources WHERE nom = 'depot-test'"
                       ).fetchone()
    assert dict(apres) == dict(avant)
    assert "b.py" not in chemins(bd)


def test_nul_apres_8_ko_indexe_sans_nul(bd, depot, tmp_path, faux_modeles):
    commit(depot, "gros_nul.py", "x = 1\n" * 2000 + "\x00" + "y = 2\n")
    b = indexer_source(bd, SRC_CODE, LecteurGit(str(depot), tmp_path / "clones" / "c"), CONF,
                       faux_modeles)
    assert b.echecs == 0 and "gros_nul.py" in chemins(bd)
    textes = [r["texte"] for r in bd.execute(
        "SELECT m.texte FROM morceaux m JOIN documents d ON d.id = m.document_id"
        " WHERE d.chemin = 'gros_nul.py'")]
    assert textes and not any("\x00" in t for t in textes)


# Contrôle post-déploiement du README (motifs de contenu ajoutés sans changer VERSION_ANALYSEUR)
_JETONS_SQL = "(glpat-[A-Za-z0-9_-]{20,}|xox[abposr]-[A-Za-z0-9]{8,}-)"
_SUSPECTS = f"""SELECT d.id FROM documents d LEFT JOIN morceaux m ON m.document_id = d.id
                 WHERE m.texte ~ '{_JETONS_SQL}' OR d.frontmatter::text ~ '{_JETONS_SQL}'"""


def test_controle_sql_post_deploiement_relit_et_exclut(bd, depot, tmp_path, faux_modeles,
                                                         monkeypatch):
    import re

    import rag.exclusions
    jeton = "glpat-" + "a" * 20
    faux = "xoxb-" + "1" * 8 + "-court"  # vu par la requête, pas par l'analyseur
    commit(depot, "ci.md", f"# CI\n\njeton: {jeton}\n")
    commit(depot, "doc.md", f"# Doc\n\nexemple : {faux}\n")
    ancien = re.compile(rb"-----BEGIN [A-Z ]*PRIVATE KEY")  # analyseur d'avant le lot
    with monkeypatch.context() as m:
        m.setattr(rag.exclusions, "_CONTENU_SECRET", ancien)
        indexer_source(bd, SRC_CODE, LecteurGit(str(depot), tmp_path / "clones" / "c"), CONF,
                       faux_modeles)
    assert {"ci.md", "doc.md"} <= chemins(bd)
    lec = LecteurGit(str(depot), tmp_path / "clones" / "c")
    assert indexer_source(bd, SRC_CODE, lec, CONF, faux_modeles).exclus == 3  # signature égale
    assert "ci.md" in chemins(bd)
    n = bd.execute(f"SELECT count(*) AS n FROM ({_SUSPECTS}) s").fetchone()["n"]
    assert n == 2
    bd.execute(f"UPDATE documents SET signature = '' WHERE id IN ({_SUSPECTS})")
    bd.commit()
    appels = faux_modeles.appels
    b = indexer_source(bd, SRC_CODE, lec, CONF, faux_modeles)
    assert "ci.md" not in chemins(bd) and "doc.md" in chemins(bd)
    assert b.exclus == 4 and faux_modeles.appels == appels  # rien de réembarqué
    assert bd.execute("SELECT raison FROM exclus WHERE chemin = 'ci.md'").fetchone()["raison"] \
        == "secret"
