import hashlib
import math
import os
import re
import unicodedata

import psycopg
import pytest

from rag.db import connexion, migrer

URL = os.environ.get("RAG_TEST_DATABASE_URL", "postgresql://rag:rag@localhost:5434/rag_test")


def _mots(texte: str) -> list[str]:
    t = unicodedata.normalize("NFKD", texte.lower())
    t = "".join(c for c in t if not unicodedata.combining(c))
    return re.findall(r"\w+", t)


class FauxModeles:
    """Même interface que rag.modeles.Modeles ; sac de mots haché, normalisé."""

    def __init__(self):
        self.appels = 0

    def _vec(self, texte: str) -> list[float]:
        v = [0.0] * 1024
        for m in _mots(texte):
            v[int(hashlib.md5(m.encode()).hexdigest(), 16) % 1024] += 1.0
        n = math.sqrt(sum(x * x for x in v)) or 1.0
        if not any(v):
            v[0] = 1.0
        return [x / n for x in v]

    def embed_documents(self, textes, modele):
        self.appels += 1
        return [self._vec(t) for t in textes]

    def embed_requete(self, question, modele, instruction):
        self.appels += 1
        return self._vec(question)

    def generer(self, modele, messages, temperature, json_mode=False):
        from rag.modeles import ErreurModele
        raise ErreurModele("pas de génération dans les faux modèles")


@pytest.fixture
def faux_modeles():
    return FauxModeles()


@pytest.fixture(scope="session")
def _base():
    nom = URL.rsplit("/", 1)[1]
    admin = URL.rsplit("/", 1)[0] + "/postgres"
    with psycopg.connect(admin, autocommit=True) as c:
        if not c.execute("SELECT 1 FROM pg_database WHERE datname=%s", (nom,)).fetchone():
            c.execute(f'CREATE DATABASE "{nom}"')
    with connexion(URL) as c:
        migrer(c)
        c.commit()
    return URL


@pytest.fixture
def bd(_base):
    conn = connexion(_base)
    conn.execute("TRUNCATE sources, index_versions CASCADE")
    conn.commit()
    yield conn
    conn.rollback()
    conn.close()


class _Corps:
    def __init__(self, b):
        self.b = b

    def read(self):
        return self.b


class FauxS3:
    """Faux client boto3 : pages de list_objects_v2 jouées dans l'ordre, objets par clé."""

    def __init__(self, pages: list[dict], objets: dict[str, bytes] | None = None):
        self.pages, self.objets = pages, objets or {}
        self.appels_list = 0
        self.args = []

    def list_objects_v2(self, **kw):
        self.args.append(kw)
        p = self.pages[self.appels_list]
        self.appels_list += 1
        return p

    def get_object(self, Bucket, Key):
        return {"Body": _Corps(self.objets[Key])}


NOTES_TEST = {
    "02 - Domaines/Cuisine/Recettes/Poulet cidre et Boursin.md":
        "---\ntype: recette\ntags: [recette, poulet]\nsource: claude\ndate: 2026-11-08\n---\n"
        "# Poulet cidre et Boursin\n\nPoitrines de poulet, carottes, cidre et Boursin au four.\n",
    "02 - Domaines/Cuisine/Recettes/Pâté chinois.md":
        "---\ntype: recette\ntags: [recette, boeuf]\ndate: 2026-10-01\n---\n"
        "# Pâté chinois\n\nBoeuf haché, maïs en crème, patates pilées.\n",
    "03 - Ressources/Grafana.md":
        "---\ntype: ressource\ndate: 2026-09-01\n---\n# Grafana\n\nTableaux de bord du homelab.\n",
    "03 - Ressources/Kubernetes.md":
        "---\ntype: note-techno\ndate: 2025-03-01\n---\n# Kubernetes\n\nOrchestration de conteneurs.\n",
    "05 - Journal/2026-11-20.md":
        "---\ntype: journal\ndate: 2026-11-20\n---\n# Journée\n\nNote du jour : souper et ménage.\n",
    "Accueil.md": "# Accueil\n\nPage d'accueil du coffre, une note de départ.\n",
}


class Index:
    def __init__(self, version, coffre):
        self.version, self.coffre = version, coffre


def _indexer(bd, tmp_path, faux_modeles, strategie="titres"):
    from pathlib import Path

    from rag.config import SourceConf, charger_config
    from rag.indexeur import indexer_source, version_courante
    from rag.sources import LecteurDossier

    coffre = tmp_path / "coffre-index"
    for chemin, texte in NOTES_TEST.items():
        (coffre / chemin).parent.mkdir(parents=True, exist_ok=True)
        (coffre / chemin).write_text(texte)
    conf = charger_config(Path("config/rag.yaml")).indexation.model_copy(
        update={"strategie": strategie})
    src = SourceConf(visibilite="public-maison", nom="coffre-maison", type="obsidian", emplacement=str(coffre))
    indexer_source(bd, src, LecteurDossier(coffre), conf, faux_modeles)
    return Index(version_courante(bd, conf), coffre)


@pytest.fixture
def index(bd, tmp_path, faux_modeles):
    return _indexer(bd, tmp_path, faux_modeles)


@pytest.fixture
def index_pe(bd, tmp_path, faux_modeles):
    return _indexer(bd, tmp_path, faux_modeles, "parent_enfant")


def git(depot, *args):
    import subprocess

    return subprocess.run(["git", "-C", str(depot), *args], check=True, capture_output=True,
                          text=True).stdout.strip()


def commit(depot, chemin, texte, message="maj"):
    (depot / chemin).parent.mkdir(parents=True, exist_ok=True)
    (depot / chemin).write_text(texte)
    git(depot, "add", chemin)
    git(depot, "commit", "-qm", message)


@pytest.fixture
def depot(tmp_path):
    """Dépôt git local : main avec du code, un secret, un verrou, un binaire, un gros JSON."""
    d = tmp_path / "depot"
    d.mkdir()
    git(d, "init", "-q", "-b", "main")
    git(d, "config", "user.email", "t@t")
    git(d, "config", "user.name", "t")
    (d / "a.py").write_text("import os\n\ndef aide():\n    return 1\n\n\nclass Indexeur:\n"
                            "    def lancer(self):\n        return aide()\n")
    (d / ".env.example").write_text("CLE=xyz\n")
    (d / "uv.lock").write_text("version = 1\n")
    (d / "logo.png").write_bytes(b"\x89PNG\r\n\x1a\n\x00\x00binaire")
    (d / "docs").mkdir()
    (d / "docs/guide.md").write_text("# Guide\n\nComment déployer le service.\n")
    (d / "gros.json").write_text('{"x": "' + "a" * 400_000 + '"}')
    git(d, "add", "-A")
    git(d, "commit", "-qm", "init")
    return d


@pytest.fixture
def index_code(bd, depot, tmp_path, faux_modeles):
    """Le dépôt de test indexé comme source de code « depot-test »."""
    from pathlib import Path

    from rag.config import SourceConf, charger_config
    from rag.indexeur import indexer_source
    from rag.sources import LecteurGit

    src = SourceConf(visibilite="public-maison", nom="depot-test", type="code", emplacement="x")
    lec = LecteurGit(str(depot), tmp_path / "clones" / "c")
    indexer_source(bd, src, lec, charger_config(Path("config/rag.yaml")).indexation, faux_modeles)
    return lec


def indexer_coffre(bd, tmp_path, faux_modeles, notes: dict, nom="coffre-maison", forcer=False):
    from pathlib import Path

    from rag.config import SourceConf, charger_config
    from rag.indexeur import indexer_source
    from rag.sources import LecteurDossier

    coffre = tmp_path / nom
    for chemin, texte in notes.items():
        (coffre / chemin).parent.mkdir(parents=True, exist_ok=True)
        (coffre / chemin).write_text(texte)
    src = SourceConf(visibilite="public-maison", nom=nom, type="obsidian", emplacement="x")
    indexer_source(bd, src, LecteurDossier(coffre),
                   charger_config(Path("config/rag.yaml")).indexation, faux_modeles, forcer=forcer)
    return coffre


RECETTES = {
    "Recettes/Poulet cidre et Boursin.md":
        "---\ntype: recette\ntags: [recette, poulet]\nsource: claude\ndate: 2026-11-08\n---\n"
        "# Poulet cidre et Boursin\n\nPoitrines de poulet, cidre et Boursin au four.\n",
    "Recettes/Poulet au beurre.md":
        "---\ntype: recette\ntags: [poulet, indien]\nsource: https://ex.com/pab\n---\n"
        "# Poulet au beurre\n\nPoulet, beurre, tomates, garam masala.\n",
    "Recettes/Pâté chinois.md":
        "---\ntype: recette\ntags: [boeuf]\n---\n# Pâté chinois\n\nBoeuf haché, maïs, patates.\n",
    "Idées de soupers.md":
        "# Idées de soupers\n\nUn poulet rôti le dimanche, des recettes de poulet faciles.\n",
}


def indexer_depot(bd, tmp_path, faux_modeles, nom: str, fichiers: dict, visibilite="public-maison"):
    from pathlib import Path

    from rag.config import SourceConf, charger_config
    from rag.indexeur import indexer_source
    from rag.sources import LecteurGit

    d = tmp_path / f"depot-{nom}"
    d.mkdir()
    git(d, "init", "-q", "-b", "main")
    git(d, "config", "user.email", "t@t")
    git(d, "config", "user.name", "t")
    for chemin, texte in fichiers.items():
        commit(d, chemin, texte)
    src = SourceConf(visibilite=visibilite, nom=nom, type="code", emplacement="x")
    indexer_source(bd, src, LecteurGit(str(d), tmp_path / "clones" / nom),
                   charger_config(Path("config/rag.yaml")).indexation, faux_modeles)


@pytest.fixture
def index_recettes(bd, tmp_path, faux_modeles):
    from pathlib import Path

    from rag.config import charger_config
    from rag.indexeur import version_courante
    indexer_coffre(bd, tmp_path, faux_modeles, RECETTES)
    return version_courante(bd, charger_config(Path("config/rag.yaml")).indexation)


GRAFANA = {f"monitoring/grafana/dashboards/{n}.json":
           f'{{"title": "Grafana dashboard {n}", "panels": [{{"title": "grafana {n}"}}]}}\n'
           for n in ("noeuds", "postgres", "litellm")}


@pytest.fixture
def index_mixte(bd, tmp_path, faux_modeles, index_recettes):
    indexer_depot(bd, tmp_path, faux_modeles, "Infra", GRAFANA)
    return index_recettes
