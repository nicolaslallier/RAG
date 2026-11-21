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
        raise NotImplementedError


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
