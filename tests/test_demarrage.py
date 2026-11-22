import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest
import yaml
from fastapi.testclient import TestClient

from rag.api import create_app
from rag.config import ErreurConfig, Reglages
from rag.db import MIGRATIONS, connexion, migrer
from rag.demarrage import attendre_base, verifier_reglages
from tests.conftest import URL

J32, J32B, J32C = "a" * 32, "b" * 32, "c" * 32
OK = {"database_url": URL, "litellm_url": "http://x", "litellm_cle": "k", "jeton": J32,
          "jeton_reindexer": J32B, "s3_secret_key": "s", "s3_endpoint": "https://s3.x", "github_jeton": "g"}


def reglages(**k):
    return Reglages(**{**OK, **k})


@pytest.fixture
def url_vierge():
    nom = "rag_test_migr"
    admin = URL.rsplit("/", 1)[0] + "/postgres"
    with psycopg.connect(admin, autocommit=True) as c:
        c.execute(f'DROP DATABASE IF EXISTS "{nom}" WITH (FORCE)')
        c.execute(f'CREATE DATABASE "{nom}"')
    yield URL.rsplit("/", 1)[0] + "/" + nom
    with psycopg.connect(admin, autocommit=True) as c:
        c.execute(f'DROP DATABASE IF EXISTS "{nom}" WITH (FORCE)')


def test_migrations_concurrentes(url_vierge):
    def une(_):
        with connexion(url_vierge) as c:
            return migrer(c)
    r = list(ThreadPoolExecutor(2).map(une, range(2)))
    assert sorted(map(len, r)) == [0, len(list(MIGRATIONS.glob("*.sql")))]


@pytest.mark.parametrize("env,nom", [
    ({"jeton": "xyz"}, "RAG_JETON"),
    ({"jeton_reindexer": J32}, "RAG_JETON_REINDEXER"),     # égal à RAG_JETON
    ({"jeton_reindexer": ""}, "RAG_JETON_REINDEXER"),
    ({"litellm_cle": ""}, "RAG_LITELLM_CLE"),
    ({"jeton_prive": J32}, "RAG_JETON_PRIVE"),             # égal à RAG_JETON
    ({"jeton_prive": "xyz"}, "RAG_JETON_PRIVE"),
])
def test_reglages_refuses(env, nom):
    with pytest.raises(ErreurConfig) as e:
        verifier_reglages(reglages(**env), "api")
    assert nom in str(e.value) and J32 not in str(e.value) and "xyz" not in str(e.value)


def test_reglages_acceptes():
    verifier_reglages(reglages(jeton_prive=J32C), "api")
    verifier_reglages(reglages(jeton_prive=""), "api")


@pytest.mark.parametrize("source,vide,nom", [
    ({"emplacement": "s3", "type": "obsidian"}, "s3_secret_key", "RAG_S3_SECRET_KEY"),
    ({"emplacement": "https://github.com/x/y.git", "type": "code"}, "github_jeton",
     "RAG_GITHUB_JETON"),
])
def test_travailleur_exige_ses_secrets(tmp_path, source, vide, nom):
    d = yaml.safe_load(Path("config/rag.yaml").read_text())
    d["sources"] = [{"nom": "x", "visibilite": "public-maison", **source}]
    f = tmp_path / "rag.yaml"
    f.write_text(yaml.safe_dump(d))
    verifier_reglages(reglages(config=f, **{vide: ""}), "api")          # l'API n'en a pas besoin
    with pytest.raises(ErreurConfig, match=nom):
        verifier_reglages(reglages(config=f, **{vide: ""}), "travailleur")
    verifier_reglages(reglages(config=f), "travailleur")


def test_attendre_base_reessaie_sans_sortir():
    essais, pauses = iter([OSError(), OSError(), "conn"]), []

    def ouvrir():
        x = next(essais)
        if isinstance(x, Exception):
            raise x
        return x
    assert attendre_base(ouvrir, dormir=pauses.append) == "conn" and pauses == [30, 30]


def test_un_401_ne_journalise_pas_le_jeton(caplog, faux_modeles):
    caplog.set_level(logging.DEBUG)
    c = TestClient(create_app(reglages(), faux_modeles))
    r = c.post("/chercher", json={"question": "x"},
               headers={"Authorization": "Bearer secret-essai-123"})
    assert r.status_code == 401 and "secret-essai-123" not in caplog.text + r.text


def test_migrer_attend_la_base(monkeypatch):
    import rag.cli
    import rag.demarrage
    vraie, essais = rag.cli.connexion, []

    def connexion(url):
        essais.append(url)
        if len(essais) == 1:
            raise psycopg.OperationalError("injoignable")
        return vraie(URL)
    monkeypatch.setattr(rag.cli, "connexion", connexion)
    monkeypatch.setattr(rag.demarrage.time, "sleep", lambda s: None)
    monkeypatch.setenv("RAG_DATABASE_URL", URL)
    monkeypatch.setenv("RAG_LITELLM_URL", "http://x")
    assert rag.cli.main(["migrer"]) == 0 and len(essais) == 2


def test_erreur_de_migration_non_masquee(url_vierge, monkeypatch):
    import rag.db
    monkeypatch.setattr(rag.db, "_migrer", lambda c: c.execute("SELECT 1/0"))
    with connexion(url_vierge) as c, pytest.raises(psycopg.errors.DivisionByZero):
        migrer(c)


def test_s3_en_clair_refuse(tmp_path):
    d = yaml.safe_load(Path("config/rag.yaml").read_text())
    d["sources"] = [{"nom": "x", "visibilite": "public-maison", "type": "obsidian",
                     "emplacement": "s3"}]
    f = tmp_path / "rag.yaml"
    f.write_text(yaml.safe_dump(d))
    with pytest.raises(ErreurConfig, match="RAG_S3_ENDPOINT"):
        verifier_reglages(reglages(config=f, s3_endpoint="http://192.168.2.10:8333"), "travailleur")
    verifier_reglages(reglages(config=f, s3_endpoint="https://s3.x"), "travailleur")


class _ErreurPg(psycopg.OperationalError):
    sqlstate = "28P01"
    diag = SimpleNamespace(message_primary='password authentication failed mdp-Secret9 rag:mdp-Secret9@h')

    def __str__(self):
        return "connection to postgresql://rag:mdp-Secret9@h/rag failed: mdp-Secret9"


def test_attendre_base_ne_journalise_jamais_le_mot_de_passe(capsys):
    essais = iter([_ErreurPg(), psycopg.OperationalError("brut mdp-Secret9"), "conn"])

    def ouvrir():
        x = next(essais)
        if isinstance(x, Exception):
            raise x
        return x
    attendre_base(ouvrir, dormir=lambda s: None, url="postgresql://rag:mdp-Secret9@h/rag")
    err = capsys.readouterr().err
    assert "mdp-Secret9" not in err and "28P01 password authentication failed" in err


def test_url_invalide_echoue_sans_reessai(capsys):
    def ouvrir():
        raise psycopg.ProgrammingError("invalid dsn: mdp-Secret9")
    with pytest.raises(ErreurConfig, match="RAG_DATABASE_URL invalide") as e:
        attendre_base(ouvrir, dormir=lambda s: pytest.fail("réessai"))
    assert "mdp-Secret9" not in str(e.value) + capsys.readouterr().err


def test_pas_de_documentation_openapi(faux_modeles):
    c = TestClient(create_app(reglages(), faux_modeles))
    assert all(c.get(u).status_code == 404 for u in ("/docs", "/redoc", "/openapi.json"))
