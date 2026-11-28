from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from rag.api import create_app
from rag.config import Reglages
from tests.conftest import URL


@pytest.fixture
def cfg(tmp_path, index):
    d = yaml.safe_load(Path("config/rag.yaml").read_text())
    d["sources"] = [{"nom": "coffre-maison", "type": "obsidian", "visibilite": "public-maison", "emplacement": str(index.coffre)}]
    f = tmp_path / "rag.yaml"
    f.write_text(yaml.safe_dump(d))
    return f


def faire(cfg, faux_modeles, **k):
    r = Reglages(database_url=URL, litellm_url="http://x", jeton="pub", config=cfg, **k)
    return TestClient(create_app(r, faux_modeles))


@pytest.fixture
def client(cfg, faux_modeles):
    return faire(cfg, faux_modeles, jeton_prive="priv")


auth = {"Authorization": "Bearer pub"}
auth_prive = {"Authorization": "Bearer priv"}


def test_sante_sans_jeton_ne_dit_rien(client):
    assert client.get("/sante").json() == {"ok": True}
    assert client.get("/sante", headers={"Authorization": "Bearer mauvais"}).json() == {"ok": True}


def test_sante_avec_jeton_compte_le_visible(client, bd):
    r = client.get("/sante", headers=auth).json()
    assert r["ok"] is True and r["documents"] == 6
    assert len(r["version"]) == 64 and int(r["version"], 16) >= 0
    bd.execute("UPDATE sources SET visibilite = 'prive'")
    bd.commit()
    assert client.get("/sante", headers=auth).json()["documents"] == 0
    assert client.get("/sante", headers=auth_prive).json()["documents"] == 6


def test_sante_503_si_base_absente(cfg, faux_modeles):
    r = Reglages(database_url="postgresql://x:x@localhost:1/x", litellm_url="http://x",
                 jeton="pub", config=cfg)
    assert TestClient(create_app(r, faux_modeles)).get("/sante").status_code == 503


def test_chercher_exige_le_jeton(client):
    assert client.post("/chercher", json={"question": "poulet"}).status_code == 401
    assert client.post("/chercher", json={"question": "poulet"},
                       headers={"Authorization": "Bearer mauvais"}).status_code == 401


def test_chercher(client):
    r = client.post("/chercher", json={"question": "poulet", "filtres": {"type": "recette"},
                                       "limite": 3}, headers=auth).json()
    assert r["resultats"][0]["cle"].startswith("coffre-maison:") and "lien" in r["resultats"][0]


def test_le_jeton_public_ne_voit_pas_une_source_privee(client, bd):
    bd.execute("UPDATE sources SET visibilite = 'prive'")
    bd.commit()
    assert client.post("/chercher", json={"question": "poulet"}, headers=auth).json()[
        "resultats"] == []
    assert client.post("/chercher", json={"question": "poulet"}, headers=auth_prive).json()[
        "resultats"]


def test_le_jeton_public_ne_voit_pas_une_source_travail(client, bd):
    bd.execute("UPDATE sources SET visibilite = 'travail'")
    bd.commit()
    assert client.post("/chercher", json={"question": "poulet"}, headers=auth).json()[
        "resultats"] == []
    assert client.post("/chercher", json={"question": "poulet"}, headers=auth_prive).json()[
        "resultats"]


def test_jeton_prive_vide_n_ouvre_rien(cfg, faux_modeles):
    c = faire(cfg, faux_modeles, jeton_prive="")
    for h in ("Bearer ", "Bearer", ""):
        assert c.post("/chercher", json={"question": "x"},
                      headers={"Authorization": h}).status_code == 401


def test_bornes(client):
    assert client.post("/chercher", json={"question": "x" * 2001}, headers=auth).status_code == 422
    assert client.post("/chercher", json={"question": "x", "limite": 51},
                       headers=auth).status_code == 422
    assert client.post("/chercher", json={"question": "x", "filtres": {"inconnu": 1}},
                       headers=auth).status_code == 422


def test_refuse_de_demarrer_sans_jeton():
    with pytest.raises(RuntimeError):
        create_app(Reglages(database_url=URL, litellm_url="http://x", jeton=""))


def test_bearer_obligatoire(client):
    for h in ("pub", "Basic pub", "bearer pub"):
        assert client.post("/chercher", json={"question": "x"},
                           headers={"Authorization": h}).status_code == 401


def test_api_chemin_expliquer_ne_rendent_jamais_500(client):
    for corps in ({"de": "config", "vers": "A"}, {"de": "zzz", "vers": "A"}, {"de": "", "vers": ""}):
        assert client.post("/chemin", json=corps, headers=auth).status_code in (200, 422)
    r = client.post("/expliquer", json={"nom": "zzz"}, headers=auth)
    assert r.status_code == 200 and r.json()["etat"] == "introuvable"
    assert client.post("/chemin", json={"de": "a", "vers": "b"}).status_code == 401


def test_api_entretien_bornes(client):
    assert client.get("/doublons?seuil=0.1", headers=auth).status_code == 422
    assert client.get("/doublons?limite=500", headers=auth).status_code == 422
    assert client.get("/orphelines").status_code == 401
    assert client.get("/doublons", headers=auth).status_code == 200
    assert client.get("/orphelines", headers=auth).json()["orphelines"]
    r = client.get("/liens-entrants?cle=coffre-maison:absente.md", headers=auth).json()
    assert r["etat"] == "introuvable"
