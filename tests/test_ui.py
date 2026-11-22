import logging
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from rag.api import create_app
from rag.config import Reglages
from tests.conftest import URL

HX = {"HX-Request": "true"}


@pytest.fixture
def cfg(tmp_path, index):
    d = yaml.safe_load(Path("config/rag.yaml").read_text())
    d["sources"] = [{"nom": "coffre-maison", "type": "obsidian", "visibilite": "public-maison",
                     "emplacement": str(index.coffre)}]
    f = tmp_path / "rag.yaml"
    f.write_text(yaml.safe_dump(d))
    return f


def faire(cfg, faux_modeles, **k):
    r = Reglages(database_url=URL, litellm_url="http://x", jeton="pub", config=cfg, **k)
    return TestClient(create_app(r, faux_modeles))


@pytest.fixture
def client(cfg, faux_modeles):
    return faire(cfg, faux_modeles, jeton_prive="priv")


def connecter(client, jeton):
    client.cookies.clear()
    r = client.post("/ui/connexion", data={"jeton": jeton}, follow_redirects=False)
    assert r.status_code == 303, r.text
    return client


def test_racine_redirige_vers_ui(client):
    r = client.get("/", follow_redirects=False)
    assert r.status_code == 307 and r.headers["location"] == "/ui/"


def test_ui_sans_cookie_redirige_vers_connexion(client):
    r = client.get("/ui/", follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/ui/connexion"
    client.cookies.set("rag_session", "mauvais", path="/ui")
    r = client.get("/ui/", follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/ui/connexion"


def test_page_de_connexion(client):
    r = client.get("/ui/connexion")
    assert r.status_code == 200 and 'name="jeton"' in r.text
    assert "/ui/statique/htmx.min.js" in r.text and "/ui/statique/rag.css" in r.text


def test_statique_sans_cookie(client):
    r = client.get("/ui/statique/htmx.min.js")
    assert r.status_code == 200 and r.text.startswith("var htmx=")
    assert client.get("/ui/statique/rag.css").status_code == 200


def test_connexion_pose_un_cookie_httponly_strict(client):
    r = client.post("/ui/connexion", data={"jeton": "priv"}, follow_redirects=False)
    c = r.headers["set-cookie"]
    assert r.status_code == 303 and r.headers["location"] == "/ui/"
    assert "rag_session=" in c and "HttpOnly" in c
    assert "SameSite=strict" in c.replace("Strict", "strict")
    assert "Path=/ui" in c and "Secure" not in c


def test_connexion_refusee(client):
    for jeton in ("mauvais", "", "   "):
        r = client.post("/ui/connexion", data={"jeton": jeton}, follow_redirects=False)
        assert r.status_code == 401 and "Jeton refusé." in r.text
        assert "set-cookie" not in r.headers and 'name="jeton"' in r.text


def test_deconnexion_efface_le_cookie(client):
    connecter(client, "pub")
    r = client.post("/ui/deconnexion", follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/ui/connexion"
    assert 'rag_session=""' in r.headers["set-cookie"] and "Path=/ui" in r.headers["set-cookie"]
    assert client.get("/ui/", follow_redirects=False).status_code == 303


def test_le_jeton_n_apparait_jamais_dans_la_page(cfg, faux_modeles):
    # jeton distinctif : « priv » est aussi un préfixe de « prive » et de « privé »
    c = faire(cfg, faux_modeles, jeton_prive="Zq8-secret-jeton")
    connecter(c, "Zq8-secret-jeton")
    r = c.get("/ui/")
    assert r.status_code == 200 and "coffre-maison" in r.text
    assert "Zq8-secret-jeton" not in r.text
    r = c.post("/ui/sources/coffre-maison/reindexer", headers=HX)
    assert r.status_code == 200 and "Zq8-secret-jeton" not in r.text


def test_sources_affiche_les_comptes(client):
    r = connecter(client, "pub").get("/ui/")
    assert "coffre-maison" in r.text and "obsidian" in r.text and ">6<" in r.text


def test_sources_public_ne_voit_pas_prive(client, bd):
    bd.execute("UPDATE sources SET visibilite = 'prive'")
    bd.commit()
    assert "coffre-maison" not in connecter(client, "pub").get("/ui/").text
    assert "coffre-maison" in connecter(client, "priv").get("/ui/").text


def test_boutons_absents_pour_le_jeton_public(client):
    assert "Réindexer" not in connecter(client, "pub").get("/ui/").text
    assert "Réindexer" in connecter(client, "priv").get("/ui/").text


def test_reindexer_exige_l_ecrivain(client, bd):
    url = "/ui/sources/coffre-maison/reindexer"
    assert client.post(url, headers=HX, follow_redirects=False).status_code == 303  # sans cookie
    assert connecter(client, "pub").post(url, headers=HX).status_code == 403
    connecter(client, "priv")
    assert client.post(url).status_code == 403
    assert client.post(url, headers={"HX-Request": "false"}).status_code == 403
    assert client.post(url, headers=HX | {"Origin": "http://ailleurs.exemple"}).status_code == 403
    assert client.post(url, headers=HX | {"Origin": "null"}).status_code == 403
    assert bd.execute("SELECT demande_le FROM sources").fetchone()["demande_le"] is None
    bd.rollback()
    r = client.post(url, headers=HX | {"Origin": "http://testserver"})
    assert r.status_code == 200 and r.text.lstrip().startswith("<tr")
    assert "coffre-maison" in r.text and "en attente" in r.text
    assert bd.execute("SELECT demande_le FROM sources").fetchone()["demande_le"] is not None
    bd.rollback()
    assert client.post(url, headers=HX).status_code == 200  # Origin absent : accepté


def test_reindexer_source_inconnue(client):
    connecter(client, "priv")
    assert client.post("/ui/sources/inconnue/reindexer", headers=HX).status_code == 404


def test_sans_jeton_prive_l_interface_est_en_lecture_seule(cfg, faux_modeles):
    c = faire(cfg, faux_modeles)
    assert c.post("/ui/connexion", data={"jeton": "priv"},
                  follow_redirects=False).status_code == 401
    connecter(c, "pub")
    assert "Réindexer" not in c.get("/ui/").text
    assert c.post("/ui/sources/coffre-maison/reindexer", headers=HX).status_code == 403


def test_jeton_absent_des_journaux(cfg, faux_modeles, caplog):
    caplog.set_level(logging.DEBUG)
    c = faire(cfg, faux_modeles, jeton_prive="Zq8-secret-jeton")
    connecter(c, "Zq8-secret-jeton")
    c.get("/ui/")
    c.post("/ui/sources/coffre-maison/reindexer", headers=HX)
    c.cookies.clear()
    c.post("/ui/connexion", data={"jeton": "Zq8-secret-jeton-faux"})
    assert caplog.text and "Zq8-secret-jeton" not in caplog.text
