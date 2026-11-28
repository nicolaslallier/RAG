"""Gardes HTTP : plafond du corps avant toute lecture (donc avant le jeton)."""

import asyncio
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from rag.api import create_app
from rag.config import Reglages
from rag.garde import CORPS_MAX, LimiteCorps
from tests.conftest import URL

auth = {"Authorization": "Bearer pub"}
JSON = {"Content-Type": "application/json"}
MCP = {"Accept": "application/json, text/event-stream"} | JSON
HX = {"HX-Request": "true"}


# --- Unitaires ASGI, sans base ----------------------------------------------------------------

class Appli:
    """Application factice : lit le corps jusqu'au bout (ou la déconnexion), puis répond 200."""

    def __init__(self, leve_apres_deconnexion=False):
        self.recu, self.deconnecte, self.leve = b"", False, leve_apres_deconnexion

    async def __call__(self, scope, receive, send):
        while True:
            m = await receive()
            if m["type"] == "http.disconnect":
                self.deconnecte = True
                if self.leve:
                    raise RuntimeError("client parti")
                break
            self.recu += m.get("body", b"")
            if not m.get("more_body"):
                break
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})


def appeler(app, entetes=(), morceaux=None, chemin="/x"):
    """Rend (messages envoyés, nombre d'appels à receive)."""
    envoyes, lus = [], 0
    morceaux = list(morceaux) if morceaux is not None else [b""]

    async def receive():
        nonlocal lus
        lus += 1
        if lus <= len(morceaux):
            return {"type": "http.request", "body": morceaux[lus - 1],
                    "more_body": lus < len(morceaux)}
        return {"type": "http.request", "body": b"x" * 16384, "more_body": True}  # sans fin

    async def send(m):
        envoyes.append(m)

    scope = {"type": "http", "method": "POST", "path": chemin, "headers": list(entetes),
             "query_string": b"", "root_path": ""}
    asyncio.run(app(scope, receive, send))
    return envoyes, lus


def statuts(envoyes):
    return [m["status"] for m in envoyes if m["type"] == "http.response.start"]


def test_content_length_au_dela_du_plafond_413_sans_lire():
    envoyes, lus = appeler(LimiteCorps(Appli()), [(b"content-length", b"65537")])
    assert statuts(envoyes) == [413] and lus == 0


def test_content_length_au_plafond_passe():
    corps = b"x" * CORPS_MAX
    appli = Appli()
    envoyes, _ = appeler(LimiteCorps(appli), [(b"content-length", b"65536")], [corps])
    assert statuts(envoyes) == [200] and appli.recu == corps


def test_flux_sans_content_length_coupe_au_plafond():
    appli = Appli()
    envoyes, lus = appeler(LimiteCorps(appli), morceaux=[])  # morceaux de 16 Ko, sans fin
    assert statuts(envoyes) == [413] and appli.deconnecte and lus <= 5


def test_exception_par_chemin():
    garde = LimiteCorps(Appli(), exceptions={"/gros": 1000})
    envoyes, _ = appeler(garde, [(b"content-length", b"999999999")], [b"x" * 500], chemin="/gros")
    assert statuts(envoyes) == [200]  # la route juge elle-même son Content-Length
    appli = Appli()
    envoyes, _ = appeler(LimiteCorps(appli, exceptions={"/gros": 1000}),
                         morceaux=[b"x" * 600, b"x" * 600], chemin="/gros")
    assert statuts(envoyes) == [413] and appli.deconnecte


def test_content_length_demesure():
    envoyes, lus = appeler(LimiteCorps(Appli()), [(b"content-length", b"9" * 30)])
    assert statuts(envoyes) == [413] and lus == 0


def test_exception_de_l_application_apres_refus_avalee():
    envoyes, _ = appeler(LimiteCorps(Appli(leve_apres_deconnexion=True)), morceaux=[])
    assert statuts(envoyes) == [413]


def test_exception_de_l_application_sans_refus_remonte():
    class Boum:
        async def __call__(self, scope, receive, send):
            raise RuntimeError("boum")
    with pytest.raises(RuntimeError):
        appeler(LimiteCorps(Boum()))


# --- Bout en bout -----------------------------------------------------------------------------

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
    with faire(cfg, faux_modeles, jeton_prive="priv") as c:
        yield c


def gros_json(octets=70 * 1024):
    return b'{"question": "' + b"x" * octets + b'"}'


def test_corps_trop_gros_refuse_avant_le_jeton(client):
    assert client.post("/chercher", content=gros_json(), headers=JSON).status_code == 413
    assert client.post("/chercher", json={"question": "x"}).status_code == 401


def test_corps_normal_passe(client):
    assert client.post("/chercher", json={"question": "poulet"}, headers=auth).status_code == 200


def test_flux_trop_gros(client):
    r = client.post("/chercher", content=(b"x" * 1024 for _ in range(100)), headers=auth | JSON)
    assert r.status_code == 413


def test_mcp_corps_trop_gros(client):
    assert client.post("/mcp", content=gros_json(), headers=auth | MCP).status_code == 413


def test_import_de_l_interface_garde_son_plafond(client):
    client.post("/ui/connexion", data={"jeton": "priv"}, follow_redirects=False)
    mo = 1024 * 1024
    juste = b"[]\n" + b"#" * (mo - 4) + b"\n"
    r = client.post("/ui/exclusions/import", headers=HX,
                    files={"fichier": ("e.yaml", juste, "text/yaml")})
    assert r.status_code == 200, r.text
    r = client.post("/ui/exclusions/import", headers=HX,
                    files={"fichier": ("e.yaml", b"#" * (2 * mo), "text/yaml")})
    assert r.status_code == 413 and "Fichier trop gros" in r.text


def test_les_chemins_d_exception_existent(client):
    from rag.ui import CHEMINS_IMPORT

    assert CHEMINS_IMPORT
    for chemin in CHEMINS_IMPORT:  # route POST existante : sans session, renvoi vers la connexion
        r = client.post(chemin, follow_redirects=False)
        assert (r.status_code, r.headers.get("location")) == (303, "/ui/connexion"), chemin
    assert client.post("/ui/inexistant/import", follow_redirects=False).status_code == 404


# --- En-têtes de sécurité ---------------------------------------------------------------------

SECURITE = ("x-frame-options", "content-security-policy", "x-content-type-options",
            "referrer-policy")


def test_entetes_de_securite_sur_l_interface(client):
    h = client.get("/ui/connexion").headers
    assert h["x-frame-options"] == "DENY" and h["x-content-type-options"] == "nosniff"
    assert h["referrer-policy"] == "same-origin" and h["cache-control"] == "no-store"
    csp = h["content-security-policy"]
    assert "default-src 'self'" in csp and "frame-ancestors 'none'" in csp
    assert "unsafe-inline" not in csp and "unsafe-eval" not in csp


@pytest.mark.parametrize("requete", [
    ("GET", "/sante", {}, None),
    ("GET", "/", {}, None),
    ("GET", "/ui/", {}, None),
    ("GET", "/ui/statique/htmx.min.js", {}, None),
    ("POST", "/chercher", JSON, b'{"question": "x"}'),
    ("POST", "/mcp", MCP, b'{"jsonrpc": "2.0", "id": 1, "method": "tools/list"}'),
    ("GET", "/inexistant", {}, None),
    ("POST", "/chercher", JSON, gros_json()),
    ("FRAGMENT", "/ui/sources/coffre-maison", HX, None),
], ids=["sante", "racine", "ui-sans-cookie", "statique", "401", "mcp-401", "404", "413",
        "fragment"])
def test_entetes_sur_toutes_les_reponses(client, requete):
    methode, chemin, entetes, corps = requete
    if methode == "FRAGMENT":
        client.post("/ui/connexion", data={"jeton": "pub"}, follow_redirects=False)
        methode = "GET"
    r = client.request(methode, chemin, headers=entetes, content=corps, follow_redirects=False)
    assert all(n in r.headers for n in SECURITE), (r.status_code, dict(r.headers))
    if requete[0] == "FRAGMENT":
        assert r.status_code == 200


def test_en_tete_deja_pose_non_ecrase():
    from rag.garde import EntetesSecurite

    class Appli:
        async def __call__(self, scope, receive, send):
            await send({"type": "http.response.start", "status": 200,
                        "headers": [(b"x-frame-options", b"SAMEORIGIN")]})
            await send({"type": "http.response.body", "body": b""})

    envoyes, _ = appeler(EntetesSecurite(Appli()))
    entetes = envoyes[0]["headers"]
    assert [v for n, v in entetes if n == b"x-frame-options"] == [b"SAMEORIGIN"]
    assert {n for n, _ in entetes} >= {n.encode() for n in SECURITE}


def test_gabarits_sans_script_ni_style_en_ligne():
    import re

    interdits = re.compile(r"<style|\sstyle=|<script(?![^>]*\ssrc=)|\son\w+=|hx-on|hx-vars|"
                           r"javascript:|js:", re.IGNORECASE)
    for f in Path("rag/ui/gabarits").glob("*.html"):
        texte = re.sub(r"\{#.*?#\}", "", f.read_text(), flags=re.DOTALL)  # commentaires Jinja
        assert not interdits.search(texte), (f.name, interdits.search(texte))


def test_htmx_configure_pour_la_csp(client):
    import html
    import json
    import re

    page = client.get("/ui/connexion").text
    brut = re.search(r"""<meta name="htmx-config" content='([^']*)'>""", page).group(1)
    conf = json.loads(html.unescape(brut))
    assert conf["includeIndicatorStyles"] is False and conf["allowEval"] is False
    assert len(conf["responseHandling"]) == 4
