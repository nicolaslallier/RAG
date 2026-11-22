import json
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from rag.api import create_app
from rag.config import Reglages
from tests.conftest import URL

auth = {"Authorization": "Bearer pub"}
ENTETES = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}


class Generation:
    def __init__(self, base):
        self.base = base

    def embed_requete(self, *a):
        return self.base.embed_requete(*a)

    def generer(self, modele, messages, temperature, json_mode=False):
        return "Du cidre [1]."


@pytest.fixture
def client(tmp_path, index, faux_modeles):
    d = yaml.safe_load(Path("config/rag.yaml").read_text())
    d["sources"] = [{"nom": "coffre-maison", "type": "obsidian", "visibilite": "public-maison",
                     "emplacement": str(index.coffre)}]
    d["reponse"]["seuil_refus_cosinus"] = d["reponse"]["seuil_refus_rerank"] = None
    f = tmp_path / "rag.yaml"
    f.write_text(yaml.safe_dump(d))
    r = Reglages(database_url=URL, litellm_url="http://x", jeton="pub", jeton_prive="priv",
                 config=f)
    with TestClient(create_app(r, Generation(faux_modeles))) as c:
        yield c


def rpc(client, methode, params=None, entetes=auth):
    r = client.post("/mcp", headers=ENTETES | entetes,
                    json={"jsonrpc": "2.0", "id": 1, "method": methode, "params": params or {}})
    assert r.status_code == 200, r.text
    return r.json()["result"]


def outil(client, nom, args, entetes=auth):
    res = rpc(client, "tools/call", {"name": nom, "arguments": args}, entetes)
    assert not res.get("isError"), res
    return json.loads(res["content"][0]["text"])


def test_les_cinq_outils_du_contrat(client):
    assert sorted(t["name"] for t in rpc(client, "tools/list")["tools"]) == [
        "rag_chemin", "rag_chercher", "rag_expliquer", "rag_lister", "rag_repondre"]


def test_rag_chercher_rend_des_sources(client):
    d = outil(client, "rag_chercher", {"question": "recette de poulet"})
    assert d[0]["cle"].startswith("coffre-maison:") and d[0]["lien"] and "origine" in d[0]


def test_rag_lister_extrait_les_filtres(client):
    d = outil(client, "rag_lister", {"question": "quelles recettes pour le poulet"})
    assert d["mode"] == "metadonnees" and d["filtres"]["type"] == "recette"


def test_rag_repondre(client):
    d = outil(client, "rag_repondre", {"question": "poulet au cidre"})
    assert d == {"refus": False, "texte": "Du cidre [1].", "sources": d["sources"]}


def test_rag_chemin_et_expliquer(client):
    assert outil(client, "rag_expliquer", {"nom": "zzz"})["etat"] == "introuvable"
    assert "etat" in outil(client, "rag_chemin", {"de": "a", "vers": "b"})


def test_mcp_sans_jeton_401(client):
    corps = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
    assert client.post("/mcp", json=corps).status_code == 401
    assert client.post("/mcp", json=corps, headers={"Authorization": "Bearer x"}).status_code == 401


def test_mcp_respecte_les_niveaux(client, bd):
    bd.execute("UPDATE sources SET visibilite = 'prive'")
    bd.commit()
    assert outil(client, "rag_chercher", {"question": "recette de poulet"}) == []
    assert outil(client, "rag_chercher", {"question": "recette de poulet"},
                 {"Authorization": "Bearer priv"})


def test_aucun_outil_de_reindexation():
    assert "reindex" not in Path("rag/mcp_serveur.py").read_text().lower()


@pytest.mark.parametrize("nom,args", [
    ("rag_chercher", {"question": "x" * 2001}),
    ("rag_chercher", {"question": "x", "limite": 51}),
    ("rag_lister", {"question": "x" * 2001}),
    ("rag_repondre", {"question": "x" * 2001}),
    ("rag_chemin", {"de": "a" * 501, "vers": "b"}),
    ("rag_expliquer", {"nom": "a" * 501}),
])
def test_entrees_trop_longues_refusees(client, nom, args):
    res = rpc(client, "tools/call", {"name": nom, "arguments": args})
    assert res["isError"] is True
