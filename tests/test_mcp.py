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


def test_les_sept_outils_du_contrat(client):
    assert sorted(t["name"] for t in rpc(client, "tools/list")["tools"]) == [
        "rag_chemin", "rag_chercher", "rag_expliquer", "rag_fichiers", "rag_lister",
        "rag_repondre", "rag_sources"]


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


def test_mcp_respecte_le_niveau_travail(client, bd):
    bd.execute("UPDATE sources SET visibilite = 'travail'")
    bd.commit()
    priv = {"Authorization": "Bearer priv"}
    assert outil(client, "rag_chercher", {"question": "recette de poulet"}) == []
    assert outil(client, "rag_chercher", {"question": "recette de poulet"}, priv)
    assert "coffre-maison" not in str(outil(client, "rag_sources", {}))
    assert "coffre-maison" in str(outil(client, "rag_sources", {}, priv))


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


def test_rag_repondre_applique_le_bonus_de_source(client, monkeypatch):
    from rag import api
    vu = []
    vrai = api.repondre
    monkeypatch.setattr(api, "repondre", lambda *a, **k: vu.append(k["bonus_sources"])
                        or vrai(*a, **k))
    outil(client, "rag_repondre", {"question": "poulet au cidre dans coffre-maison"})
    assert vu == [["coffre-maison"]]


def test_rag_sources_liste_les_sources(client):
    d = outil(client, "rag_sources", {})
    assert [x["nom"] for x in d["sources"]] == ["coffre-maison"] and d["en_cours"] is None


def test_rag_sources_donne_la_passe_en_cours(client, bd):
    bd.execute("INSERT INTO passes (genre) VALUES ('demandee')")
    bd.commit()
    assert outil(client, "rag_sources", {})["en_cours"]["genre"] == "demandee"


def test_rag_fichiers_indexes(client):
    d = outil(client, "rag_fichiers", {"source": "coffre-maison", "etat": "indexe"})
    assert d["source"] == "coffre-maison" and len(d["fichiers"]) == 6


def test_rag_fichiers_borne_la_page(client):
    from rag.gestion import PAGE_MAX

    d = outil(client, "rag_fichiers", {"source": "coffre-maison", "page": PAGE_MAX})
    assert d["fichiers"] == []
    res = rpc(client, "tools/call", {"name": "rag_fichiers",
                                     "arguments": {"source": "coffre-maison",
                                                   "page": PAGE_MAX + 1}})
    assert res["isError"] is True


def test_rag_sources_et_fichiers_respectent_les_niveaux(client, bd):
    bd.execute("UPDATE sources SET visibilite = 'prive'")
    bd.commit()
    assert outil(client, "rag_sources", {})["sources"] == []
    res = rpc(client, "tools/call", {"name": "rag_fichiers",
                                     "arguments": {"source": "coffre-maison"}})
    assert res["isError"] is True and "source inconnue" in res["content"][0]["text"]
    priv = {"Authorization": "Bearer priv"}
    assert outil(client, "rag_fichiers", {"source": "coffre-maison"}, priv)["total"] > 0


SECRET = "connexion postgresql://rag:motdepasse@hote.interne/rag /srv/secret"


def boum(*a, **k):
    raise RuntimeError(SECRET)


def erreur_de(client, nom, args):
    res = rpc(client, "tools/call", {"name": nom, "arguments": args})
    assert res["isError"] is True, res
    return res["content"][0]["text"]


def test_erreur_interne_generique_et_journalisee(client, monkeypatch, caplog):
    from rag import api
    monkeypatch.setattr(api, "chercher", boum)
    with caplog.at_level("ERROR", logger="rag.mcp"):
        texte = erreur_de(client, "rag_chercher", {"question": "x"})
    assert "erreur interne" in texte
    for x in ("hote.interne", "motdepasse", "/srv/secret", "RuntimeError"):
        assert x not in texte
    assert any(r.name == "rag.mcp" and r.exc_info and "rag_chercher" in r.getMessage()
               for r in caplog.records)


@pytest.mark.parametrize("nom,cible,args", [
    ("rag_lister", "lister", {"question": "quelles recettes", "type": "recette"}),
    ("rag_repondre", "repondre", {"question": "poulet"}),
    ("rag_chemin", "chemin", {"de": "a", "vers": "b"}),
    ("rag_expliquer", "expliquer", {"nom": "a"}),
    ("rag_sources", "gestion.sources", {}),
    ("rag_fichiers", "gestion.fichiers", {"source": "coffre-maison"}),
])
def test_erreur_interne_sur_chaque_outil(client, monkeypatch, nom, cible, args):
    from rag import api
    module, _, attr = cible.rpartition(".")
    monkeypatch.setattr(getattr(api, module) if module else api, attr, boum)
    texte = erreur_de(client, nom, args)
    assert "erreur interne" in texte and "hote.interne" not in texte and "/srv/secret" not in texte


def test_modele_indisponible_dit_sans_detail(client, monkeypatch):
    from rag import api
    from rag.modeles import ErreurModele

    def panne(*a, **k):
        raise ErreurModele("LiteLLM chat : HTTP 500 https://litellm.interne")
    monkeypatch.setattr(api, "repondre", panne)
    texte = erreur_de(client, "rag_repondre", {"question": "poulet"})
    assert "modèle indisponible" in texte and "litellm.interne" not in texte
