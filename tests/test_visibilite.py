"""Visibilité de bout en bout : aucune sortie (API, SSE, MCP, interface) ne laisse passer une
source privée ou travail au jeton public. Chaque contrôle négatif a son contrôle positif."""

import json
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from rag.api import create_app
from rag.config import Reglages, charger_config
from rag.graphe import charger_graphe
from rag.semantique import passe_semantique
from tests.conftest import URL, indexer_coffre, indexer_depot

PUB = {"Authorization": "Bearer pub"}
PRIV = {"Authorization": "Bearer priv"}
REINDEX = {"Authorization": "Bearer reindex"}
ENTETES_MCP = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}

RECETTE = "---\ntype: recette\ntags: [recette, poulet]\n---\n"
NOTES_PUB = {
    "Recettes/Poulet.md": RECETTE + "# Poulet au cidre\n\nRecette de poulet au cidre.\n",
    "Deploiement.md": "# Deploiement\n\nLe deploiement appelle `calculer_salaire_secret` du depot "
                      "depot-prive, voir paie/salaires.py.\n",
    "Accueil.md": "# Accueil\n\nPage de depart du coffre.\n",
}
NOTES_PRIVE = {
    "Journal intime.md": RECETTE + "# Journal intime\n\nRecette de poulet au cidre ZZPRIVE.\n",
    "Copie.md": NOTES_PUB["Accueil.md"],
}
NOTES_TRAVAIL = {
    "Dossier client.md": RECETTE + "# Dossier client\n\nRecette de poulet au cidre ZZTRAVAIL.\n",
}
INTERDITS = ("ZZPRIVE", "ZZTRAVAIL", "ZZCODE", "coffre-prive", "coffre-travail", "Journal intime",
             "Dossier client")


class Modeles:
    """Faux modèles : embeddings du conftest, génération fixe ; garde chaque prompt vu."""

    def __init__(self, base):
        self.base, self.vus = base, []

    def embed_documents(self, *a):
        return self.base.embed_documents(*a)

    def embed_requete(self, *a):
        return self.base.embed_requete(*a)

    def generer(self, modele, messages, temperature, json_mode=False):
        self.vus.append(messages)
        if json_mode:
            return '{"concepts": []}' if "concepts" in messages[0]["content"] else \
                '{"mode": "repondre"}'
        return "Du cidre [1]."

    def generer_flux(self, modele, messages, temperature):
        self.vus.append(messages)
        yield from ("Du cidre ", "[1].")


@pytest.fixture
def monde(bd, tmp_path, faux_modeles):
    indexer_coffre(bd, tmp_path, faux_modeles, NOTES_PUB)
    for nom, notes, vis in (("coffre-prive", NOTES_PRIVE, "prive"),
                            ("coffre-travail", NOTES_TRAVAIL, "travail")):
        indexer_coffre(bd, tmp_path, faux_modeles, notes, nom=nom)
        bd.execute("UPDATE sources SET visibilite = %s WHERE nom = %s", (vis, nom))
    bd.commit()
    indexer_depot(bd, tmp_path, faux_modeles, "depot-prive",
                  {"paie/salaires.py": "def calculer_salaire_secret():\n    return 'ZZCODE'\n"},
                  visibilite="prive")
    charger_graphe(bd, "depot-prive", {"noeuds": [
        {"cle": "f", "libelle": "salaires.py", "type": "fichier", "fichier": "paie/salaires.py"},
        {"cle": "s", "libelle": "calculer_salaire_secret", "type": "fonction",
         "fichier": "paie/salaires.py"}], "liens": []})
    bd.commit()
    return {"id_prive": bd.execute("SELECT id FROM noeuds WHERE libelle = "
                                   "'calculer_salaire_secret'").fetchone()["id"]}


@pytest.fixture
def cfg(tmp_path, monde, bd, faux_modeles):
    d = yaml.safe_load(Path("config/rag.yaml").read_text())
    d["sources"] = [{"nom": "coffre-maison", "type": "obsidian", "visibilite": "public-maison",
                     "emplacement": str(tmp_path / "coffre-maison")}]
    d["reponse"]["seuil_refus_cosinus"] = d["reponse"]["seuil_refus_rerank"] = None
    d["recherche"]["graphe"]["expansion"] = True
    f = tmp_path / "rag.yaml"
    f.write_text(yaml.safe_dump(d))
    bilan = passe_semantique(bd, charger_config(f), Modeles(faux_modeles))
    assert bilan["liens"] == 3, bilan  # Deploiement -> dépôt, fichier, symbole privés
    bd.commit()
    return f


@pytest.fixture
def modeles(faux_modeles):
    return Modeles(faux_modeles)


@pytest.fixture
def client(cfg, modeles):
    r = Reglages(database_url=URL, litellm_url="http://x", jeton="pub", jeton_prive="priv",
                 jeton_reindexer="reindex", config=cfg)
    with TestClient(create_app(r, modeles)) as c:
        yield c


def rpc(client, methode, params=None, entetes=PUB):
    r = client.post("/mcp", headers=ENTETES_MCP | entetes,
                    json={"jsonrpc": "2.0", "id": 1, "method": methode, "params": params or {}})
    assert r.status_code == 200, r.text
    return r.json()["result"]


def outil(client, nom, args, entetes=PUB):
    res = rpc(client, "tools/call", {"name": nom, "arguments": args}, entetes)
    assert not res.get("isError"), res
    return res["content"][0]["text"], json.loads(res["content"][0]["text"])


def sse(client, chemin, corps, entetes):
    with client.stream("POST", chemin, json=corps | {"flux": True}, headers=entetes) as r:
        brut = "".join(r.iter_text())
    assert r.status_code == 200, brut
    return brut, [json.loads(ligne[6:]) for ligne in brut.splitlines()
                  if ligne.startswith("data: ")]


def sources_vues(objet) -> set[str]:
    """Toute valeur de clé `source`, et le préfixe de toute `cle` « source:chemin »."""
    vues = set()
    if isinstance(objet, dict):
        for k, v in objet.items():
            if k == "source" and isinstance(v, str):
                vues.add(v)
            elif k == "cle" and isinstance(v, str) and ":" in v:
                vues.add(v.split(":", 1)[0])
            else:
                vues |= sources_vues(v)
    elif isinstance(objet, list | tuple):
        for v in objet:
            vues |= sources_vues(v)
    return vues


def sans_fuite(texte: str, objet) -> None:
    for x in INTERDITS:
        assert x not in texte, x
    assert sources_vues(objet) <= {"coffre-maison"}, sources_vues(objet)


def poster(client, chemin, corps, entetes):
    r = client.post(chemin, json=corps, headers=entetes)
    assert r.status_code == 200, r.text
    return r.text, r.json()


# --- API JSON et SSE --------------------------------------------------------------------------

def test_chercher_avec_expansion(client):
    corps = {"question": "deploiement calculer_salaire_secret", "limite": 50}
    texte, r = poster(client, "/chercher", corps, PUB)
    sans_fuite(texte, r)
    assert "ZZCODE" not in texte and r["resultats"]
    _, r = poster(client, "/chercher", corps, PRIV)
    assert "depot-prive" in sources_vues(r)


def test_lister(client):
    corps = {"question": "quelles recettes pour le poulet", "filtres": {"type": "recette"}}
    texte, r = poster(client, "/lister", corps, PUB)
    sans_fuite(texte, r)
    # la recherche complète la liste de notes publiques « probables » : seule la recette compte
    assert [e["cle"] for e in r["elements"] if e["type"] == "recette"] == [
        "coffre-maison:Recettes/Poulet.md"]
    _, r = poster(client, "/lister", corps, PRIV)
    assert {"coffre-prive", "coffre-travail"} <= sources_vues(r)
    texte, r = poster(client, "/lister", {"question": "recettes",
                                          "filtres": {"sources": ["coffre-prive"]}}, PUB)
    assert r["elements"] == []
    sans_fuite(texte.replace('"coffre-prive"', ""), r)  # le filtre demandé revient en écho


def test_repondre_json(client, modeles):
    corps = {"question": "recette de poulet au cidre"}
    texte, r = poster(client, "/repondre", corps, PUB)
    sans_fuite(texte, r)
    assert "ZZPRIVE" not in str(modeles.vus) and "ZZTRAVAIL" not in str(modeles.vus)
    poster(client, "/repondre", corps, PRIV)
    assert "ZZPRIVE" in str(modeles.vus) or "ZZTRAVAIL" in str(modeles.vus)


def test_repondre_flux(client, modeles):
    corps = {"question": "recette de poulet au cidre"}
    brut, ev = sse(client, "/repondre", corps, PUB)
    sans_fuite(brut, ev)
    sources = next(e for e in ev if e["type"] == "sources")
    assert sources["sources"] and sources_vues(sources) == {"coffre-maison"}
    assert "ZZPRIVE" not in str(modeles.vus) and "ZZTRAVAIL" not in str(modeles.vus)
    _, ev = sse(client, "/repondre", corps, PRIV)
    assert sources_vues(ev) - {"coffre-maison"}


def test_question_json(client, modeles):
    for corps in ({"question": "Quelle est la recette de poulet au cidre ?"},
                  {"question": "Quelles recettes pour le poulet ?"}):
        texte, r = poster(client, "/question", corps, PUB)
        sans_fuite(texte, r)
    assert "ZZPRIVE" not in str(modeles.vus) and "ZZTRAVAIL" not in str(modeles.vus)
    corps = {"question": "Quelle est la recette dans coffre-prive ?"}
    texte, r = poster(client, "/question", corps, PUB)
    assert r["aiguillage"]["bonus_sources"] is None
    sans_fuite(texte.replace("coffre-prive ?", ""), r)  # la question revient en écho
    _, r = poster(client, "/question", corps, PRIV)
    assert r["aiguillage"]["bonus_sources"] == ["coffre-prive"]


def test_question_flux(client):
    brut, ev = sse(client, "/question", {"question": "Quelle est la recette de poulet au cidre ?"},
                   PUB)
    sans_fuite(brut, ev)
    assert [e["type"] for e in ev][:2] == ["aiguillage", "sources"]
    _, ev = sse(client, "/question", {"question": "Quelle est la recette de poulet au cidre ?"},
                PRIV)
    assert sources_vues(ev) - {"coffre-maison"}


def test_chemin(client, monde):
    for vers in ("calculer_salaire_secret", f"id:{monde['id_prive']}"):
        corps = {"de": "Deploiement", "vers": vers}
        texte, r = poster(client, "/chemin", corps, PUB)
        assert r["etat"] == "introuvable" and r["candidats"]["b"] == []
        sans_fuite(texte, r)
        _, r = poster(client, "/chemin", corps, PRIV)
        assert r["etat"] == "ok" and len(r["etapes"]) == 2


def test_expliquer(client, monde):
    texte, r = poster(client, "/expliquer", {"nom": "Deploiement"}, PUB)
    assert (r["etat"], r["n_voisins"], r["voisins"]) == ("ok", 0, [])
    sans_fuite(texte, r)
    _, r = poster(client, "/expliquer", {"nom": "Deploiement"}, PRIV)
    assert r["n_voisins"] == 3
    for nom in ("calculer_salaire_secret", f"id:{monde['id_prive']}"):
        texte, r = poster(client, "/expliquer", {"nom": nom}, PUB)
        assert r["etat"] == "introuvable" and r["candidats"] == []
        _, r = poster(client, "/expliquer", {"nom": nom}, PRIV)
        assert r["etat"] == "ok"


def lire(client, chemin, entetes):
    r = client.get(chemin, headers=entetes)
    assert r.status_code == 200, r.text
    return r.text, r.json()


def test_doublons(client):
    texte, r = lire(client, "/doublons", PUB)
    sans_fuite(texte, r)
    _, r = lire(client, "/doublons", PRIV)
    assert {(d["a"]["cle"], d["b"]["cle"]) for d in r["doublons"]} & {
        ("coffre-maison:Accueil.md", "coffre-prive:Copie.md"),
        ("coffre-prive:Copie.md", "coffre-maison:Accueil.md")}


def test_orphelines(client):
    texte, r = lire(client, "/orphelines", PUB)
    sans_fuite(texte, r)
    assert r["orphelines"] and all(o["cle"].startswith("coffre-maison:") for o in r["orphelines"])
    _, r = lire(client, "/orphelines", PRIV)
    assert "coffre-prive:Journal intime.md" in {o["cle"] for o in r["orphelines"]}


def test_liens_entrants(client):
    chemin = "/liens-entrants?cle=coffre-prive:Journal intime.md"
    _, r = lire(client, chemin, PUB)
    assert r == {"etat": "introuvable", "note": None, "entrants": []}
    _, r = lire(client, chemin, PRIV)
    assert r["etat"] == "ok"


def test_etat(client):
    texte, r = lire(client, "/etat", PUB)
    assert [s["nom"] for s in r["sources"]] == ["coffre-maison"]
    sans_fuite(texte, r)
    assert "depot-prive" not in texte
    _, r = lire(client, "/etat", PRIV)
    assert {s["nom"] for s in r["sources"]} == {"coffre-maison", "coffre-prive", "coffre-travail",
                                                "depot-prive"}


# --- MCP --------------------------------------------------------------------------------------

def test_mcp_lister(client):
    args = {"question": "quelles recettes pour le poulet", "type": "recette"}
    texte, r = outil(client, "rag_lister", args)
    sans_fuite(texte, r)
    # la recherche complète la liste de notes publiques « probables » : seule la recette compte
    assert [e["cle"] for e in r["elements"] if e["type"] == "recette"] == [
        "coffre-maison:Recettes/Poulet.md"]
    _, r = outil(client, "rag_lister", args, PRIV)
    assert {"coffre-prive", "coffre-travail"} <= sources_vues(r)


def test_mcp_repondre(client, modeles):
    args = {"question": "recette de poulet au cidre"}
    texte, r = outil(client, "rag_repondre", args)
    sans_fuite(texte, r)
    assert "ZZPRIVE" not in str(modeles.vus) and "ZZTRAVAIL" not in str(modeles.vus)
    outil(client, "rag_repondre", args, PRIV)
    assert "ZZPRIVE" in str(modeles.vus) or "ZZTRAVAIL" in str(modeles.vus)


def test_mcp_chemin(client, monde):
    for vers in ("calculer_salaire_secret", f"id:{monde['id_prive']}"):
        args = {"de": "Deploiement", "vers": vers}
        texte, r = outil(client, "rag_chemin", args)
        assert r["etat"] == "introuvable" and r["candidats"]["b"] == []
        sans_fuite(texte, r)
        _, r = outil(client, "rag_chemin", args, PRIV)
        assert r["etat"] == "ok" and len(r["etapes"]) == 2


def test_mcp_expliquer(client, monde):
    texte, r = outil(client, "rag_expliquer", {"nom": "Deploiement"})
    assert (r["etat"], r["n_voisins"], r["voisins"]) == ("ok", 0, [])
    sans_fuite(texte, r)
    _, r = outil(client, "rag_expliquer", {"nom": "Deploiement"}, PRIV)
    assert r["n_voisins"] == 3
    for nom in ("calculer_salaire_secret", f"id:{monde['id_prive']}"):
        _, r = outil(client, "rag_expliquer", {"nom": nom})
        assert r["etat"] == "introuvable" and r["candidats"] == []


# --- Jeton de réindexation : il ne lit rien ---------------------------------------------------

@pytest.mark.parametrize("methode,chemin,corps", [
    ("POST", "/mcp", {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
    ("GET", "/etat", None),
    ("GET", "/doublons", None),
    ("GET", "/orphelines", None),
    ("GET", "/liens-entrants?cle=x", None),
    ("POST", "/lister", {"question": "x"}),
    ("POST", "/repondre", {"question": "x"}),
    ("POST", "/question", {"question": "x"}),
    ("POST", "/chemin", {"de": "a", "vers": "b"}),
    ("POST", "/expliquer", {"nom": "a"}),
])
def test_jeton_reindexer_ne_lit_rien(client, methode, chemin, corps):
    entetes = REINDEX | (ENTETES_MCP if chemin == "/mcp" else {})
    r = client.request(methode, chemin, json=corps, headers=entetes)
    assert r.status_code == 401, r.text


def test_jeton_reindexer_refuse_par_l_interface(client):
    r = client.post("/ui/connexion", data={"jeton": "reindex"}, follow_redirects=False)
    assert r.status_code == 401 and "Jeton refusé." in r.text
    assert "set-cookie" not in r.headers
    client.cookies.set("rag_session", "reindex", path="/ui")
    r = client.get("/ui/", follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/ui/connexion"
    r = client.get("/ui/", headers={"HX-Request": "true"}, follow_redirects=False)
    assert r.status_code == 401
