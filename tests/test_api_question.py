import json
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from rag.api import create_app
from rag.config import Reglages
from rag.modeles import ErreurModele
from tests.conftest import URL

auth = {"Authorization": "Bearer pub"}


class Generation:
    """Faux modèles d'embedding + génération fixe (ou en panne)."""

    def __init__(self, base, texte="Du cidre [1].", panne=False):
        self.base, self.texte, self.panne = base, texte, panne

    def embed_requete(self, *a):
        return self.base.embed_requete(*a)

    def generer(self, modele, messages, temperature, json_mode=False):
        if self.panne:
            raise ErreurModele("panne")
        return '{"mode": "repondre"}' if json_mode else self.texte

    def generer_flux(self, modele, messages, temperature):
        if self.panne:
            raise ErreurModele("panne")
        yield from ("Du cidre ", "[1].")


@pytest.fixture
def cfg(tmp_path, index):
    d = yaml.safe_load(Path("config/rag.yaml").read_text())
    d["sources"] = [{"nom": "coffre-maison", "type": "obsidian", "visibilite": "public-maison",
                     "emplacement": str(index.coffre)}]
    d["reponse"]["seuil_refus_cosinus"] = d["reponse"]["seuil_refus_rerank"] = None
    f = tmp_path / "rag.yaml"
    f.write_text(yaml.safe_dump(d))
    return f


def faire(cfg, modeles):
    r = Reglages(database_url=URL, litellm_url="http://x", jeton="pub", config=cfg)
    return TestClient(create_app(r, modeles))


@pytest.fixture
def client(cfg, faux_modeles):
    return faire(cfg, Generation(faux_modeles))


def test_question_aiguille_vers_lister(client):
    r = client.post("/question", json={
        "question": "Quelles recettes j'ai pour le poulet, avec les sources ?"}, headers=auth).json()
    assert r["mode"] == "lister" and r["aiguillage"]["par"] == "regles"
    assert r["mode_liste"] == "metadonnees"
    assert (r["aiguillage"]["filtres"]["type"], r["aiguillage"]["filtres"]["tags"]) == (
        "recette", ["poulet"])
    assert r["elements"] and all("lien" in e and "origine" in e for e in r["elements"])


def test_question_aiguille_vers_repondre(client):
    r = client.post("/question", json={"question": "Quelle est la recette du poulet au cidre ?"},
                    headers=auth).json()
    assert r["mode"] == "repondre" and r["aiguillage"]["par"] == "regles"
    assert r["refus"] is False and r["citations"] == [1] and r["sources"][0]["cite"]


def test_mode_et_filtres_forces(client):
    r = client.post("/question", json={"question": "Quelle est…", "mode": "lister",
                                       "filtres": {"type": "recette"}}, headers=auth).json()
    assert r["aiguillage"]["par"] == "appelant" and r["mode"] == "lister"
    assert r["elements"] and r["aiguillage"]["filtres"]["type"] == "recette"


def test_lister_et_repondre_directs(client):
    r = client.post("/lister", json={"question": "recettes", "filtres": {"type": "recette"}},
                    headers=auth).json()
    assert r["mode"] == "metadonnees" and [e["certitude"] for e in r["elements"]].count("tague") == 2
    r = client.post("/repondre", json={"question": "poulet au cidre"}, headers=auth).json()
    assert r["texte"] == "Du cidre [1]." and r["raison"] == ""


def test_flux_sse(client):
    with client.stream("POST", "/repondre", json={"question": "recette de poulet au cidre",
                                                  "flux": True}, headers=auth) as r:
        ev = [json.loads(ligne[6:]) for ligne in r.iter_lines() if ligne.startswith("data: ")]
    assert r.headers["content-type"].startswith("text/event-stream")
    assert ev[0]["type"] == "sources" and ev[-1]["type"] == "fin" and ev[-1]["citations"] == [1]


def test_question_en_flux(client):
    with client.stream("POST", "/question", json={"question": "Quelle est la recette ?",
                                                  "flux": True}, headers=auth) as r:
        ev = [json.loads(ligne[6:]) for ligne in r.iter_lines() if ligne.startswith("data: ")]
    assert ev[0]["type"] == "aiguillage" and ev[0]["mode"] == "repondre"
    assert ev[1]["type"] == "sources" and ev[-1]["type"] == "fin"


def test_tout_exige_un_jeton(client):
    assert {client.post(p, json={"question": "x"}).status_code
            for p in ("/question", "/lister", "/repondre")} == {401}


def test_panne_du_modele_donne_503_pas_500(cfg, faux_modeles):
    c = faire(cfg, Generation(faux_modeles, panne=True))
    r = c.post("/repondre", json={"question": "poulet au cidre"}, headers=auth)
    assert r.status_code == 503 and r.json() == {"detail": "modèle indisponible"}
