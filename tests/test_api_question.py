import json
from pathlib import Path

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient

from rag.api import create_app
from rag.config import Reglages
from rag.modeles import ErreurModele, Modeles
from tests.conftest import URL

auth = {"Authorization": "Bearer pub"}


class Generation:
    """Faux modèles d'embedding + génération fixe (ou en panne)."""

    def __init__(self, base, texte="Du cidre [1].", panne=False):
        self.base, self.texte, self.panne = base, texte, panne

    def embed_requete(self, *a):
        return self.base.embed_requete(*a)

    def generer(self, modele, messages, temperature, json_mode=False, delai=None):
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


def test_rerank_indisponible_l_api_demarre_quand_meme(cfg, faux_modeles):
    d = yaml.safe_load(Path(cfg).read_text())
    d["recherche"]["rerank"]["actif"] = True
    Path(cfg).write_text(yaml.safe_dump(d))

    class Casse:
        def noter(self, q, t):
            raise OSError("téléchargement impossible")
    r = Reglages(database_url=URL, litellm_url="http://x", jeton="pub", config=cfg)
    with TestClient(create_app(r, Generation(faux_modeles), reranker=Casse())) as c:
        res = c.post("/chercher", json={"question": "poulet"}, headers=auth)
        assert res.status_code == 200 and res.json()["resultats"][0]["rerank"] is None
        assert c.get("/docs").status_code == 404 and c.get("/openapi.json").status_code == 404


def test_repondre_applique_le_bonus_de_source(cfg, faux_modeles, monkeypatch):
    from rag import api
    vu = []
    vrai = api.passages_pour

    def espion(*a, **k):
        vu.append(k.get("bonus_sources"))
        return vrai(*a, **k)
    monkeypatch.setattr(api, "passages_pour", espion)
    c = faire(cfg, Generation(faux_modeles))
    c.post("/repondre", json={"question": "poulet au cidre dans coffre-maison"}, headers=auth)
    assert vu == [["coffre-maison"]]


def _evenements(c, route="/repondre", question="recette de poulet au cidre"):
    with c.stream("POST", route, json={"question": question, "flux": True}, headers=auth) as r:
        assert r.status_code == 200
        return [json.loads(ligne[6:]) for ligne in r.iter_lines() if ligne.startswith("data: ")]


def test_flux_panne_du_modele_evenement_erreur(cfg, faux_modeles):
    ev = _evenements(faire(cfg, Generation(faux_modeles, panne=True)))
    assert ev[0]["type"] == "sources"
    assert ev[-1] == {"type": "erreur", "detail": "modèle indisponible"}


def test_flux_exception_imprevue_evenement_erreur_generique(cfg, faux_modeles, caplog):
    class Casse(Generation):
        def generer_flux(self, modele, messages, temperature):
            yield "Du cidre "
            raise RuntimeError("détail interne à ne pas montrer")
    with caplog.at_level("ERROR", logger="rag.api"):
        ev = _evenements(faire(cfg, Casse(faux_modeles)))
    assert ev[-1] == {"type": "erreur", "detail": "erreur interne"}
    assert "détail interne" not in json.dumps(ev, ensure_ascii=False)
    assert "flux de réponse en échec" in caplog.text


def test_flux_question_exception_avant_les_passages(cfg, faux_modeles):
    class SansEmbedding(Generation):
        def embed_requete(self, *a):
            raise KeyError("embedding")
    ev = _evenements(faire(cfg, SansEmbedding(faux_modeles)), "/question",
                     "Quelle est la recette ?")
    assert ev[0]["type"] == "aiguillage" and ev[-1] == {"type": "erreur",
                                                        "detail": "erreur interne"}


def test_contenu_nul_du_modele_donne_503(cfg, faux_modeles):
    client = httpx.Client(transport=httpx.MockTransport(
        lambda r: httpx.Response(200, json={"choices": [{"message": {"content": None}}]})))
    vrais = Modeles("http://x", "k", client=client)
    vrais.embed_requete = faux_modeles.embed_requete
    r = faire(cfg, vrais).post("/repondre", json={"question": "poulet au cidre"}, headers=auth)
    assert r.status_code == 503 and r.json() == {"detail": "modèle indisponible"}


def test_connexion_rendue_avant_la_generation(cfg, faux_modeles, monkeypatch):
    from rag import api
    ouvertes, etats = [], []
    vraie = api.connexion

    def espionne(url):
        ouvertes.append(vraie(url))
        return ouvertes[-1]
    monkeypatch.setattr(api, "connexion", espionne)

    class Espion(Generation):
        def generer(self, modele, messages, temperature, json_mode=False, delai=None):
            if not json_mode:
                etats.append([c.closed for c in ouvertes])
            return super().generer(modele, messages, temperature, json_mode, delai)

        def generer_flux(self, modele, messages, temperature):
            etats.append([c.closed for c in ouvertes])
            yield from super().generer_flux(modele, messages, temperature)

    c = faire(cfg, Espion(faux_modeles))
    assert _evenements(c)[-1]["type"] == "fin"
    assert c.post("/repondre", json={"question": "poulet au cidre"},
                  headers=auth).json()["refus"] is False
    assert _evenements(c, "/question", "Quelle est la recette du poulet au cidre ?")[-1][
        "type"] == "fin"
    assert len(etats) == 3 and all(e and all(e) for e in etats)
