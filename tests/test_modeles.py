import json
import math
import os
import random
from pathlib import Path

import httpx
import pytest

from rag.config import charger_config
from rag.modeles import ErreurModele, Modeles

M = "qwen3-embedding:0.6b"


def faux(vu, dims=1024, statut=200, melanger=False):
    def gerer(req):
        corps = json.loads(req.content)
        vu.append(corps)
        if statut != 200:
            return httpx.Response(statut, text="boom")
        data = [{"index": i, "embedding": [float(i)] * dims} for i in range(len(corps["input"]))]
        if melanger:
            random.shuffle(data)
        return httpx.Response(200, json={"data": data})
    return httpx.Client(transport=httpx.MockTransport(gerer))


def test_documents_sans_instruction_requete_avec():
    vu = []
    m = Modeles("http://x", "sk-test", client=faux(vu))
    m.embed_documents(["poulet"], M)
    assert vu[-1]["input"] == ["poulet"]
    m.embed_requete("poulet ?", M, "Trouve les passages")
    assert vu[-1]["input"] == ["Instruct: Trouve les passages\nQuery: poulet ?"]


def test_lots_de_32_et_ordre_conserve():
    vu = []
    r = Modeles("http://x", "k", client=faux(vu, melanger=True)).embed_documents(
        [str(i) for i in range(70)], M)
    assert len(vu) == 3 and [v[0] for v in r] == [float(i % 32) for i in range(70)]


def test_mauvaise_dimension_leve():
    with pytest.raises(ErreurModele):
        Modeles("http://x", "k", client=faux([], dims=768)).embed_documents(["x"], M)


def test_erreur_http_sans_fuite_de_cle():
    m_500 = Modeles("http://x", "sk-test", client=faux([], statut=500))
    with pytest.raises(ErreurModele) as e:
        m_500.embed_documents(["x"], "q")
    assert "sk-test" not in str(e.value)


def test_generer_json_mode():
    vu = []

    def gerer(req):
        vu.append(json.loads(req.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": "{}"}}]})
    m = Modeles("http://x", "k", client=httpx.Client(transport=httpx.MockTransport(gerer)))
    assert m.generer("g", [{"role": "user", "content": "x"}], 0.1, json_mode=True) == "{}"
    assert vu[0]["response_format"] == {"type": "json_object"} and vu[0]["stream"] is False


def cosinus(a, b):
    return sum(x * y for x, y in zip(a, b)) / math.sqrt(sum(x * x for x in a) * sum(y * y for y in b))


@pytest.fixture
def modeles_vivants():
    if not os.environ.get("RAG_LITELLM_CLE"):
        pytest.skip("RAG_LITELLM_CLE absente : la clé LiteLLM `rag` n'existe pas encore (PR Infra A)")
    from rag.config import Reglages
    from rag.modeles import modeles_depuis
    return modeles_depuis(Reglages(database_url=""))


@pytest.mark.vivant
def test_qwen3_bout_en_bout(modeles_vivants):
    inst = charger_config(Path("config/rag.yaml")).recherche.instruction_requete
    doc_p, doc_g = modeles_vivants.embed_documents(
        ["Poulet cidre et Boursin : poitrines de poulet, carottes, cidre.",
         "Tableaux de bord Grafana du homelab."], M)
    assert len(doc_p) == 1024
    q = modeles_vivants.embed_requete("Quelle recette contient du poulet ?", M, inst)
    brut = modeles_vivants.embed_documents(["Quelle recette contient du poulet ?"], M)[0]
    assert cosinus(q, brut) < 0.999
    assert cosinus(q, doc_p) > cosinus(q, doc_g)
