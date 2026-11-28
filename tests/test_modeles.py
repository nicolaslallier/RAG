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


@pytest.mark.skipif(not __import__("shutil").which("openssl"), reason="openssl absent")
def test_contexte_tls_ca_maison(tmp_path):
    import ssl
    import subprocess

    from rag.modeles import contexte_tls
    cle, crt = tmp_path / "k.pem", tmp_path / "ca.crt"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", cle,
                    "-out", crt, "-days", "1", "-subj", "/CN=ca-test"], check=True,
                   capture_output=True)
    ctx, defaut = contexte_tls(crt), contexte_tls(None)
    # seul le drapeau strict diffère, la CA maison est chargée en plus des CA système
    assert ctx.verify_flags == defaut.verify_flags & ~ssl.VERIFY_X509_STRICT
    assert any(("commonName", "ca-test") in rdn for c in ctx.get_ca_certs() for rdn in c["subject"])
    assert len(ctx.get_ca_certs()) > len(defaut.get_ca_certs()) or defaut.get_ca_certs() == []
    for c in (ctx, defaut):
        assert c.verify_mode == ssl.CERT_REQUIRED and c.check_hostname
    assert defaut.verify_flags & ssl.VERIFY_X509_STRICT


def test_generer_flux_lit_le_sse():
    vu = []

    def gerer(req):
        vu.append(json.loads(req.content))
        ev = [{"choices": [{"delta": {"role": "assistant"}}]},
              {"choices": [{"delta": {"content": "Du cidre "}}]},
              {"choices": [{"delta": {"content": "[1]."}}]}]
        corps = "".join(f"data: {json.dumps(e)}\n\n" for e in ev) + "data: [DONE]\n\n"
        return httpx.Response(200, text=corps, headers={"content-type": "text/event-stream"})

    m = Modeles("http://x", "k", client=httpx.Client(transport=httpx.MockTransport(gerer)))
    assert list(m.generer_flux("g", [{"role": "user", "content": "q"}], 0.1)) == [
        "Du cidre ", "[1]."]
    assert vu[0]["stream"] is True and vu[0]["temperature"] == 0.1


def test_generer_flux_erreur_http():
    m = Modeles("http://x", "secret", client=httpx.Client(
        transport=httpx.MockTransport(lambda r: httpx.Response(500))))
    with pytest.raises(ErreurModele) as e:
        list(m.generer_flux("g", [], 0))
    assert "secret" not in str(e.value)


def _flux(evenements, done=True):
    corps = "".join(f"data: {e if isinstance(e, str) else json.dumps(e)}\n\n"
                    for e in evenements) + ("data: [DONE]\n\n" if done else "")
    return Modeles("http://x", "sk-secret", client=httpx.Client(transport=httpx.MockTransport(
        lambda r: httpx.Response(200, text=corps, headers={"content-type": "text/event-stream"}))))


def _d(texte, fin=None):
    return {"choices": [{"delta": {"content": texte}, "finish_reason": fin}]}


def test_flux_evenement_error_leve_sans_fuite():
    m = _flux([_d("a"), {"error": {"message": "boom sk-secret", "code": 500}}])
    with pytest.raises(ErreurModele) as e:
        list(m.generer_flux("g", [], 0))
    assert "sk-secret" not in str(e.value) and "boom" not in str(e.value)


def test_flux_coupe_sans_done_ni_finish_reason_leve():
    recus = []
    with pytest.raises(ErreurModele):
        recus.extend(_flux([_d("a")], done=False).generer_flux("g", [], 0))  # garde le reçu
    assert recus == ["a"]


def test_flux_finish_reason_sans_done_est_une_fin_normale():
    assert list(_flux([_d("a", "stop")], done=False).generer_flux("g", [], 0)) == ["a"]


def test_flux_length_signale_la_troncature():
    from rag.modeles import TRONQUE
    assert list(_flux([_d("a"), _d(None, "length")]).generer_flux("g", [], 0)) == ["a", TRONQUE]


def test_flux_bloc_sans_choices_ignore():
    assert list(_flux([_d("a"), {"choices": [], "usage": {}}]).generer_flux("g", [], 0)) == ["a"]


@pytest.mark.parametrize("brut", ["[1, 2]", "42", "{oups", '{"choices": ["x"]}',
                                  '{"choices": [{"delta": {"content": 5}}]}'])
def test_flux_evenement_malforme_leve_erreur_modele(brut):
    with pytest.raises(ErreurModele):
        list(_flux([brut]).generer_flux("g", [], 0))


@pytest.fixture
def sans_attente(monkeypatch):
    dodo = []
    monkeypatch.setattr("rag.modeles.time.sleep", dodo.append)
    return dodo


def _client(gerer):
    return Modeles("http://x", "sk-test", client=httpx.Client(transport=httpx.MockTransport(gerer)))


def _vecs(req):
    n = len(json.loads(req.content)["input"])
    return httpx.Response(200, json={"data": [{"index": i, "embedding": [0.0] * 1024}
                                              for i in range(n)]})


def test_delais_par_usage():
    vus = []

    def gerer(req):
        vus.append(req.extensions["timeout"])
        if req.url.path.endswith("/embeddings"):
            return _vecs(req)
        return httpx.Response(200, json={"choices": [{"message": {"content": "{}"}}]})
    m = _client(gerer)
    m.embed_requete("q", M, "i")
    m.embed_documents(["a"], M)
    m.generer("g", [], 0)
    m.generer("g", [], 0, json_mode=True, delai=15)
    assert [v["read"] for v in vus] == [15, 300, 300, 15]
    assert all(v["connect"] == 5 and v["pool"] == 5 for v in vus)


def test_delai_du_flux_et_du_client_par_defaut():
    vus = []

    def gerer(req):
        vus.append(req.extensions["timeout"])
        return httpx.Response(200, text="data: [DONE]\n\n")
    list(_client(gerer).generer_flux("g", [], 0))
    assert (vus[0]["read"], vus[0]["connect"]) == (300, 5)
    assert Modeles("http://x", "k").client.timeout == httpx.Timeout(300, connect=5, pool=5)


@pytest.mark.parametrize("statut", [502, 503, 504])
def test_embeddings_repris_sur_erreur_transitoire(statut, sans_attente):
    appels = []

    def gerer(req):
        appels.append(1)
        return httpx.Response(statut) if len(appels) < 3 else _vecs(req)
    assert len(_client(gerer).embed_requete("q", M, "i")) == 1024
    assert len(appels) == 3 and sans_attente == [0.5, 1.5]


def test_embeddings_repris_sur_erreur_de_connexion_puis_abandon(sans_attente):
    appels = []

    def gerer(req):
        appels.append(1)
        raise httpx.ConnectError("refus")
    with pytest.raises(ErreurModele) as e:
        _client(gerer).embed_documents(["a"], M)
    assert len(appels) == 3 and "ConnectError" in str(e.value) and "sk-test" not in str(e.value)


def test_pas_de_reprise_sur_500_ni_sur_delai_de_lecture(sans_attente):
    appels = []

    def lent(req):
        appels.append(1)
        raise httpx.ReadTimeout("lent")
    with pytest.raises(ErreurModele):
        _client(lent).embed_requete("q", M, "i")
    with pytest.raises(ErreurModele):
        _client(lambda r: (appels.append(1), httpx.Response(500))[1]).embed_documents(["a"], M)
    assert len(appels) == 2 and sans_attente == []


def test_generation_jamais_reprise(sans_attente):
    appels = []

    def gerer(req):
        appels.append(1)
        return httpx.Response(503)
    with pytest.raises(ErreurModele):
        _client(gerer).generer("g", [], 0)
    with pytest.raises(ErreurModele):
        list(_client(gerer).generer_flux("g", [], 0))
    assert len(appels) == 2 and sans_attente == []


@pytest.mark.parametrize("corps", [{"choices": [{"message": {"content": None}}]},
                                   {"choices": []}, {"choices": [{"message": None}]}, [1], "x"])
def test_generation_malformee_leve_erreur_modele(corps):
    with pytest.raises(ErreurModele):
        _client(lambda r: httpx.Response(200, json=corps)).generer("g", [], 0)


@pytest.mark.parametrize("data", [[{"index": 0}], [{"embedding": [0.0] * 1024}], None, [None],
                                  [{"index": 0, "embedding": None}], "x"])
def test_embeddings_malformes_levent_erreur_modele(data):
    with pytest.raises(ErreurModele):
        _client(lambda r: httpx.Response(200, json={"data": data})).embed_documents(["a"], M)
    with pytest.raises(ErreurModele):
        _client(lambda r: httpx.Response(200, json=[1])).embed_documents(["a"], M)


def _flux_coupe(evenements):
    def corps():
        for e in evenements:
            yield f"data: {json.dumps(e)}\n\n".encode()
        raise httpx.ReadError("connexion tombée")
    return Modeles("http://x", "sk-secret", client=httpx.Client(transport=httpx.MockTransport(
        lambda r: httpx.Response(200, content=corps(),
                                 headers={"content-type": "text/event-stream"}))))


def test_flux_coupure_apres_finish_reason_garde_la_reponse():
    assert list(_flux_coupe([_d("a"), _d(None, "stop")]).generer_flux("g", [], 0)) == ["a"]


def test_flux_coupure_apres_length_signale_la_troncature():
    from rag.modeles import TRONQUE
    assert list(_flux_coupe([_d("a", "length")]).generer_flux("g", [], 0)) == ["a", TRONQUE]


def test_flux_coupure_avant_finish_reason_leve():
    with pytest.raises(ErreurModele, match="ReadError"):
        list(_flux_coupe([_d("a")]).generer_flux("g", [], 0))
