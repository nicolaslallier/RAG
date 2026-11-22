import re
from pathlib import Path

import yaml

BRUT = Path("deploy/portainer-stack.yml").read_text()
PILE = yaml.safe_load(BRUT)


APPLI = ("rag", "rag-indexeur")


def test_un_seul_port_lie_au_lan():
    services = PILE["services"]
    assert services["rag"]["ports"] == ["192.168.2.40:8000:8000"]
    assert "ports" not in services["rag-indexeur"] and "ports" not in services["rag-postgres"]
    assert "infra-net" not in BRUT and "networks" not in PILE
    assert "0.0.0.0" not in BRUT


def test_durcissement():
    for nom, s in PILE["services"].items():
        if nom in APPLI:
            assert s["user"] == "1000:1000" and s["read_only"] is True
            assert s["cap_drop"] == ["ALL"]
            assert s["depends_on"] == {"rag-postgres": {"condition": "service_healthy"}}
        assert "no-new-privileges:true" in s["security_opt"]
        assert s["mem_limit"] and s["restart"] == "unless-stopped"


def test_noms_du_contrat():
    e = PILE["services"]["rag"]["environment"]
    assert e["RAG_S3_ACCESS_KEY"] == "rag" and e["RAG_COFFRE_BUCKET"] == "obsidian"
    assert e["RAG_LITELLM_URL"] == "https://litellm.infra.famillelallier.net"
    assert e["RAG_S3_ENDPOINT"] == "https://s3.infra.famillelallier.net"
    assert e["RAG_LITELLM_CA"] == e["AWS_CA_BUNDLE"] == "/etc/rag/infra-ca.crt"
    assert "@rag-postgres:5432/rag" in e["RAG_DATABASE_URL"]
    assert set(PILE["services"]) == {"rag", "rag-indexeur", "rag-postgres"}
    assert PILE["services"]["rag"]["image"].startswith("ghcr.io/nicolaslallier/rag:")
    pg = PILE["services"]["rag-postgres"]
    assert re.search(r"^pgvector/pgvector:\S+@sha256:[0-9a-f]{64}$", pg["image"])
    assert pg["environment"]["POSTGRES_PASSWORD"] == "${RAG_DB_PASSWORD:?}"
    assert set(PILE["volumes"]) == {"rag-donnees", "rag-pgdata"}


def test_la_base_n_est_pas_sur_le_lan():
    for s in PILE["services"].values():
        assert "192.168.2.10" not in str(s.get("environment", {}))


def test_ni_http_ni_ip_pour_litellm_et_s3():
    e = PILE["services"]["rag"]["environment"]
    assert all(e[k].startswith("https://") for k in ("RAG_LITELLM_URL", "RAG_S3_ENDPOINT"))


def test_la_ca_de_l_infra_est_un_certificat_public():
    ca = Path("deploy/infra-ca.crt").read_text()
    assert ca.startswith("-----BEGIN CERTIFICATE-----") and "PRIVATE" not in ca


def test_les_variables_de_la_pile_sont_celles_du_script_de_rendu():
    pile = set(re.findall(r"\$\{([A-Z0-9_]+)[:}]", BRUT)) - {"IMAGE_TAG"}
    script = Path("scripts/vault-render-env.sh").read_text()
    carte = set(re.search(r'^CLES="([^"]+)"', script, re.MULTILINE).group(1).split())
    assert pile == carte


def test_aucun_secret_en_clair():
    assert not re.search(r"(sk-|ghp_|github_pat_)[A-Za-z0-9]", BRUT)
