import re
from pathlib import Path

import yaml

BRUT = Path("deploy/portainer-stack.yml").read_text()
PILE = yaml.safe_load(BRUT)


def test_aucun_port_publie_et_reseau_externe():
    assert all("ports" not in s for s in PILE["services"].values())
    assert PILE["networks"]["infra-net"] == {"external": True, "name": "infra-net"}
    assert all(s["networks"] == ["infra-net"] for s in PILE["services"].values())


def test_durcissement():
    for s in PILE["services"].values():
        assert s["user"] == "1000:1000" and s["read_only"] is True and s["cap_drop"] == ["ALL"]
        assert "no-new-privileges:true" in s["security_opt"]
        assert s["mem_limit"] and s["restart"] == "unless-stopped"


def test_noms_du_contrat():
    e = PILE["services"]["rag"]["environment"]
    assert e["RAG_S3_ACCESS_KEY"] == "rag" and e["RAG_COFFRE_BUCKET"] == "obsidian"
    assert e["RAG_LITELLM_URL"] == "http://litellm:4000"
    assert "@postgres:5432/rag" in e["RAG_DATABASE_URL"]
    assert set(PILE["services"]) == {"rag", "rag-indexeur"}
    assert PILE["services"]["rag"]["image"].startswith("ghcr.io/nicolaslallier/rag:")


def test_les_variables_de_la_pile_sont_celles_du_script_de_rendu():
    pile = set(re.findall(r"\$\{([A-Z0-9_]+)[:}]", BRUT)) - {"IMAGE_TAG"}
    script = Path("scripts/vault-render-env.sh").read_text()
    carte = set(re.search(r'^CLES="([^"]+)"', script, re.MULTILINE).group(1).split())
    assert pile == carte


def test_aucun_secret_en_clair():
    assert not re.search(r"(sk-|ghp_|github_pat_)[A-Za-z0-9]", BRUT)
