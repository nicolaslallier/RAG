import re
from pathlib import Path

import yaml

W = {n: yaml.safe_load(Path(f".github/workflows/{n}.yml").read_text()) for n in ("ci", "deploy")}


def test_aucun_declencheur_dangereux():
    for w in W.values():   # PyYAML lit `on:` comme True
        assert not {"pull_request_target", "issue_comment", "workflow_run"} & set(w[True])


def test_le_runner_auto_heberge_ne_sert_qu_au_deploiement_de_main():
    auto = {n: j for w in W.values() for n, j in w["jobs"].items()
            if "self-hosted" in str(j.get("runs-on"))}
    assert set(auto) == {"deploy"} and auto["deploy"]["environment"] == "production"
    assert "refs/heads/main" in auto["deploy"]["if"] and "rag" in auto["deploy"]["runs-on"]


def test_actions_epinglees_par_sha():
    for f in Path(".github/workflows").glob("*.yml"):
        for u in re.findall(r"uses:\s*(\S+)", f.read_text()):
            assert u.startswith("./") or re.search(r"@[0-9a-f]{40}$", u), u


def test_la_ci_ne_lance_pas_les_tests_vivants():
    assert "not vivant and not lent" in Path(".github/workflows/ci.yml").read_text()


def test_un_seul_deploiement_a_la_fois():
    assert W["deploy"]["concurrency"] == {"group": "deploy-rag", "cancel-in-progress": False}


def test_l_image_est_arm64():
    image = W["deploy"]["jobs"]["image"]
    assert image["runs-on"] == "ubuntu-24.04-arm"
    pas = [p for p in image["steps"] if "build-push-action" in p.get("uses", "")]
    assert [p["with"]["platforms"] for p in pas] == ["linux/arm64"]


def test_les_correctifs_debian_sont_rejoues_chaque_jour():
    # Le cache gha figeait l'`apt-get upgrade` ; Trivy bloquait sur un correctif publié depuis.
    image = W["deploy"]["jobs"]["image"]
    pas_ = image["steps"]
    jour = [i for i, p in enumerate(pas_) if p.get("id") == "jour"]
    build = [i for i, p in enumerate(pas_) if "build-push-action" in p.get("uses", "")]
    assert jour and build and jour[0] < min(build)
    assert "date -u +%F" in pas_[jour[0]]["run"]
    pas = [pas_[i] for i in build]
    assert pas
    assert all("MAJ_DEBIAN=${{ steps.jour.outputs.date }}" in p["with"]["build-args"] for p in pas)
    lignes = Path("Dockerfile").read_text().splitlines()
    arg = next(i for i, l in enumerate(lignes) if l.startswith("ARG MAJ_DEBIAN"))
    upgrade = next(i for i, l in enumerate(lignes) if i > arg and "apt-get upgrade" in l)
    assert "MAJ_DEBIAN" in lignes[upgrade]
    deps = max(i for i, l in enumerate(lignes) if l.startswith("RUN uv sync"))
    user = next(i for i, l in enumerate(lignes) if l.startswith("USER "))
    assert deps < arg < upgrade < user
