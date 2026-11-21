from pathlib import Path

import pytest
import yaml

from rag import cli
from tests.conftest import NOTES_TEST, URL


@pytest.fixture
def env_cli(bd, tmp_path, monkeypatch, faux_modeles):
    coffre = tmp_path / "coffre"
    for chemin, texte in list(NOTES_TEST.items())[:4]:
        (coffre / chemin).parent.mkdir(parents=True, exist_ok=True)
        (coffre / chemin).write_text(texte)
    d = yaml.safe_load(Path("config/rag.yaml").read_text())
    d["sources"] = [{"nom": "coffre-maison", "type": "obsidian", "emplacement": str(coffre)}]
    f = tmp_path / "rag.yaml"
    f.write_text(yaml.safe_dump(d))
    monkeypatch.setenv("RAG_CONFIG", str(f))
    monkeypatch.setenv("RAG_DATABASE_URL", URL)
    monkeypatch.setenv("RAG_LITELLM_URL", "http://x")
    monkeypatch.setattr(cli, "modeles_depuis", lambda r: faux_modeles)
    return coffre


def test_cli_indexer_puis_chercher(env_cli, capsys):
    assert cli.main(["indexer"]) == 0
    assert "4 nouveaux" in capsys.readouterr().out
    assert cli.main(["chercher", "poulet"]) == 0
    premiere = capsys.readouterr().out.splitlines()[0]
    assert "Poulet cidre et Boursin.md" in premiere


def test_cli_coffre_partiel_sort_en_2(env_cli, capsys):
    assert cli.main(["indexer"]) == 0
    for f in list(env_cli.rglob("*.md"))[:3]:
        f.unlink()
    assert cli.main(["indexer"]) == 2
    assert cli.main(["indexer", "--forcer"]) == 0
