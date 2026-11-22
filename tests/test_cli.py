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
    d["sources"] = [{"nom": "coffre-maison", "type": "obsidian", "visibilite": "public-maison", "emplacement": str(coffre)}]
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


def test_cli_question_liste(env_cli, capsys):
    assert cli.main(["indexer"]) == 0
    capsys.readouterr()
    assert cli.main(["question", "Quelles recettes j'ai pour le poulet, avec les sources ?"]) == 0
    out = capsys.readouterr().out
    assert "mode : lister (par regles)" in out and "type=recette" in out
    assert "Poulet cidre et Boursin" in out and "obsidian://" in out and "origine : claude" in out


def test_cli_exclusions_exporter_puis_importer(env_cli, capsys, tmp_path):
    assert cli.main(["indexer"]) == 0
    capsys.readouterr()
    f = tmp_path / "exclusions.yaml"
    f.write_text("- {source: coffre-maison, motif: '*.md', raison: tout}\n")
    assert cli.main(["exclusions", "importer", str(f)]) == 0
    assert "1 ajoutée(s), 0 ignorée(s), 4 document(s) retiré(s)" in capsys.readouterr().err
    assert cli.main(["exclusions", "exporter"]) == 0
    assert yaml.safe_load(capsys.readouterr().out) == [
        {"source": "coffre-maison", "motif": "*.md", "raison": "tout"}]


def test_cli_exclusions_importer_malforme(env_cli, capsys, tmp_path):
    f = tmp_path / "exclusions.yaml"
    f.write_text("pas une liste\n")
    assert cli.main(["exclusions", "importer", str(f)]) == 1
    assert "invalide" in capsys.readouterr().err


def test_cli_exclusions_importer_fichier_illisible(env_cli, capsys, tmp_path):
    assert cli.main(["exclusions", "importer", str(tmp_path / "absent.yaml")]) == 1
    err = capsys.readouterr().err
    assert "illisible" in err and "Traceback" not in err
    f = tmp_path / "latin1.yaml"
    f.write_bytes(b"- {source: coffre-maison, motif: 'caf\xe9.md'}\n")
    assert cli.main(["exclusions", "importer", str(f)]) == 1
    assert "UTF-8" in capsys.readouterr().err
    assert cli.main(["exclusions", "exporter"]) == 0
    assert yaml.safe_load(capsys.readouterr().out) == []
