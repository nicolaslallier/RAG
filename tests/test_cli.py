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


def test_cli_sources_exporter_lister_importer(env_cli, capsys, tmp_path):
    f = tmp_path / "sources.yaml"
    f.write_text("- {nom: depot, emplacement: https://github.com/a/b.git,"
                 " visibilite: public-maison}\n")
    assert cli.main(["sources", "importer", str(f)]) == 0
    assert "1 ajoutée(s), 0 modifiée(s), 0 inchangée(s)" in capsys.readouterr().err
    assert cli.main(["sources", "lister"]) == 0
    sortie = capsys.readouterr().out
    assert "depot" in sortie and "https://github.com/a/b.git" in sortie and "—" in sortie
    assert cli.main(["sources", "exporter"]) == 0
    assert yaml.safe_load(capsys.readouterr().out) == [
        {"nom": "depot", "emplacement": "https://github.com/a/b.git", "branche": "main",
         "visibilite": "public-maison", "exclure": [], "utilisateur": "x-access-token"}]


def test_cli_sources_importer_nom_reserve(env_cli, capsys, tmp_path):
    f = tmp_path / "sources.yaml"
    f.write_text("- {nom: coffre-maison, emplacement: https://github.com/a/b.git,"
                 " visibilite: prive}\n")
    assert cli.main(["sources", "importer", str(f)]) == 1
    assert "entrée 1" in capsys.readouterr().err


def test_cli_sources_importer_fichier_illisible(env_cli, capsys, tmp_path):
    assert cli.main(["sources", "importer", str(tmp_path / "absent.yaml")]) == 1
    assert "illisible" in capsys.readouterr().err


def test_migrer_role_monitoring_puis_amorcage(env_cli, tmp_path, monkeypatch, bd, capsys):
    """`rag migrer` : migrations (008 comprise), rôle monitoring, puis amorçage (noms réservés)."""
    import rag.db
    import rag.sources_gerees
    ordre = []
    # le vrai rôle est global au serveur Postgres partagé : seul l'ordre des appels est vérifié ici
    monkeypatch.setattr(rag.db, "assurer_role_monitoring",
                        lambda conn, mdp, **k: ordre.append(("monitoring", mdp)))
    amorcer = rag.sources_gerees.amorcer
    monkeypatch.setattr(rag.sources_gerees, "amorcer",
                        lambda *a, **k: ordre.append(("amorcer", k["noms_reserves"]))
                        or amorcer(*a, **k))
    f = tmp_path / "amorce.yaml"
    f.write_text('- {nom: depot, emplacement: "https://x/d.git", visibilite: prive}\n')
    monkeypatch.setenv("RAG_MONITORING_DB_PASSWORD", "Essai123")
    monkeypatch.setenv("RAG_SOURCES_AMORCE", str(f))
    assert cli.main(["migrer"]) == 0
    assert ordre == [("monitoring", "Essai123"), ("amorcer", {"coffre-maison"})]
    sortie = capsys.readouterr().out
    assert "rôle monitoring à jour" in sortie and "1 source(s) git amorcée(s)" in sortie
    assert "Essai123" not in sortie
    assert bd.execute("SELECT column_default FROM information_schema.columns WHERE"
                      " table_name = 'sources' AND column_name = 'visibilite'"
                      ).fetchone()["column_default"] is None
    assert bd.execute("SELECT visibilite FROM sources WHERE nom = 'depot'"
                      ).fetchone()["visibilite"] == "prive"


JETON = "sk-" + "a" * 24  # faux jeton construit : jamais de littéral réaliste dans le dépôt


def test_cli_quarantaine_lister_accepter_refuser_annuler(env_cli, capsys):
    (env_cli / "Cles.md").write_text(f"# Clés\n\nexemple : {JETON}\n")
    assert cli.main(["indexer"]) == 0
    capsys.readouterr()
    assert cli.main(["quarantaine", "lister"]) == 0
    out = capsys.readouterr().out
    assert ("a_traiter\tcoffre-maison\tCles.md\tsecret\t"
            "contenu : ligne 3 sk-aaa… (27 caractères)") in out
    assert JETON not in out
    assert cli.main(["quarantaine", "accepter", "coffre-maison", "Cles.md"]) == 0
    assert "accepté (contenu_secret)" in capsys.readouterr().err
    assert cli.main(["indexer"]) == 0
    capsys.readouterr()
    assert cli.main(["chercher", "exemple"]) == 0
    assert "Cles.md" in capsys.readouterr().out
    assert cli.main(["quarantaine", "refuser", "coffre-maison", "Cles.md"]) == 0
    capsys.readouterr()
    assert cli.main(["quarantaine", "lister", "--etat", "refuse"]) == 0
    assert "Cles.md" in capsys.readouterr().out
    assert cli.main(["quarantaine", "annuler", "coffre-maison", "Cles.md"]) == 0
    capsys.readouterr()
    assert cli.main(["quarantaine", "annuler", "coffre-maison", "Cles.md"]) == 1
    assert "aucune décision" in capsys.readouterr().err


def test_cli_quarantaine_accepter_refuse(env_cli, capsys):
    assert cli.main(["indexer"]) == 0
    capsys.readouterr()
    assert cli.main(["quarantaine", "accepter", "coffre-maison", "Accueil.md"]) == 1
    assert "refusé : ce fichier n'est pas en quarantaine" in capsys.readouterr().err
    assert cli.main(["quarantaine", "accepter", "inconnue", "x.md"]) == 1
    assert "source inconnue" in capsys.readouterr().err


def test_cli_quarantaine_exporter_importer(env_cli, capsys, tmp_path):
    assert cli.main(["indexer"]) == 0
    capsys.readouterr()
    f = tmp_path / "quarantaine.yaml"
    f.write_text("- {source: coffre-maison, chemin: x.md, decision: refuse, signature: s, "
                 "regles: []}\n")
    assert cli.main(["quarantaine", "importer", str(f)]) == 0
    assert "1 ajoutée(s), 0 ignorée(s)" in capsys.readouterr().err
    assert cli.main(["quarantaine", "exporter"]) == 0
    assert yaml.safe_load(capsys.readouterr().out) == [
        {"source": "coffre-maison", "chemin": "x.md", "decision": "refuse", "signature": "s",
         "regles": [], "contenu_sha256": None}]
    assert cli.main(["quarantaine", "importer", str(tmp_path / "absent.yaml")]) == 1
    assert "illisible" in capsys.readouterr().err
    f.write_text("pas une liste\n")
    assert cli.main(["quarantaine", "importer", str(f)]) == 1
    assert "fichier invalide, rien d'importé" in capsys.readouterr().err


def test_cli_quarantaine_lister_orphelins(env_cli, capsys):
    (env_cli / "Cles.md").write_text(f"# Clés\n\nexemple : {JETON}\n")
    assert cli.main(["indexer"]) == 0
    assert cli.main(["quarantaine", "refuser", "coffre-maison", "Cles.md"]) == 0
    capsys.readouterr()
    assert cli.main(["quarantaine", "lister", "--etat", "orphelin"]) == 0
    assert "Cles.md" not in capsys.readouterr().out
    (env_cli / "Cles.md").unlink()
    assert cli.main(["indexer"]) == 0
    capsys.readouterr()
    assert cli.main(["quarantaine", "lister", "--etat", "orphelin"]) == 0
    sortie = capsys.readouterr()
    assert "orphelin\tcoffre-maison\tCles.md\trefuse\tcontenu_secret" in sortie.out
    assert "1 décision(s) sans fichier en file" in sortie.err
