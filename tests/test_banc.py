import json
from pathlib import Path

import pytest

from rag import cli
from rag.banc.execution import Cas, charger_jeu, comparer, executer
from rag.banc.metriques import mrr, ndcg, rappel
from rag.config import charger_config
from tests.test_cli import env_cli  # noqa: F401 - fixture réutilisée

CONFIG = charger_config(Path("config/rag.yaml"))
CLE_POULET = "coffre-maison:02 - Domaines/Cuisine/Recettes/Poulet cidre et Boursin.md"


def test_metriques():
    assert rappel({"a", "b"}, ["x", "a", "y"], 5) == 0.5
    assert rappel({"a"}, ["x"] * 5 + ["a"], 5) == 0.0
    assert mrr({"a"}, ["x", "a"]) == 0.5 and mrr({"a"}, ["x"]) == 0.0
    assert ndcg({"a"}, ["a", "x"]) == 1.0 and 0 < ndcg({"a"}, ["x", "a"]) < 1
    assert ndcg({"a", "b"}, ["a", "b"]) == 1.0


def test_obtenus_dedoublonnes_par_document(bd, index, faux_modeles):
    long = "\n\n".join(f"# S{i}\n\n" + "poulet " * 200 for i in range(3))
    (index.coffre / "Gros.md").write_text(long)
    from rag.config import SourceConf
    from rag.indexeur import indexer_source
    from rag.sources import LecteurDossier
    indexer_source(bd, SourceConf(visibilite="public-maison", nom="coffre-maison", type="obsidian", emplacement="x"),
                   LecteurDossier(index.coffre), CONFIG.indexation, faux_modeles)
    r = executer(bd, [Cas(id="g", question="poulet", nature="factuel",
                          attendus=["coffre-maison:Gros.md"])], CONFIG, faux_modeles,
                 index.version)
    obt = r["cas"][0]["obtenus"]
    assert obt.count("coffre-maison:Gros.md") == 1 and len(obt) == len(set(obt))


def test_executer_et_resume(bd, index, faux_modeles):
    r = executer(bd, [Cas(id="p", question="recette de poulet", nature="lister",
                          attendus=[CLE_POULET]),
                      Cas(id="n", question="quel est le prix du bitcoin", nature="sans_reponse")],
                 CONFIG, faux_modeles, index.version)
    assert r["resume"]["n"] == 1 and r["resume"]["rappel5"] == 1.0
    assert r["cas"][1]["meilleur_cosinus"] is not None and "latence_p95_ms" in r["resume"]
    assert r["par_nature"]["sans_reponse"]["n"] == 1


def rapport(rappel10, cas):
    return {"resume": {"rappel10": rappel10},
            "cas": [{"id": k, "rappel10": v} for k, v in cas.items()]}


def test_comparer_signale_la_baisse():
    lignes = comparer(rapport(rappel10=0.6, cas={"p": 0.0}), rapport(rappel10=0.8, cas={"p": 1.0}))
    assert any("rappel10" in x and "-0.20" in x for x in lignes)
    assert any("p" in x for x in lignes[-2:])


def test_reference_refusee_si_cas_proposes(env_cli, tmp_path, monkeypatch):  # noqa: F811
    jeu = tmp_path / "jeu.yaml"
    jeu.write_text("- {id: p, question: poulet, nature: lister, attendus: ['coffre-maison:a.md']}\n")
    monkeypatch.chdir(tmp_path)
    assert cli.main(["banc", "--jeu", str(jeu), "--reference"]) == 2
    assert not (tmp_path / "banc/reference.json").exists()


def test_banc_cli_ecrit_le_rapport_et_la_reference(env_cli, tmp_path, monkeypatch, capsys):  # noqa: F811
    jeu = tmp_path / "jeu.yaml"
    jeu.write_text(f"- {{id: p, question: poulet, nature: lister, statut: valide, "
                   f"attendus: ['{CLE_POULET}']}}\n")
    monkeypatch.chdir(tmp_path)
    assert cli.main(["banc", "--jeu", str(jeu), "--reference", "--etiquette", "t"]) == 0
    ref = json.loads((tmp_path / "banc/reference.json").read_text())
    assert ref["resume"]["rappel10"] == 1.0 and ref["etiquette"] == "t"
    assert cli.main(["banc", "--jeu", str(jeu), "--sans-indexer"]) == 0
    assert "rappel10 en baisse : aucun" in capsys.readouterr().out


def test_jeu_invalide(tmp_path):
    f = tmp_path / "j.yaml"
    f.write_text("- {id: a, question: q, nature: factuel}\n- {id: a, question: q, nature: factuel}\n")
    with pytest.raises(ValueError):
        charger_jeu(f)
    f.write_text("- {id: a, question: q, nature: factuel, attendus: [sans-deux-points]}\n")
    with pytest.raises(ValueError):
        charger_jeu(f)


def test_exactitude_mode(bd, index, faux_modeles):
    from rag.banc.execution import Cas, executer
    from rag.config import charger_config
    conf = charger_config(Path("config/rag.yaml"))
    cas = [Cas(id="a", question="Quelles recettes ?", nature="lister", attendus=["x:y"],
               mode_attendu="lister"),
           Cas(id="b", question="Quelle est la recette ?", nature="factuel", attendus=["x:y"],
               mode_attendu="lister"),
           Cas(id="c", question="poulet", nature="factuel", attendus=["x:y"])]
    r = executer(bd, cas, conf, faux_modeles, index.version)
    assert r["resume"]["exactitude_mode"] == 0.5 and r["mal_aiguilles"] == ["b"]
