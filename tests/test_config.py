from pathlib import Path

import pydantic
import pytest
import yaml

from rag.config import charger_config, empreinte_indexation


def test_la_config_de_prod_se_charge():
    c = charger_config(Path("config/rag.yaml"))
    assert c.indexation.strategie == "titres" and c.indexation.dimensions == 1024
    assert c.recherche.rrf_k == 60 and c.recherche.rerank.actif is False
    assert c.sources[0].nom == "coffre-maison" and c.sources[0].repos_minutes == 10
    assert c.sources[0].visibilite == "public-maison" and "_versions/**" in c.sources[0].exclure


def test_la_config_de_dev_se_charge():
    c = charger_config(Path("config/dev.yaml"))
    assert c.sources[0].emplacement == "~/Obsidian/Maison"


def test_empreinte_change_avec_la_strategie():
    c = charger_config(Path("config/rag.yaml")).indexation
    assert empreinte_indexation(c) == empreinte_indexation(c.model_copy())
    autre = c.model_copy(update={"strategie": "recursif"})
    assert empreinte_indexation(c) != empreinte_indexation(autre)


def test_une_cle_inconnue_est_refusee(tmp_path):
    d = yaml.safe_load(Path("config/rag.yaml").read_text())
    d["recherche"]["rrf_kk"] = 60
    f = tmp_path / "c.yaml"
    f.write_text(yaml.safe_dump(d))
    with pytest.raises(pydantic.ValidationError):
        charger_config(f)
