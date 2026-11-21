from pathlib import Path

import pytest

from rag.config import charger_config
from rag.decoupage import compter_tokens, decouper_note

CONF = charger_config(Path("config/rag.yaml")).indexation
LONG = "mot " * 400


def test_titres_fil_d_ariane():
    m = decouper_note("Recette", f"# A\n\n{LONG}\n\n## B\n\n{LONG}", CONF)
    assert [x.fil for x in m] == ["Recette > A", "Recette > A > B"]


def test_titres_fusionne_les_petites_sections():
    m = decouper_note("N", f"# A\n\ncourt\n\n# B\n\n{LONG}", CONF)
    assert len(m) == 1 and "court" in m[0].texte and LONG.strip()[:20] in m[0].texte


def test_titres_redecoupe_au_dessus_du_plafond():
    m = decouper_note("N", "# A\n\n" + "\n\n".join(["mot " * 150] * 8), CONF)
    assert len(m) >= 3 and all(compter_tokens(x.texte) <= CONF.plafond_tokens for x in m)
    assert {x.fil for x in m} == {"N > A"}


def test_bloc_de_code_jamais_coupe():
    code = "```python\n" + "x = 1\n" * 900 + "```"
    m = decouper_note("N", f"# A\n\n{code}", CONF)
    assert any(code in x.texte for x in m)


def test_diese_dans_un_bloc_de_code_n_est_pas_un_titre():
    assert len(decouper_note("N", f"# A\n\n```sh\n# commentaire\n```\n\n{LONG}", CONF)) == 1


def test_recursif_recouvre():
    texte = "\n\n".join(f"p{i} " + "mot " * 100 for i in range(12))
    m = decouper_note("N", texte, CONF.model_copy(update={"strategie": "recursif"}))
    assert len(m) >= 3 and all(x.fil == "N" for x in m)
    assert " ".join(m[0].texte.split()[-5:]) in m[1].texte


def test_parent_enfant():
    m = decouper_note("N", f"# A\n\n{LONG}\n\n{LONG}",
                      CONF.model_copy(update={"strategie": "parent_enfant"}))
    parents = [x for x in m if not x.cherchable]
    assert len(parents) == 1 and all(x.parent == parents[0].ordre for x in m if x.cherchable)
    assert all(compter_tokens(x.texte) <= 260 for x in m if x.cherchable)


def test_note_vide_ou_sans_titre():
    assert decouper_note("N", "", CONF) == []
    assert [x.fil for x in decouper_note("N", "juste du texte", CONF)] == ["N"]


@pytest.mark.parametrize("strategie", ["titres", "recursif", "parent_enfant"])
def test_ordres_uniques_et_croissants(strategie):
    corps = f"intro\n\n# A\n\n{LONG}\n\n## B\n\n{LONG * 3}\n\n# C\n\ncourt"
    m = decouper_note("N", corps, CONF.model_copy(update={"strategie": strategie}))
    assert [x.ordre for x in m] == list(range(len(m)))
