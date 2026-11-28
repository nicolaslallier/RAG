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


def test_bloc_de_code_sous_la_borne_reste_entier():
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


def _empreinte_sous_les_bornes() -> str:
    import hashlib
    import json

    from rag.decoupage_code import decouper_code
    corpus = [f"# A\n\n{LONG}\n\n## B\n\n{LONG}",
              "# A\n\n" + "\n\n".join(["mot " * 150] * 8),
              "# A\n\n```python\n" + "x = 1\n" * 900 + "```",
              f"intro\n\n# A\n\n{LONG}\n\n## B\n\n{LONG * 3}\n\n# C\n\ncourt",
              "\n\n".join(f"p{i} " + "mot " * 100 for i in range(12)),
              "# T\n\n" + "Une phrase. " * 700 + "\n\n```sh\nls\n```\n\nfin " * 30]
    vus = []
    for strategie in ("titres", "recursif", "parent_enfant"):
        conf = CONF.model_copy(update={"strategie": strategie})
        vus += [[(m.ordre, m.fil, m.texte, m.parent, m.cherchable)
                 for m in decouper_note("N", t, conf)] for t in corpus]
    for chemin, src in (("f.py", "def f():\n" + "    a = 1\n" * 2000),
                        ("c.yaml", "cle: valeur\n" * 800)):
        vus.append([(m.ordre, m.fil, m.texte, m.symbole, m.ligne)
                    for m in decouper_code("R", chemin, src, CONF)])
    return hashlib.sha256(json.dumps(vus, ensure_ascii=False).encode()).hexdigest()


def test_sous_les_bornes_le_decoupage_ne_change_pas():
    # Empreinte prise sur le découpage d'avant la borne dure : tant qu'elle tient, les morceaux
    # déjà indexés restent valides et VERSION_ANALYSEUR n'a pas à bouger.
    assert _empreinte_sous_les_bornes() == (
        "e7d6c6d62fb4029282022ec173df27e3a5005aad9b49796f5d0398950eb4a345")


BORNE = 2 * CONF.plafond_tokens   # tokens : au-delà, même un bloc de code est coupé


@pytest.mark.parametrize("strategie", ["titres", "recursif", "parent_enfant"])
@pytest.mark.parametrize("corps", [
    "# A\n\n" + "debut " * 50 + "x" * 20000 + " fin",                    # mot géant
    "intro\n\n" + "A" * 7000,                                            # hors section
    "# A\n\n```python\n" + "x = 1\n" * 3000 + "```\n\nsuite",            # bloc > 2 × plafond
    "# A\n\n```json\n" + '{"a":' + "1," * 9000 + "}\n```",               # ligne géante en bloc
])
def test_borne_dure_sur_les_morceaux_cherchables(strategie, corps):
    m = decouper_note("N", corps, CONF.model_copy(update={"strategie": strategie}))
    cherchables = [x for x in m if x.cherchable]
    assert cherchables and all(compter_tokens(x.texte) <= BORNE + 50 for x in cherchables)
    assert [x.ordre for x in m] == list(range(len(m)))


def test_mot_geant_coupe_sans_perte():
    m = decouper_note("N", "# A\n\n" + "x" * 20000, CONF)
    assert "".join(x.texte for x in m) == "x" * 20000
    assert all(compter_tokens(x.texte) <= CONF.cible_tokens for x in m)


def test_gros_bloc_de_code_coupe_sur_les_lignes():
    code = "```python\n" + "x = 1\n" * 3000 + "```"
    m = decouper_note("N", f"# A\n\n{code}", CONF)
    assert len(m) > 1 and all(compter_tokens(x.texte) <= CONF.cible_tokens for x in m)
    assert all(ligne in ("x = 1", "```python", "```")
               for x in m for ligne in x.texte.split("\n"))
    assert sum(x.texte.count("x = 1") for x in m) == 3000


def test_code_source_a_ligne_geante_borne():
    from rag.decoupage_code import decouper_code
    m = decouper_code("R", "gros.json", '{"x": "' + "a" * 30000 + '"}', CONF)
    assert len(m) > 1 and all(compter_tokens(x.texte) <= BORNE + 50 for x in m)
