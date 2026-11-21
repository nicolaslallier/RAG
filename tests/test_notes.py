from datetime import date

import pytest

from rag.notes import analyser_note

RECETTE = """---
type: recette
tags: [recette, cuisine, poulet]
source: claude
date: 2026-11-08
aliases: Poulet au cidre
---

# Poulet cidre et Boursin

Souper pour [[Lena Hope Capp]]. Retour sur [[Cuisine|la cuisine]]. %%note privée%%
#souper
"""


def test_frontmatter_devient_metadonnees():
    n = analyser_note("02 - Domaines/Cuisine/Recettes/Poulet cidre et Boursin.md", RECETTE)
    assert (n.titre, n.type, n.origine) == ("Poulet cidre et Boursin", "recette", "claude")
    assert n.tags == ["recette", "cuisine", "poulet", "souper"]
    assert n.alias == ["Poulet au cidre"] and n.date == date(2026, 11, 8)
    assert n.frontmatter["date"] == "2026-11-08"
    assert "type: recette" not in n.corps and "note privée" not in n.corps
    assert n.liens == ["Lena Hope Capp", "Cuisine"]


def test_note_sans_frontmatter():
    n = analyser_note("Accueil.md", "# Accueil\n\nBonjour")
    assert (n.titre, n.type, n.tags, n.frontmatter) == ("Accueil", None, [], {})


@pytest.mark.parametrize("fm", ["---\ntags: [a, b\n---\n", "---\n: : :\n---\n",
                                "---\njuste du texte\n---\n", "---\nsans fin"])
def test_frontmatter_invalide_n_arrete_rien(fm):
    n = analyser_note("x.md", fm + "corps")
    assert n.titre == "x" and n.frontmatter == {} and "corps" in n.corps


def test_commentaire_sur_plusieurs_lignes_retire():
    assert "secret" not in analyser_note("x.md", "a\n%%\nsecret\n%%\nb").corps


def test_lien_dans_un_bloc_de_code_ignore():
    assert analyser_note("x.md", "```\n[[pas un lien]]\n```\n[[vrai]]").liens == ["vrai"]


def test_lien_avec_section():
    assert analyser_note("x.md", "[[Note#Partie]] [[Note]]").liens == ["Note"]


@pytest.mark.parametrize("fm", [
    "---\na: &x [1, 2]\nb: *x\n---\n",            # alias YAML refusé
    "---\na: .nan\n---\n",                         # NaN non sérialisable en JSON strict
    "---\n? [1, 2]\n: v\n---\n",                   # clé non hachable / exotique
    "---\na: " + "x" * 70000 + "\n---\n",          # frontmatter trop gros
])
def test_frontmatter_dangereux_ignore(fm):
    n = analyser_note("x.md", fm + "corps")
    assert n.frontmatter == {} and "corps" in n.corps


def test_commentaire_non_ferme_masque_jusqu_a_la_fin():
    assert "secret" not in analyser_note("x.md", "a\n%%\nsecret privé").corps


def test_tags_numeriques_et_liens_internes_ignores():
    n = analyser_note("x.md", "Voir #12 et PR #186, [[#Se connecter]] et #vrai-tag #2026a")
    assert n.tags == ["vrai-tag", "2026a"]
