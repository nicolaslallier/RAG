import pytest

from rag.aiguillage import aiguiller
from rag.modeles import ErreurModele
from rag.recherche import Filtres, normaliser

TAXO = {"types": {"recette": 3, "projet": 14, "ressource": 24},
        "tags": {"poulet": 2, "souper": 4, "source": 1, "cuisine": 5},
        "sources": ["coffre-maison", "AI", "Infra"]}


class FauxGenerateur:
    def __init__(self, reponses):
        self.reponses, self.appels = list(reponses), []

    def generer(self, modele, messages, temperature, json_mode=False):
        self.appels.append((modele, temperature, json_mode))
        if not self.reponses:
            raise AssertionError("le modèle ne devait pas être appelé")
        return self.reponses.pop(0)


class FauxGenerateurQuiTombe:
    def generer(self, *a, **k):
        raise ErreurModele("panne")


JAMAIS = FauxGenerateur([])


def test_les_trois_questions_de_nicolas():
    d = aiguiller("Quelles recettes j'ai pour le poulet, avec les sources ?", TAXO, JAMAIS, "g")
    assert (d.mode, d.par, d.filtres.type, d.filtres.tags) == ("lister", "regles", "recette",
                                                               ["poulet"])
    assert d.filtres.sources is None  # « j'ai » n'est pas la source AI
    d = aiguiller("Quelle est l'architecture de mon projet AI ?", TAXO, JAMAIS, "g")
    assert d.mode == "repondre" and d.filtres.type is None and d.filtres.tags is None
    d = aiguiller("Quelles sont mes dashboards Grafana ?", TAXO, JAMAIS, "g")
    assert d.mode == "lister" and d.filtres.type is None and not d.filtres.tags


def test_avec_les_sources_n_est_pas_le_tag_source():
    d = aiguiller("Liste mes recettes avec les sources", TAXO, JAMAIS, "g")
    assert "source" not in (d.filtres.tags or []) and d.filtres.type == "recette"


def test_pluriel_et_accents():
    assert aiguiller("Quels sont mes projets ?", TAXO, JAMAIS, "g").filtres.type == "projet"
    assert aiguiller("Énumère mes ressources", TAXO, JAMAIS, "g").filtres.type == "ressource"


def test_source_citee():
    d = aiguiller("Liste les fichiers Dockerfile dans Infra", TAXO, JAMAIS, "g")
    assert d.filtres.sources == ["Infra"]
    assert aiguiller("Liste les fichiers dans infra", TAXO, JAMAIS, "g").filtres.sources == ["Infra"]


def test_quelle_est_n_est_pas_une_liste():
    d = aiguiller("Quelle est la recette du poulet ?", TAXO, JAMAIS, "g")
    assert (d.mode, d.filtres.type, d.filtres.tags) == ("repondre", None, None)


def test_apostrophe_typographique():
    assert aiguiller("Qu’est-ce que Glee ?", TAXO, JAMAIS, "g").mode == "repondre"


def test_repli_sur_le_modele_puis_defaut():
    g = FauxGenerateur(['{"mode": "lister"}'])
    d = aiguiller("Les soupers de la semaine", TAXO, g, "g")
    assert (d.mode, d.par) == ("lister", "modele") and g.appels == [("g", 0, True)]
    assert d.filtres.tags == ["souper"]
    d = aiguiller("Les soupers de la semaine", TAXO, FauxGenerateur(["n'importe quoi"]), "g")
    assert (d.mode, d.par) == ("repondre", "defaut")
    d = aiguiller("Les soupers", TAXO, FauxGenerateur(['{"mode": "autre"}']), "g")
    assert (d.mode, d.par) == ("repondre", "defaut")
    assert aiguiller("Les soupers", TAXO, FauxGenerateurQuiTombe(), "g").par == "defaut"


def test_l_appelant_force():
    d = aiguiller("Quelle est…", TAXO, JAMAIS, "g", mode_force="lister",
                  filtres_forces=Filtres(type="projet"))
    assert (d.mode, d.par, d.filtres.type) == ("lister", "appelant", "projet")
    d = aiguiller("Quelles recettes", TAXO, JAMAIS, "g", filtres_forces=Filtres())
    assert (d.mode, d.par, d.filtres.type) == ("lister", "regles", None)


@pytest.mark.parametrize("q", ["", "?", "quels", "liste"])
def test_questions_degenerees(q):
    d = aiguiller(q, TAXO, FauxGenerateur(['{"mode": "repondre"}']), "g")
    assert d.mode in ("lister", "repondre")


@pytest.mark.parametrize("q,mode", [
    ("C'est quoi mes menus de la semaine ?", "lister"),
    ("Qu'est-ce que j'ai sur ma liste Costco ?", "lister"),
    ("Dans quelles veilles on parle de Mistral ?", "lister"),
    ("J'ai quels agents A2A derrière LiteLLM ?", "lister"),
    ("Combien coûte ma facture d'Hydro-Québec ?", None),
    ("C'est quoi Glee ?", "repondre"),
])
def test_regles_corrigees_au_banc(q, mode):
    from rag.aiguillage import _mode_par_regles
    assert _mode_par_regles(normaliser(q)) == mode


def test_en_mode_repondre_la_source_est_un_bonus_pas_un_filtre():
    d = aiguiller("Quelle est l'architecture de mon projet AI ?", TAXO, JAMAIS, "g")
    assert d.filtres.sources is None and d.bonus_sources == ["AI"]
    d = aiguiller("Liste les fichiers Dockerfile dans Infra", TAXO, JAMAIS, "g")
    assert d.filtres.sources == ["Infra"] and d.bonus_sources is None
