from pathlib import Path

import pytest

from rag.config import charger_config
from rag.recherche import Resultat
from rag.refus import REFUS, calibrer, doit_refuser

BASE = charger_config(Path("config/rag.yaml")).reponse


def CONF(**k):
    return BASE.model_copy(update={"seuil_refus_rerank": None, "seuil_refus_cosinus": None} | k)


def R(cos, rr=None, via="hybride", score=9.9):
    return Resultat(morceau_id=1, document_id=1, cle="s:c", source="s", type_source="obsidian",
                    chemin="c", titre="t", fil="f", texte="x", symbole=None, ligne=None,
                    origine=None, lien="l", score=score, cosinus=cos, rerank=rr, via=via)


def test_texte_exact():
    assert REFUS == "Je n'ai pas trouvé cela dans vos notes."


def test_refus_sur_le_rerank_quand_il_est_actif():
    c = CONF(seuil_refus_rerank=0.3, seuil_refus_cosinus=0.9)
    assert doit_refuser([R(0.95, 0.1)], c, rerank_actif=True) is True  # le cosinus ne sauve pas
    assert doit_refuser([R(0.10, 0.5)], c, rerank_actif=True) is False


def test_repli_sur_le_cosinus_sans_rerank():
    c = CONF(seuil_refus_rerank=0.3, seuil_refus_cosinus=0.5)
    assert doit_refuser([R(0.4)], c, rerank_actif=False) is True
    assert doit_refuser([R(0.6)], c, rerank_actif=False) is False


def test_jamais_sur_le_score_rrf():
    assert doit_refuser([R(0.6, score=0.0001)], CONF(seuil_refus_cosinus=0.5), False) is False


def test_seuil_non_calibre_ne_refuse_pas_mais_vide_refuse():
    assert doit_refuser([R(0.01)], CONF(), False) is False and doit_refuser([], CONF(), False)


def test_un_passage_venu_du_graphe_ne_sauve_pas():
    assert doit_refuser([R(0.9, via="graphe"), R(0.2)], CONF(seuil_refus_cosinus=0.5), False)


def test_seulement_le_graphe_refuse():
    assert doit_refuser([R(0.9, via="graphe")], CONF(), False) is True


def test_calibrer():
    rap = {"cas": [{"nature": "factuel", "meilleur_cosinus": c} for c in (0.8, 0.7, 0.65)]
           + [{"nature": "sans_reponse", "meilleur_cosinus": c} for c in [0.3] * 9 + [0.66]]}
    k = calibrer(rap, "meilleur_cosinus")
    assert 0.3 < k["seuil"] <= 0.65 and k["faux_refus"] == 0 and k["refus_justes"] == 0.9
    assert (k["n_avec"], k["n_sans"]) == (3, 10)


def test_calibrer_refuse_un_petit_jeu():
    rap = {"cas": [{"nature": "sans_reponse", "meilleur_cosinus": 0.1}] * 3}
    with pytest.raises(ValueError):
        calibrer(rap, "meilleur_cosinus")


def test_resume_du_banc_compte_les_refus():
    from rag.banc.execution import taux_refus
    cas = [{"nature": "sans_reponse", "meilleur_cosinus": 0.2},
           {"nature": "sans_reponse", "meilleur_cosinus": 0.8},
           {"nature": "factuel", "meilleur_cosinus": 0.4},
           {"nature": "factuel", "meilleur_cosinus": 0.9}]
    assert taux_refus(cas, CONF(seuil_refus_cosinus=0.5), False) == {
        "refus_justes": 0.5, "faux_refus": 0.5}
