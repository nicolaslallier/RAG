"""Refus avant tout appel au LLM : sur le score du reranker, sinon sur le cosinus. Jamais sur RRF."""

from typing import Literal

from rag.config import ReponseConf
from rag.recherche import Resultat

REFUS = "Je n'ai pas trouvé cela dans vos notes."


def _seuil(conf: ReponseConf, rerank_actif: bool) -> float | None:
    return conf.seuil_refus_rerank if rerank_actif else conf.seuil_refus_cosinus


def refuse_score(meilleur: float | None, seuil: float | None) -> bool:
    if meilleur is None:
        return True
    return seuil is not None and meilleur < seuil


def doit_refuser(resultats: list[Resultat], conf: ReponseConf, rerank_actif: bool) -> bool:
    directs = [r for r in resultats if r.via != "graphe"]
    scores = [s for r in directs if (s := (r.rerank if rerank_actif else r.cosinus)) is not None]
    return refuse_score(max(scores, default=None), _seuil(conf, rerank_actif))


def calibrer(rapport: dict, champ: Literal["meilleur_rerank", "meilleur_cosinus"]) -> dict:
    """Seuil qui maximise le F1 moyen des deux classes (sans_reponse refusé, autre rendu).

    Le F1 de la seule classe « sans_reponse » pousse le seuil vers le haut dès qu'un cas sans
    réponse ressemble à un vrai : la moyenne des deux F1 pèse aussi les faux refus.
    À égalité, le seuil le plus bas."""
    cas = [(c["nature"] == "sans_reponse", c.get(champ) or 0.0) for c in rapport["cas"]]
    n_sans = sum(s for s, _ in cas)
    n_avec = len(cas) - n_sans
    if n_sans < 10:
        raise ValueError(f"{n_sans} cas sans_reponse : il en faut au moins 10")
    meilleur = None
    for t in sorted({0.0} | {v for _, v in cas}):
        tp = sum(s and v < t for s, v in cas)
        fp = sum(not s and v < t for s, v in cas)
        fn, tn = n_sans - tp, n_avec - fp
        f1 = (2 * tp / (2 * tp + fp + fn) + 2 * tn / (2 * tn + fn + fp)) / 2
        if meilleur is None or f1 > meilleur["f1"] + 1e-12:
            meilleur = {"seuil": round(t, 4), "f1": round(f1, 4),
                        "refus_justes": round(tp / n_sans, 4),
                        "faux_refus": round(fp / n_avec, 4) if n_avec else 0.0,
                        "n_avec": n_avec, "n_sans": n_sans}
    return meilleur
