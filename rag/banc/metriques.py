"""Métriques de retrieval au niveau du document (pertinence binaire)."""

import math


def rappel(attendus: set[str], obtenus: list[str], k: int) -> float:
    return len(attendus & set(obtenus[:k])) / len(attendus) if attendus else 0.0


def mrr(attendus: set[str], obtenus: list[str]) -> float:
    return next((1 / i for i, o in enumerate(obtenus, 1) if o in attendus), 0.0)


def ndcg(attendus: set[str], obtenus: list[str], k: int = 10) -> float:
    dcg = sum(1 / math.log2(i + 2) for i, o in enumerate(obtenus[:k]) if o in attendus)
    idcg = sum(1 / math.log2(i + 2) for i in range(min(len(attendus), k)))
    return dcg / idcg if idcg else 0.0
