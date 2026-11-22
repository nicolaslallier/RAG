"""Aiguillage d'une question vers lister ou répondre : règles d'abord, gemma en repli."""

import json
import re
from dataclasses import dataclass, field
from typing import Literal

from rag.modeles import ErreurModele
from rag.recherche import Filtres, normaliser

_REPONDRE = ["quel est", "quelle est", "comment", "pourquoi", "explique", "resume", "decris",
             "qu'est-ce que", "c'est quoi", "que sais-tu"]
# « combien » retiré (banc 2026-11-22) : « combien coûte / de temps / de VM » attend un fait.
_LISTER = ["quels", "quelles", "liste", "lister", "enumere", "montre-moi tous",
           "montre-moi toutes", "donne-moi la liste", "tous les", "toutes les"]
_SOURCES_DITES = re.compile(r"\bavec (?:les|la|leurs) sources?\b")


def _commence(q: str, debuts: list[str]) -> bool:
    return any(re.match(re.escape(d) + r"\b", q) for d in debuts)


@dataclass
class Decision:
    mode: Literal["lister", "repondre"]
    filtres: Filtres = field(default_factory=Filtres)
    par: Literal["appelant", "regles", "modele", "defaut"] = "defaut"
    bonus_sources: list[str] | None = None   # mode répondre : source citée = bonus, pas filtre


# Avant les règles « répondre » : « c'est quoi mes menus », « qu'est-ce que j'ai sur ma liste »,
# et un « quels/quelles » en milieu de phrase (« J'ai quels agents… », « Dans quelles veilles… »).
_LISTER_D_ABORD = re.compile(r"^(?:c'est quoi mes|qu'est-ce que j'ai)\b|.\b(?:quels|quelles)\b")


def _mode_par_regles(q: str) -> str | None:
    if _LISTER_D_ABORD.search(q):
        return "lister"
    if _commence(q, _REPONDRE):
        return "repondre"
    if _commence(q, _LISTER) or "la liste de" in q:
        return "lister"
    return None


def _mode_par_modele(question: str, modeles, modele: str) -> str | None:
    messages = [
        {"role": "system", "content":
            "Classe la question. « lister » : l'utilisateur veut une liste ou un inventaire de "
            "notes ou de fichiers. « repondre » : il veut une explication ou un fait. "
            'Réponds uniquement en JSON : {"mode": "lister"} ou {"mode": "repondre"}.'},
        {"role": "user", "content": question}]
    try:
        mode = json.loads(modeles.generer(modele, messages, temperature=0, json_mode=True))["mode"]
    except (ErreurModele, ValueError, KeyError, TypeError):
        return None
    return mode if mode in ("lister", "repondre") else None


def sources_citees(question: str, taxo: dict) -> list[str] | None:
    mots = re.findall(r"[\w-]+", question)
    bas = {m.lower() for m in mots}
    # Nom court (AI) : casse exacte, sinon « j'ai » deviendrait un filtre ; sinon sans casse.
    trouves = [s for s in taxo.get("sources", [])
               if (s in mots if len(s) <= 3 else s.lower() in bas)]
    return trouves or None


def _filtres(question: str, taxo: dict, mode: str) -> Filtres:
    f = Filtres(sources=sources_citees(question, taxo))
    if mode != "lister":
        return Filtres()
    q = _SOURCES_DITES.sub(" ", normaliser(question).replace("’", "'"))
    types = {normaliser(t): t for t in taxo.get("types", {})}
    tags = {normaliser(t): t for t in taxo.get("tags", {})}
    trouves: list[str] = []
    for mot in re.findall(r"\w+", q):
        if len(mot) < 3:
            continue
        formes = [mot] + ([mot[:-1]] if mot[-1] in "sx" else [])
        if f.type is None and (t := next((types[x] for x in formes if x in types), None)):
            f.type = t
            continue
        if (t := next((tags[x] for x in formes if x in tags), None)) and t not in trouves:
            trouves.append(t)
    f.tags = [t for t in trouves if t != f.type] or None
    return f


def aiguiller(question: str, taxo: dict, modeles, modele: str, mode_force: str | None = None,
              filtres_forces: Filtres | None = None) -> Decision:
    q = normaliser(question).replace("’", "'").strip()
    if mode_force:
        mode, par = mode_force, "appelant"
    elif mode := _mode_par_regles(q):
        par = "regles"
    elif mode := _mode_par_modele(question, modeles, modele):
        par = "modele"
    else:
        mode, par = "repondre", "defaut"
    f = filtres_forces if filtres_forces is not None else _filtres(question, taxo, mode)
    bonus = sources_citees(question, taxo) if mode == "repondre" else None
    return Decision(mode=mode, filtres=f, par=par, bonus_sources=bonus)
