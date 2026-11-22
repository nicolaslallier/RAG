"""Jeu synthétique : un modèle local pose une question sur un passage, un autre la juge."""

import json
import re
from collections import Counter

from rag.banc.execution import Cas
from rag.config import PUBLIC, BancConf
from rag.decoupage import compter_tokens
from rag.modeles import ErreurModele
from rag.recherche import normaliser

MIN_TOKENS = 80
PAR_DOCUMENT = 2

CONSIGNE = (
    "Voici un passage d'une note personnelle. Écris UNE question en français à laquelle ce "
    "passage répond. La question doit être compréhensible sans voir le passage (nomme le sujet), "
    "ne pas reprendre plus de 3 mots consécutifs du passage, et finir par « ? ». "
    'Réponds uniquement en JSON : {"question": "…"}')
CONSIGNE_JUGE = (
    "Cette question est-elle claire seule, et ce passage y répond-il ? "
    'Réponds uniquement en JSON : {"ok": true} ou {"ok": false}')


def _mots(texte: str) -> list[str]:
    return re.findall(r"\w+", normaliser(texte))


def _recopie(question: str, passage: str, n: int = 4) -> bool:
    p = _mots(passage)
    suites = {tuple(p[i:i + n]) for i in range(len(p) - n + 1)}
    q = _mots(question)
    return any(tuple(q[i:i + n]) in suites for i in range(len(q) - n + 1))


def _json(texte: str) -> dict | None:
    try:
        d = json.loads(texte)
    except (TypeError, ValueError):
        return None
    return d if isinstance(d, dict) else None


def tirer(conn, version_id: int, graine: int, sources: list[str] | None = None):
    """Morceaux cherchables publics dans un ordre pseudo-aléatoire fixé par la graine,
    au plus PAR_DOCUMENT par document, d'au moins MIN_TOKENS tokens."""
    rows = conn.execute(
        """SELECT m.id, m.texte, s.nom || ':' || d.chemin AS cle
           FROM morceaux m JOIN documents d ON d.id = m.document_id
           JOIN sources s ON s.id = d.source_id
           WHERE m.version_id = %s AND m.embedding IS NOT NULL
             AND s.visibilite = ANY(%s) AND (%s::text[] IS NULL OR s.nom = ANY(%s))
           ORDER BY md5(m.id::text || %s)""",
        (version_id, sorted(PUBLIC), sources, sources, str(graine))).fetchall()
    par_doc = Counter()
    for r in rows:
        mid, texte, cle = r["id"], r["texte"], r["cle"]
        if compter_tokens(texte) < MIN_TOKENS or par_doc[cle] >= PAR_DOCUMENT:
            continue
        par_doc[cle] += 1
        yield mid, texte, cle


class _Panne:
    """Le modèle n'a pas répondu (délai, HTTP 5xx) : compté à part des JSON invalides."""


def _appel(modeles, modele, consigne, contenu, temperature) -> dict | None | type[_Panne]:
    try:
        return _json(modeles.generer(modele, [{"role": "system", "content": consigne},
                                              {"role": "user", "content": contenu}],
                                     temperature=temperature, json_mode=True))
    except ErreurModele:  # on rejette le cas sans perdre le lot
        return _Panne


def generer_jeu(conn, modeles, version_id: int, conf: BancConf, n: int = 250, graine: int = 1,
                sources: list[str] | None = None, rejets: Counter | None = None) -> list[Cas]:
    rejets = rejets if rejets is not None else Counter()
    gardes: list[Cas] = []
    vues: set[str] = set()
    essais = 0
    for mid, texte, cle in tirer(conn, version_id, graine, sources):
        if len(gardes) >= n or essais >= 3 * n:
            break
        essais += 1
        d = _appel(modeles, conf.modele_synthetique, CONSIGNE, texte, 0.7)
        if d is _Panne:
            rejets["modele"] += 1
            continue
        q = d.get("question") if d else None
        if not isinstance(q, str):
            rejets["json"] += 1
            continue
        q = q.strip()
        if len(q) < 20 or not q.endswith("?"):
            rejets["forme"] += 1
            continue
        if _recopie(q, texte):
            rejets["recopie"] += 1
            continue
        if normaliser(q) in vues:
            rejets["doublon"] += 1
            continue
        verdict = _appel(modeles, conf.modele_juge, CONSIGNE_JUGE,
                         f"Question : {q}\n\nPassage :\n{texte}", 0.0)
        if verdict is _Panne:
            rejets["modele"] += 1
            continue
        if not verdict or verdict.get("ok") is not True:
            rejets["juge"] += 1
            continue
        vues.add(normaliser(q))
        gardes.append(Cas(id=f"syn-{mid}", question=q, nature="factuel", attendus=[cle],
                          statut="valide", note=f"morceau {mid}"))
    return gardes


def commande_synthetique(a, conn, config, modeles) -> int:
    from dataclasses import asdict
    from pathlib import Path

    from rag.indexeur import version_courante
    rejets = Counter()
    cas = generer_jeu(conn, modeles, version_courante(conn, config.indexation), config.banc,
                      a.n, a.graine, a.source, rejets)
    sortie = Path("banc/synthetique.jsonl")
    sortie.write_text("".join(json.dumps(asdict(c), ensure_ascii=False) + "\n" for c in cas))
    print(f"gardés : {len(cas)} ; rejetés : {sum(rejets.values())} "
          f"({', '.join(f'{k} {v}' for k, v in rejets.most_common()) or 'aucun'})")
    print(f"jeu : {sortie}")
    return 0
