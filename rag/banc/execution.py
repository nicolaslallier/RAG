"""Banc de mesure : charger un jeu, l'exécuter, comparer à une référence."""

import json
import shutil
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import yaml

from rag.banc.metriques import mrr, ndcg, rappel
from rag.config import PUBLIC, Config, empreinte_indexation
from rag.indexeur import version_courante
from rag.recherche import chercher

NATURES = {"lister", "synthetiser", "factuel", "sans_reponse"}
METRIQUES = ["rappel5", "rappel10", "mrr", "ndcg10"]


@dataclass
class Cas:
    id: str
    question: str
    nature: str
    attendus: list[str] = field(default_factory=list)
    mode_attendu: str | None = None
    statut: str = "propose"
    note: str = ""


def charger_jeu(chemin: Path) -> list[Cas]:
    texte = Path(chemin).read_text(encoding="utf-8")
    if Path(chemin).suffix == ".jsonl":
        brut = [json.loads(ligne) for ligne in texte.splitlines() if ligne.strip()]
    else:
        brut = yaml.safe_load(texte) or []
    cas, vus = [], set()
    for d in brut:
        c = Cas(**d)
        if c.id in vus:
            raise ValueError(f"id en double : {c.id}")
        if c.nature not in NATURES:
            raise ValueError(f"{c.id} : nature inconnue {c.nature}")
        if any(":" not in a for a in c.attendus):
            raise ValueError(f"{c.id} : attendu sans « source: »")
        vus.add(c.id)
        cas.append(c)
    return cas


def _centile(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return xs[round(p * (len(xs) - 1))] if xs else 0.0


def _moyennes(lignes: list[dict]) -> dict:
    return {m: round(statistics.fmean(c[m] for c in lignes), 4) if lignes else 0.0
            for m in METRIQUES}


def executer(conn, cas: list[Cas], config: Config, modeles, version_id: int,
             etiquette: str = "", reranker=None) -> dict:
    lignes = []
    for c in cas:
        t0 = time.perf_counter()
        r = chercher(conn, c.question, config.recherche, modeles, version_id,
                     config.indexation.modele_embedding, niveaux=PUBLIC, limite=10, reranker=reranker)
        latence = (time.perf_counter() - t0) * 1000
        obtenus = list(dict.fromkeys(x.cle for x in r))
        a = set(c.attendus)
        lignes.append({
            "id": c.id, "nature": c.nature, "obtenus": obtenus,
            "rappel5": rappel(a, obtenus, 5), "rappel10": rappel(a, obtenus, 10),
            "mrr": mrr(a, obtenus), "ndcg10": ndcg(a, obtenus, 10),
            "latence_ms": round(latence, 1),
            "meilleur_cosinus": max((x.cosinus for x in r if x.cosinus is not None),
                                    default=None),
            "meilleur_rerank": max((x.rerank for x in r if x.rerank is not None), default=None),
        })
    avec = [x for x in lignes if x["nature"] != "sans_reponse"]
    latences = [x["latence_ms"] for x in lignes]
    par_nature = {}
    for n in sorted({x["nature"] for x in lignes}):
        sous = [x for x in lignes if x["nature"] == n]
        par_nature[n] = {"n": len(sous)} | (
            {"cosinus_moyen": round(statistics.fmean(
                x["meilleur_cosinus"] or 0 for x in sous), 4)}
            if n == "sans_reponse" else _moyennes(sous))
    return {
        "etiquette": etiquette,
        "empreinte_indexation": empreinte_indexation(config.indexation),
        "config": config.recherche.model_dump(),
        "cas": lignes,
        "resume": _moyennes(avec) | {
            "latence_p50_ms": _centile(latences, 0.5), "latence_p95_ms": _centile(latences, 0.95),
            "n": len(avec)},
        "par_nature": par_nature,
    }


def comparer(rapport: dict, reference: dict) -> list[str]:
    lignes = []
    for nom, ref in reference["resume"].items():
        nouv = rapport["resume"].get(nom, 0)
        lignes.append(f"{nom:<16} {ref:.2f} → {nouv:.2f}  ({nouv - ref:+.2f})")
    avant = {c["id"]: c["rappel10"] for c in reference["cas"]}
    baisses = [c["id"] for c in rapport["cas"] if c["rappel10"] < avant.get(c["id"], 0)]
    lignes.append("rappel10 en baisse : " + (", ".join(baisses) or "aucun"))
    return lignes


def ajouter_cli_banc(sp) -> None:
    pb = sp.add_parser("banc")
    pb.add_argument("--jeu", default="banc/etalon.yaml")
    pb.add_argument("--etiquette", default="essai")
    pb.add_argument("--sans-indexer", action="store_true")
    pb.add_argument("--reference", action="store_true")


def commande_banc(a, conn, reglages, config: Config, modeles, indexer) -> int:
    cas = charger_jeu(Path(a.jeu))
    proposes = [c.id for c in cas if c.statut != "valide"]
    if a.reference and proposes:
        print(f"refus : {len(proposes)} cas encore à statut « propose » "
              f"({', '.join(proposes[:5])}…)", file=sys.stderr)
        return 2
    if not a.sans_indexer and (code := indexer(conn, reglages, config, modeles)):
        return code
    from rag.rerank import reranker_depuis
    rapport = executer(conn, cas, config, modeles, version_courante(conn, config.indexation),
                       a.etiquette, reranker_depuis(config, reglages))
    dossier = Path("banc/resultats")
    dossier.mkdir(parents=True, exist_ok=True)
    sortie = dossier / f"{datetime.now():%Y%m%d-%H%M%S}-{a.etiquette}.json"
    sortie.write_text(json.dumps(rapport, ensure_ascii=False, indent=1))
    print(json.dumps(rapport["resume"], ensure_ascii=False))
    ref = Path("banc/reference.json")
    if ref.exists():
        print("\n".join(comparer(rapport, json.loads(ref.read_text()))))
    if a.reference:
        shutil.copy(sortie, ref)
        print(f"référence posée : {ref}")
    print(f"rapport : {sortie}")
    return 0

