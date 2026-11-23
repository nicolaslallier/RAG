"""Travailleur : passes d'indexation (5 min, nuit, « réindexer maintenant »), une à la fois."""

import logging
import shutil
import time
from dataclasses import asdict
from datetime import datetime
from datetime import time as heure
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg.errors
from psycopg.types.json import Jsonb

from rag.db import connexion
from rag.graphe import maj_graphe
from rag.indexeur import SourceSupprimee, indexer_source
from rag.semantique import passe_semantique
from rag.sources import lecteur_pour
from rag.sources_gerees import coffret_depuis, sources_actives

log = logging.getLogger("rag.travailleur")
VERROU = 7241
MTL = ZoneInfo("America/Montreal")
NUIT = heure(3, 0)
ATTENTE_S = 300


def prochaine(maintenant: datetime, derniere_nocturne: datetime | None,
              demandes: list[str]) -> tuple[str, list[str] | None]:
    if demandes:
        return "demandee", demandes
    m = maintenant.astimezone(MTL)
    deja = derniere_nocturne is not None and derniere_nocturne.astimezone(MTL).date() == m.date()
    if m.time() >= NUIT and not deja:
        return "nocturne", None
    return "incrementale", None


def menage_dossiers(donnees_dir: Path, noms: set[str]) -> list[str]:
    """Efface les clones et graphes des sources disparues ; jamais hors de clones/ et graphe/."""
    effaces: set[str] = set()
    for sous in ("clones", "graphe"):
        base = Path(donnees_dir) / sous
        if not base.is_dir():
            continue
        for d in sorted(base.iterdir()):
            if d.is_dir() and not d.is_symlink() and d.name not in noms:
                shutil.rmtree(d, ignore_errors=True)
                effaces.add(d.name)
    return sorted(effaces)


def passe(conn, genre: str, config, reglages, modeles, sources: list[str] | None = None,
          sans_garde: frozenset[str] = frozenset()) -> dict:
    if not conn.execute("SELECT pg_try_advisory_lock(%s) AS ok", (VERROU,)).fetchone()["ok"]:
        conn.commit()
        return {"ignoree": "passe en cours"}
    bilan: dict = {}
    try:
        fermer_interrompues(conn)  # verrou tenu : une passe sans fin est forcément orpheline
        actives = sources_actives(conn, config, coffret_depuis(reglages))
        conn.commit()
        for nom in menage_dossiers(Path(reglages.donnees_dir), {a.conf.nom for a in actives}):
            log.info("dossier de source disparue effacé : %s", nom)
        pid = conn.execute("INSERT INTO passes (genre) VALUES (%s) RETURNING id",
                           (genre,)).fetchone()["id"]
        conn.commit()
        for active in actives:
            src = active.conf
            if sources is not None and src.nom not in sources:
                continue
            try:
                lecteur = lecteur_pour(src, reglages, active.jeton, active.utilisateur)
                b = indexer_source(conn, src, lecteur, config.indexation, modeles,
                                   forcer=src.nom in sans_garde,
                                   sans_repos=genre != "incrementale")
                graphe = maj_graphe(conn, src, lecteur, reglages)
                if "ÉCHEC" in graphe:  # le message d'erreur reste au journal (maj_graphe)
                    bilan[src.nom] = asdict(b) | {"erreur": "echec"}
                else:
                    bilan[src.nom] = asdict(b) | {"graphe": graphe}
            except SourceSupprimee:
                conn.rollback()
                bilan[src.nom] = {"supprimee": True}
            except psycopg.errors.ForeignKeyViolation:
                conn.rollback()  # source supprimée en cours de passe : ses documents n'ont plus de parent
                bilan[src.nom] = {"supprimee": True}
            except Exception:
                conn.rollback()
                log.exception("%s : passe en échec", src.nom)
                bilan[src.nom] = {"erreur": "echec"}  # détail dans le journal seulement
        if genre == "nocturne":
            try:
                bilan["semantique"] = passe_semantique(conn, config, modeles)
            except Exception:
                conn.rollback()
                log.exception("passe sémantique en échec")
                bilan["semantique"] = {"erreur": "echec"}
        conn.execute("ANALYZE morceaux, documents, noeuds, liens")
        conn.commit()
        return bilan
    finally:
        conn.rollback()
        ok = not any(isinstance(v, dict) and "erreur" in v for v in bilan.values())
        if "pid" in locals():
            conn.execute("UPDATE passes SET fin = now(), ok = %s, bilan = %s WHERE id = %s",
                         (ok, Jsonb(bilan), pid))
        conn.execute("SELECT pg_advisory_unlock(%s)", (VERROU,))
        conn.commit()


def fermer_interrompues(conn) -> int:
    n = conn.execute("""UPDATE passes SET fin = now(), ok = false,
                               bilan = bilan || '{"erreur": "interrompue"}'
                         WHERE fin IS NULL""").rowcount
    conn.commit()
    return n


def tour(conn, config, reglages, modeles, maintenant: datetime | None = None) -> dict:
    lignes = conn.execute(
        "SELECT nom, sans_garde FROM sources WHERE demande_le IS NOT NULL ORDER BY nom").fetchall()
    demandes = [r["nom"] for r in lignes]
    sans_garde = frozenset(r["nom"] for r in lignes if r["sans_garde"])
    derniere = conn.execute("SELECT max(debut) AS d FROM passes WHERE genre = 'nocturne'"
                            ).fetchone()["d"]
    t0 = conn.execute("SELECT now() AS t").fetchone()["t"]
    conn.commit()
    genre, sources = prochaine(maintenant or datetime.now(MTL), derniere, demandes)
    b = passe(conn, genre, config, reglages, modeles, sources, sans_garde)
    if "ignoree" not in b:  # une demande arrivée pendant la passe reste pour la suivante
        echecs = [n for n, v in b.items() if isinstance(v, dict) and "erreur" in v]
        conn.execute("UPDATE sources SET demande_le = NULL, "
                     "sans_garde = sans_garde AND nom = ANY(%s) "
                     "WHERE nom = ANY(%s) AND demande_le <= %s", (echecs, demandes, t0))
        conn.commit()
    return b


def _ecouter(url: str):
    ecoute = connexion(url)
    ecoute.autocommit = True
    ecoute.execute("LISTEN rag_reindexer")
    return ecoute


def boucle(reglages, config, modeles, tours: int | None = None, pause_s: float = 30) -> None:
    """Sans fin (tours=None). Toute erreur, y compris une connexion LISTEN morte : journal,
    pause, puis connexion d'écoute rouverte."""
    with connexion(reglages.database_url) as conn:
        log.info("%d passe(s) interrompue(s) fermée(s)", fermer_interrompues(conn))
    ecoute = None
    n = 0
    while tours is None or n < tours:
        n += 1
        try:
            if ecoute is None or ecoute.closed:
                ecoute = _ecouter(reglages.database_url)
            for _ in ecoute.notifies(timeout=ATTENTE_S, stop_after=1):
                pass
            for _ in ecoute.notifies(timeout=0):  # vider la file : un tour sert tout
                pass
            with connexion(reglages.database_url) as conn:
                log.info("passe : %s", tour(conn, config, reglages, modeles))
        except Exception:
            log.exception("tour en échec ; reprise dans %s s", pause_s)
            if ecoute is not None:
                ecoute.close()
            ecoute = None
            time.sleep(pause_s)
