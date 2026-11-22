"""Travailleur : passes d'indexation (5 min, nuit, « réindexer maintenant »), une à la fois."""

import logging
import time
from dataclasses import asdict
from datetime import datetime
from datetime import time as heure
from zoneinfo import ZoneInfo

from psycopg.types.json import Jsonb

from rag.db import connexion
from rag.graphe import maj_graphe
from rag.indexeur import indexer_source
from rag.semantique import passe_semantique
from rag.sources import lecteur_pour

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


def passe(conn, genre: str, config, reglages, modeles, sources: list[str] | None = None) -> dict:
    if not conn.execute("SELECT pg_try_advisory_lock(%s) AS ok", (VERROU,)).fetchone()["ok"]:
        conn.commit()
        return {"ignoree": "passe en cours"}
    bilan: dict = {}
    try:
        pid = conn.execute("INSERT INTO passes (genre) VALUES (%s) RETURNING id",
                           (genre,)).fetchone()["id"]
        conn.commit()
        for src in config.sources:
            if sources is not None and src.nom not in sources:
                continue
            try:
                lecteur = lecteur_pour(src, reglages)
                b = indexer_source(conn, src, lecteur, config.indexation, modeles,
                                   sans_repos=genre != "incrementale")
                bilan[src.nom] = asdict(b) | {"graphe": maj_graphe(conn, src, lecteur, reglages)}
            except Exception as e:
                conn.rollback()
                log.exception("%s : passe en échec", src.nom)
                bilan[src.nom] = {"erreur": f"{type(e).__name__}: {e}"[:500]}
        if genre == "nocturne":
            try:
                bilan["semantique"] = passe_semantique(conn, config, modeles)
            except Exception as e:  # noqa: BLE001
                conn.rollback()
                bilan["semantique"] = {"erreur": f"{type(e).__name__}: {e}"[:500]}
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
    demandes = [r["nom"] for r in conn.execute(
        "SELECT nom FROM sources WHERE demande_le IS NOT NULL ORDER BY nom")]
    derniere = conn.execute("SELECT max(debut) AS d FROM passes WHERE genre = 'nocturne'"
                            ).fetchone()["d"]
    t0 = conn.execute("SELECT now() AS t").fetchone()["t"]
    conn.commit()
    genre, sources = prochaine(maintenant or datetime.now(MTL), derniere, demandes)
    b = passe(conn, genre, config, reglages, modeles, sources)
    if "ignoree" not in b:  # une demande arrivée pendant la passe reste pour la suivante
        conn.execute("UPDATE sources SET demande_le = NULL WHERE demande_le <= %s", (t0,))
        conn.commit()
    return b


def boucle(reglages, config, modeles) -> None:  # pragma: no cover - boucle sans fin
    with connexion(reglages.database_url) as conn:
        log.info("%d passe(s) interrompue(s) fermée(s)", fermer_interrompues(conn))
    ecoute = connexion(reglages.database_url)
    ecoute.autocommit = True
    ecoute.execute("LISTEN rag_reindexer")
    while True:
        try:
            for _ in ecoute.notifies(timeout=ATTENTE_S, stop_after=1):
                pass
            with connexion(reglages.database_url) as conn:
                log.info("passe : %s", tour(conn, config, reglages, modeles))
        except Exception:
            log.exception("tour en échec ; reprise dans 30 s")
            time.sleep(30)
