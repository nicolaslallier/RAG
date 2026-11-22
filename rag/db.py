"""Connexion Postgres et migrations SQL numérotées."""

import logging
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

MIGRATIONS = Path(__file__).parent / "migrations"


def connexion(url: str) -> psycopg.Connection:
    return psycopg.connect(url, row_factory=dict_row)


VERROU_MIGRATION = 7240


def migrer(conn: psycopg.Connection) -> list[str]:
    # Deux conteneurs démarrent ensemble : un seul migre, l'autre attend puis ne trouve rien.
    conn.execute("SELECT pg_advisory_lock(%s)", (VERROU_MIGRATION,))
    try:
        return _migrer(conn)
    finally:
        # Ne pas masquer l'erreur d'origine : la session qui meurt relâche le verrou de toute façon.
        try:
            conn.rollback()
            conn.execute("SELECT pg_advisory_unlock(%s)", (VERROU_MIGRATION,))
            conn.commit()
        except Exception as e:  # noqa: BLE001
            logging.getLogger(__name__).warning("verrou de migration non relâché : %s",
                                                type(e).__name__)


def _migrer(conn: psycopg.Connection) -> list[str]:
    with conn.transaction():
        conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (nom text PRIMARY KEY)")
    faites = {r["nom"] for r in conn.execute("SELECT nom FROM schema_migrations")}
    conn.commit()
    appliquees = []
    for f in sorted(MIGRATIONS.glob("*.sql")):
        if f.name in faites:
            continue
        with conn.transaction():
            conn.execute(f.read_text(encoding="utf-8"))
            conn.execute("INSERT INTO schema_migrations (nom) VALUES (%s)", (f.name,))
        appliquees.append(f.name)
    return appliquees
