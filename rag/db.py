"""Connexion Postgres et migrations SQL numérotées."""

from pathlib import Path

import psycopg
from psycopg.rows import dict_row

MIGRATIONS = Path(__file__).parent / "migrations"


def connexion(url: str) -> psycopg.Connection:
    return psycopg.connect(url, row_factory=dict_row)


def migrer(conn: psycopg.Connection) -> list[str]:
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
