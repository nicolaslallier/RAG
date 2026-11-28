"""Connexion Postgres et migrations SQL numérotées."""

import logging
from pathlib import Path

import psycopg
from psycopg import sql
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


def assurer_role_monitoring(conn: psycopg.Connection, mot_de_passe: str,
                            role: str = "monitoring") -> None:
    """Rôle en lecture pour postgres-exporter (pg_monitor, pas superuser). Rejouable : aligne le mot de passe."""
    with conn.transaction():
        # rag et rag-indexeur migrent ensemble : même verrou que les migrations, relâché au COMMIT.
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (VERROU_MIGRATION,))
        existe = conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)).fetchone()
        # CREATE/ALTER ROLE ne prennent pas de paramètre lié : sql.Literal, jamais de f-string.
        conn.execute(sql.SQL("{} ROLE {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD {}").format(
            sql.SQL("ALTER" if existe else "CREATE"), sql.Identifier(role), sql.Literal(mot_de_passe)))
        conn.execute(sql.SQL("GRANT pg_monitor TO {}").format(sql.Identifier(role)))
        base = conn.execute("SELECT current_database() AS b").fetchone()["b"]
        conn.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            sql.Identifier(base), sql.Identifier(role)))
