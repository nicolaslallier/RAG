import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from rag.db import assurer_role_monitoring, connexion, migrer
from tests.conftest import URL, inscrire_source


def test_migrer_est_idempotent(bd):
    assert migrer(bd) == []


def test_recherche_plein_texte_sans_accents(bd):
    r = bd.execute(
        "SELECT to_tsvector('fr','Pâté chinois') @@ plainto_tsquery('fr','pate') AS ok"
    ).fetchone()
    assert r["ok"] is True


def test_embedding_est_un_halfvec_1024(bd):
    r = bd.execute(
        "SELECT format_type(atttypid, atttypmod) t FROM pg_attribute "
        "WHERE attrelid='morceaux'::regclass AND attname='embedding'"
    ).fetchone()
    assert r["t"] == "halfvec(1024)"


def test_visibilite_travail_acceptee_inconnue_refusee(bd):
    inscrire_source(bd, "t", visibilite="travail")
    with pytest.raises(psycopg.errors.CheckViolation):
        bd.execute("UPDATE sources SET visibilite = 'pro'")
    bd.rollback()


def test_visibilite_sans_defaut(bd):
    with pytest.raises(psycopg.errors.NotNullViolation):
        bd.execute("INSERT INTO sources (nom, type, emplacement) VALUES ('x','code','x')")
    bd.rollback()


def test_role_monitoring_rejouable_et_sans_superuser(bd):
    # Rôle d'essai : la fonction valide sa transaction, `monitoring` survivrait dans la base de dev.
    try:
        assurer_role_monitoring(bd, "Essai123", role="monitoring_essai")
        assurer_role_monitoring(bd, "Autre456", role="monitoring_essai")   # rejouable, aligne le mot de passe
        r = bd.execute("SELECT rolsuper, rolcanlogin, rolcreaterole, rolcreatedb, "
                       "pg_has_role(rolname, 'pg_monitor', 'member') AS moniteur "
                       "FROM pg_roles WHERE rolname = 'monitoring_essai'").fetchone()
        assert r == {"rolsuper": False, "rolcanlogin": True, "rolcreaterole": False,
                     "rolcreatedb": False, "moniteur": True}
        bd.commit()
        info = conninfo_to_dict(URL)
        url = make_conninfo(URL, user="monitoring_essai", password="Autre456")
        with connexion(url) as m:   # le dernier mot de passe ouvre une session qui lit les stats
            assert m.execute("SELECT count(*) AS n FROM pg_stat_database WHERE datname = %s",
                             (info["dbname"],)).fetchone()["n"] == 1
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                m.execute("SELECT 1 FROM sources LIMIT 1")
    finally:
        bd.rollback()
        bd.execute("DROP OWNED BY monitoring_essai")   # retire le GRANT CONNECT
        bd.execute("DROP ROLE monitoring_essai")
        bd.commit()


def test_quarantaine_decisions_contraintes(bd):
    ins = ("INSERT INTO quarantaine_decisions (source, chemin, decision, signature, regles) "
           "VALUES ('s', %s, %s, 'sig', %s)")
    bd.execute(ins, ("a", "accepte", ["contenu_secret"]))
    bd.execute(ins, ("b", "refuse", []))
    bd.commit()
    for chemin, decision, regles in (("c", "accepte", []), ("d", "peut-etre", ["genere"]),
                                     ("e", "accepte", ["binaire"]), ("a", "refuse", [])):
        with pytest.raises((psycopg.errors.CheckViolation, psycopg.errors.UniqueViolation)):
            bd.execute(ins, (chemin, decision, regles))
        bd.rollback()
