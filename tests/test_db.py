import psycopg
import pytest

from rag.db import migrer
from tests.conftest import inscrire_source


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
