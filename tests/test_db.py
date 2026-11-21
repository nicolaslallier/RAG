from rag.db import migrer


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
