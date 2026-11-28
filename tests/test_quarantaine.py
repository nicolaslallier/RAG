from pathlib import Path

import pytest

from rag.config import PUBLIC, TOUT, SourceConf, charger_config
from rag.db import connexion
from rag.gestion import SourceInconnue, exclure
from rag.indexeur import indexer_source
from rag.quarantaine import (
    DecisionInvalide,
    accepter,
    annuler,
    comptes,
    decrire,
    file,
    levees,
    refuser,
)
from rag.sources import LecteurGit
from tests.conftest import URL, commit, git, inscrire_source
from tests.test_gestion import bloque_par_le_verrou

CONF = charger_config(Path("config/rag.yaml")).indexation
SRC = SourceConf(visibilite="public-maison", nom="depot-test", type="code", emplacement="x")
JETON = "sk-" + "a" * 24  # faux jeton construit : jamais de littéral réaliste dans le dépôt
PLAN = "docs/plan.md"


def passe(bd, depot, tmp_path, faux_modeles):
    return indexer_source(bd, SRC, LecteurGit(str(depot), tmp_path / "clones" / "c"), CONF,
                          faux_modeles)


@pytest.fixture
def q(bd, depot, tmp_path, faux_modeles):
    """Dépôt de test indexé, avec un faux secret dans docs/plan.md ; rend une fonction de passe."""
    inscrire_source(bd, "depot-test")
    commit(depot, PLAN, f"# Plan\n\nexemple : {JETON}\n")
    passe(bd, depot, tmp_path, faux_modeles)
    return lambda: passe(bd, depot, tmp_path, faux_modeles)


def chemins(bd):
    return {r["chemin"] for r in bd.execute("SELECT chemin FROM documents")}


def exclu(bd, chemin):
    return bd.execute("SELECT * FROM exclus WHERE chemin = %s", (chemin,)).fetchone()


def decision(bd, chemin):
    return bd.execute("SELECT * FROM quarantaine_decisions WHERE chemin = %s", (chemin,)).fetchone()


def etats(bd, **k):
    return {f["chemin"]: f["etat"] for e in ("a_traiter", "accepte", "refuse")
            for f in file(bd, niveaux=TOUT, etat=e, par_page=1000, **k)["fichiers"]}


def ligne(bd, chemin, etat="a_traiter"):
    return next(f for f in file(bd, niveaux=TOUT, etat=etat, raison=None, par_page=1000)["fichiers"]
                if f["chemin"] == chemin)


def source_en_base(bd):
    return bd.execute("SELECT * FROM sources WHERE nom = 'depot-test'").fetchone()


# --- file et acceptation ---

def test_file_liste_secrets_et_generes_a_traiter(bd, q):
    r = file(bd, niveaux=PUBLIC)
    assert r["total"] == 3 and r["pages"] == 1 and r["page"] == 1
    assert [f["chemin"] for f in r["fichiers"]] == [".env.example", "docs/plan.md", "uv.lock"]
    f = next(f for f in r["fichiers"] if f["chemin"] == PLAN)
    assert f["source"] == "depot-test" and f["raison"] == "secret" and f["etat"] == "a_traiter"
    assert f["indexe"] is False and f["modifie"] is False and f["regles_levees"] == []
    assert f["detail"]["regle"] == "contenu_secret"
    assert f["signature"] == exclu(bd, PLAN)["signature"]


def test_file_filtre_raison_source_et_pagine(bd, q):
    assert [f["chemin"] for f in file(bd, niveaux=TOUT, raison="genere")["fichiers"]] == ["uv.lock"]
    assert file(bd, niveaux=TOUT, raison="secret")["total"] == 2
    assert file(bd, niveaux=TOUT, source="autre")["total"] == 0
    r = file(bd, niveaux=TOUT, par_page=1, page=2)
    assert len(r["fichiers"]) == 1 and r["pages"] == 3


def test_file_cache_une_source_privee_au_jeton_public(bd, q):
    bd.execute("UPDATE sources SET visibilite = 'prive'")
    bd.commit()
    assert file(bd, niveaux=PUBLIC)["total"] == 0
    assert comptes(bd, niveaux=PUBLIC) == {"secret": 0, "genere": 0}
    assert file(bd, niveaux=TOUT)["total"] == 3


def test_comptes_par_raison(bd, q):
    assert comptes(bd, niveaux=TOUT) == {"secret": 2, "genere": 1}


def test_decrire():
    assert decrire({"regle": "nom_secret", "motif": ".env.*"}) == "nom de fichier : .env.*"
    assert decrire({"regle": "genere", "motif": "dist/"}) == "fichier généré : dist/"
    assert decrire({"regle": "contenu_secret", "tronque": True, "correspondances": [
        {"ligne": 3, "extrait": "sk-aaa… (27 caractères)"}]}) == \
        "contenu : ligne 3 sk-aaa… (27 caractères) ; et d'autres"
    assert decrire(None) == "—"


def test_accepter_enregistre_signature_et_regle_et_demande_la_passe(bd, q):
    bd.execute("UPDATE sources SET graphe_revision = 'abc'")
    bd.commit()
    signature = exclu(bd, PLAN)["signature"]
    r = accepter(bd, "depot-test", PLAN, signature)
    assert r == {"regle": "contenu_secret", "regles": ["contenu_secret"]}
    d = decision(bd, PLAN)
    assert d["decision"] == "accepte" and d["signature"] == signature
    assert d["regles"] == ["contenu_secret"]
    s = source_en_base(bd)
    assert s["demande_le"] is not None and s["graphe_revision"] is None
    assert etats(bd)[PLAN] == "accepte"
    lv = levees(bd, "depot-test")
    assert lv.keys() == {PLAN} and lv[PLAN].signature == signature
    assert lv[PLAN].regles == {"contenu_secret"}
    assert lv[PLAN].contenu_sha256 == exclu(bd, PLAN)["contenu_sha256"] is not None
    assert d["contenu_sha256"] == exclu(bd, PLAN)["contenu_sha256"]


def test_accepter_sans_signature_prend_la_courante(bd, q):
    assert accepter(bd, "depot-test", ".env.example")["regles"] == ["nom_secret"]


def test_accepter_refuse_hors_quarantaine(bd, q):
    for chemin in ("logo.png", "a.py", "absent.md"):
        with pytest.raises(DecisionInvalide):
            accepter(bd, "depot-test", chemin)
    with pytest.raises(SourceInconnue):
        accepter(bd, "inconnue", PLAN)
    assert bd.execute("SELECT count(*) n FROM quarantaine_decisions").fetchone()["n"] == 0
    assert source_en_base(bd)["demande_le"] is None


def test_accepter_refuse_une_signature_perimee(bd, q):
    with pytest.raises(DecisionInvalide):
        accepter(bd, "depot-test", PLAN, "autre")
    assert decision(bd, PLAN) is None
    assert source_en_base(bd)["demande_le"] is None


def test_accepter_refuse_sans_detail(bd, q):
    bd.execute("UPDATE exclus SET detail = NULL WHERE chemin = 'docs/plan.md'")
    bd.commit()
    with pytest.raises(DecisionInvalide):
        accepter(bd, "depot-test", PLAN)
    assert decision(bd, PLAN) is None


def test_accepter_cumule_les_regles_a_signature_egale(bd, q):
    accepter(bd, "depot-test", ".env.example")
    bd.execute("""UPDATE exclus SET detail = '{"regle": "contenu_secret", "tronque": false,
                  "correspondances": []}', lu = true, contenu_sha256 = 'x'
                  WHERE chemin = '.env.example'""")
    bd.commit()
    assert etats(bd)[".env.example"] == "a_traiter"
    assert ligne(bd, ".env.example")["regles_levees"] == ["nom_secret"]
    assert accepter(bd, "depot-test", ".env.example")["regles"] == ["contenu_secret", "nom_secret"]


def test_accepter_remplace_une_decision_d_une_autre_signature(bd, q):
    bd.execute("INSERT INTO quarantaine_decisions (source, chemin, decision, signature, regles) "
               "VALUES ('depot-test', %s, 'accepte', 'vieille', '{genere}')", (PLAN,))
    bd.commit()
    f = ligne(bd, PLAN)
    assert f["etat"] == "a_traiter" and f["modifie"] is True
    assert accepter(bd, "depot-test", PLAN)["regles"] == ["contenu_secret"]
    assert decision(bd, PLAN)["signature"] == exclu(bd, PLAN)["signature"]


def test_levees_ignore_les_refus(bd, q):
    bd.execute("INSERT INTO quarantaine_decisions (source, chemin, decision, signature, regles) "
               "VALUES ('depot-test', %s, 'refuse', 'sig', '{contenu_secret}')", (PLAN,))
    bd.commit()
    assert levees(bd, "depot-test") == {}


# --- l'indexeur applique les acceptations ---

def test_fichier_accepte_est_indexe_a_la_passe_suivante(bd, q):
    accepter(bd, "depot-test", PLAN)
    b = q()
    assert PLAN in chemins(bd) and exclu(bd, PLAN) is None and b.nouveaux == 1
    assert bd.execute("SELECT count(*) n FROM morceaux m JOIN documents d ON d.id = m.document_id "
                      "WHERE d.chemin = %s AND m.texte LIKE '%%exemple%%'", (PLAN,)
                      ).fetchone()["n"] >= 1
    assert ligne(bd, PLAN, "accepte")["indexe"] is True


def test_fichier_accepte_inchange_n_est_pas_relu(bd, q, depot, tmp_path, faux_modeles):
    accepter(bd, "depot-test", PLAN)
    q()
    lus = []
    lec = LecteurGit(str(depot), tmp_path / "clones" / "c")
    lire = lec.lire
    lec.lire = lambda c: lus.append(c) or lire(c)
    indexer_source(bd, SRC, lec, CONF, faux_modeles)
    assert PLAN not in lus and PLAN in chemins(bd)


def test_nom_secret_accepte_est_lu_et_indexe(bd, q):
    accepter(bd, "depot-test", ".env.example")
    q()
    assert ".env.example" in chemins(bd) and exclu(bd, ".env.example") is None


def test_fichier_accepte_puis_modifie_retourne_en_quarantaine(bd, q, depot):
    accepter(bd, "depot-test", PLAN)
    q()
    commit(depot, PLAN, f"# Plan\n\nautre : {JETON}b\n")
    q()
    assert PLAN not in chemins(bd)
    assert exclu(bd, PLAN)["raison"] == "secret"
    f = ligne(bd, PLAN)
    assert f["etat"] == "a_traiter" and f["modifie"] is True
    assert bd.execute("SELECT count(*) n FROM morceaux WHERE texte LIKE '%%sk-aaa%%'"
                      ).fetchone()["n"] == 0


def test_nom_accepte_au_contenu_secret_reste_exclu(bd, q, depot):
    commit(depot, ".env.local", f"K={JETON}\n")
    q()
    accepter(bd, "depot-test", ".env.local")
    q()
    assert ".env.local" not in chemins(bd)
    e = exclu(bd, ".env.local")
    assert e["lu"] is True and e["detail"]["regle"] == "contenu_secret"
    assert etats(bd)[".env.local"] == "a_traiter"
    assert accepter(bd, "depot-test", ".env.local")["regles"] == ["contenu_secret", "nom_secret"]
    q()
    assert ".env.local" in chemins(bd)


def test_genere_accepte_mais_binaire_reste_exclu(bd, q, depot):
    (depot / "dist").mkdir()
    (depot / "dist/app.bin").write_bytes(b"\x00\x01binaire")
    git(depot, "add", "-A")
    git(depot, "commit", "-qm", "binaire")
    q()
    assert exclu(bd, "dist/app.bin")["raison"] == "genere"
    accepter(bd, "depot-test", "dist/app.bin")
    q()
    assert "dist/app.bin" not in chemins(bd)
    assert exclu(bd, "dist/app.bin")["raison"] == "binaire"
    assert "dist/app.bin" not in {f["chemin"] for f in file(bd, niveaux=TOUT)["fichiers"]}


def test_exclusion_manuelle_prime_sur_l_acceptation(bd, q):
    accepter(bd, "depot-test", PLAN)
    q()
    assert PLAN in chemins(bd)
    exclure(bd, "depot-test", PLAN)
    q()
    assert PLAN not in chemins(bd) and exclu(bd, PLAN)["raison"] == "manuelle"


def test_acceptation_retiree_pendant_les_embeddings_n_est_pas_ecrite(bd, q, faux_modeles):
    accepter(bd, "depot-test", PLAN)
    embed, fait = faux_modeles.embed_documents, []

    def embed_et_retirer(textes, modele):
        if not fait and any("exemple" in t for t in textes):
            fait.append(1)
            with connexion(URL) as autre:
                autre.execute("DELETE FROM quarantaine_decisions")
                autre.commit()
        return embed(textes, modele)

    faux_modeles.embed_documents = embed_et_retirer
    b = q()
    assert fait and PLAN not in chemins(bd) and exclu(bd, PLAN) is not None
    assert b.reportes == 1


def test_secret_exclu_et_accepte_ailleurs_ne_fuit_pas(bd, q, depot):
    commit(depot, "docs/autre.md", f"x : {JETON}\n")
    q()
    accepter(bd, "depot-test", PLAN)
    q()
    assert "docs/autre.md" not in chemins(bd)


# --- refuser, annuler ---

def nb_decisions(bd):
    return bd.execute("SELECT count(*) n FROM quarantaine_decisions").fetchone()["n"]


def test_refuser_sort_de_la_file_et_reste_exclu(bd, q):
    refuser(bd, "depot-test", PLAN)
    assert etats(bd)[PLAN] == "refuse"
    d = decision(bd, PLAN)
    assert d["decision"] == "refuse" and d["regles"] == ["contenu_secret"]
    assert source_en_base(bd)["demande_le"] is None
    q()
    assert PLAN not in chemins(bd) and etats(bd)[PLAN] == "refuse"


def test_refus_tient_quand_le_fichier_change(bd, q, depot):
    refuser(bd, "depot-test", PLAN)
    commit(depot, PLAN, f"autre : {JETON}c\n")
    q()
    assert etats(bd)[PLAN] == "refuse"


def test_refuser_hors_quarantaine(bd, q):
    for chemin in ("logo.png", "a.py", "absent.md"):
        with pytest.raises(DecisionInvalide):
            refuser(bd, "depot-test", chemin)
    with pytest.raises(SourceInconnue):
        refuser(bd, "inconnue", PLAN)
    assert nb_decisions(bd) == 0


def test_refuser_un_fichier_accepte_et_indexe_le_retire(bd, q):
    accepter(bd, "depot-test", PLAN)
    q()
    doc = bd.execute("SELECT id, source_id FROM documents WHERE chemin = %s", (PLAN,)).fetchone()
    bd.execute("INSERT INTO noeuds (source_id, cle, libelle, libelle_norm, type, document_id) "
               "VALUES (%s, %s, 'plan', 'plan', 'fichier', %s)",
               (doc["source_id"], PLAN, doc["id"]))
    bd.execute("UPDATE sources SET graphe_revision = 'abc'")
    bd.commit()
    refuser(bd, "depot-test", PLAN)
    assert PLAN not in chemins(bd)
    assert bd.execute("SELECT 1 FROM noeuds WHERE cle = %s", (PLAN,)).fetchone() is None
    assert source_en_base(bd)["graphe_revision"] is None
    e = exclu(bd, PLAN)
    assert e["raison"] == "secret" and e["empreinte"] == "" and e["detail"] is None
    assert etats(bd)[PLAN] == "refuse"
    q()
    assert exclu(bd, PLAN)["detail"]["regle"] == "contenu_secret" and PLAN not in chemins(bd)


def test_annuler_une_acceptation_indexee_retire_le_document(bd, q):
    accepter(bd, "depot-test", PLAN)
    q()
    bd.execute("UPDATE sources SET demande_le = NULL")
    bd.commit()
    assert annuler(bd, "depot-test", PLAN) is True
    assert PLAN not in chemins(bd) and decision(bd, PLAN) is None
    assert source_en_base(bd)["demande_le"] is not None
    q()
    assert etats(bd)[PLAN] == "a_traiter" and ligne(bd, PLAN)["detail"] is not None


def test_annuler_une_acceptation_en_attente(bd, q):
    accepter(bd, "depot-test", PLAN)
    assert annuler(bd, "depot-test", PLAN) is True
    q()
    assert PLAN not in chemins(bd)


def test_annuler_un_refus_remet_en_file(bd, q):
    refuser(bd, "depot-test", PLAN)
    assert annuler(bd, "depot-test", PLAN) is True
    assert etats(bd)[PLAN] == "a_traiter"
    assert source_en_base(bd)["demande_le"] is None


def test_annuler_sans_decision(bd, q):
    assert annuler(bd, "depot-test", PLAN) is False


def test_annuler_ne_retire_pas_un_document_redevenu_propre(bd, q, depot):
    accepter(bd, "depot-test", PLAN)
    q()
    commit(depot, PLAN, "# Plan\n\nsans jeton\n")
    q()
    assert annuler(bd, "depot-test", PLAN) is True
    assert PLAN in chemins(bd)


def test_ecritures_de_quarantaine_prennent_le_verrou_de_la_source(bd, q):
    assert bloque_par_le_verrou("depot-test", accepter, "depot-test", PLAN)
    assert bloque_par_le_verrou("depot-test", refuser, "depot-test", PLAN)
    assert bloque_par_le_verrou("depot-test", annuler, "depot-test", PLAN)


def test_contenu_change_a_signature_egale_reste_exclu(bd, q, depot, tmp_path, faux_modeles):
    """TOCTOU listage -> lecture, `touch -r` : l'acceptation de contenu_secret vaut pour un
    contenu (sha256), pas seulement pour une signature."""
    accepter(bd, "depot-test", PLAN)
    lec = LecteurGit(str(depot), tmp_path / "clones" / "c")
    lire = lec.lire
    autre = f"# Plan\n\nvrai : {JETON}z\n".encode()
    lec.lire = lambda c: autre if c == PLAN else lire(c)
    indexer_source(bd, SRC, lec, CONF, faux_modeles)
    assert PLAN not in chemins(bd)
    e = exclu(bd, PLAN)
    assert e["raison"] == "secret" and e["detail"]["regle"] == "contenu_secret"
    assert e["contenu_sha256"] != decision(bd, PLAN)["contenu_sha256"]
    f = ligne(bd, PLAN)
    assert f["etat"] == "a_traiter" and f["modifie"] is True
    assert bd.execute("SELECT count(*) n FROM morceaux WHERE texte LIKE '%%sk-aaa%%'"
                      ).fetchone()["n"] == 0
    q()  # contenu d'origine relu : l'acceptation vaut de nouveau
    assert PLAN in chemins(bd)


def test_accepter_contenu_secret_exige_l_empreinte_du_contenu(bd, q):
    bd.execute("UPDATE exclus SET contenu_sha256 = NULL WHERE chemin = 'docs/plan.md'")
    bd.commit()
    with pytest.raises(DecisionInvalide):
        accepter(bd, "depot-test", PLAN)
    assert decision(bd, PLAN) is None
