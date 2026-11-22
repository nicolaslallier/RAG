import threading
import time
import unicodedata
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest
import yaml

from rag.config import PUBLIC, TOUT, SourceConf, charger_config
from rag.db import connexion
from rag.gestion import (
    MotifInvalide,
    SourceInconnue,
    annuler,
    apercu,
    correspond,
    exclure,
    exclusions,
    exporter,
    fichier,
    fichiers,
    importer,
    passe_en_cours,
    sources,
    valider_motif,
    verrouiller_source,
)
from rag.graphe import _copier_sans_exclus, charger_graphe, charger_wikiliens, maj_graphe
from rag.indexeur import indexer_source
from rag.sources import LecteurDossier, nfc
from tests.conftest import URL, git

CONF = charger_config(Path("config/rag.yaml")).indexation


def nfd(s):
    return unicodedata.normalize("NFD", s)


RESSOURCES = {"03 - Ressources/Grafana.md", "03 - Ressources/Kubernetes.md"}


def source_coffre(index, **kw):
    return SourceConf(visibilite="public-maison", nom="coffre-maison", type="obsidian",
                      emplacement=str(index.coffre), **kw)


def reindexer_coffre(bd, index, faux_modeles, lecteur=None, **kw):
    return indexer_source(bd, source_coffre(index), lecteur or LecteurDossier(index.coffre),
                          CONF, faux_modeles, **kw)


def exclure_a_la_main(conn, source, motif):
    conn.execute("INSERT INTO exclusions_manuelles (source, motif) VALUES (%s, %s)",
                 (source, motif))
    conn.commit()


def chemins(bd):
    return {r["chemin"] for r in bd.execute("SELECT chemin FROM documents")}


def raisons(bd):
    return {r["chemin"]: (r["raison"], r["lu"])
            for r in bd.execute("SELECT chemin, raison, lu FROM exclus")}


def test_correspond_suit_fnmatch():
    assert correspond("03 - Ressources/Grafana.md", ["03 - Ressources/**"])
    assert correspond("a/b/c.csv", ["*.csv"])  # le * traverse les /
    assert not correspond("Accueil.md", ["03 - Ressources/**", "*.csv"])
    assert not correspond("Accueil.md", [])


def test_motif_manuel_exclu_a_la_passe(bd, index, faux_modeles):
    exclure_a_la_main(bd, "coffre-maison", "03 - Ressources/**")
    bd.execute("DELETE FROM documents WHERE chemin LIKE '03 - Ressources/%'")  # fait par « exclure »
    bd.commit()
    lus = []
    lec = LecteurDossier(index.coffre)
    lire = lec.lire
    lec.lire = lambda c: lus.append(c) or lire(c)
    (index.coffre / "03 - Ressources/Grafana.md").write_text("# Grafana\n\nModifiée.\n")
    reindexer_coffre(bd, index, faux_modeles, lec)
    assert RESSOURCES.isdisjoint(chemins(bd)) and RESSOURCES.isdisjoint(lus)
    assert raisons(bd)["03 - Ressources/Grafana.md"] == ("manuelle", False)
    assert raisons(bd)["03 - Ressources/Kubernetes.md"] == ("manuelle", False)


def test_motif_manuel_d_une_autre_source_ignore(bd, index, faux_modeles):
    exclure_a_la_main(bd, "autre-source", "*")
    reindexer_coffre(bd, index, faux_modeles)
    assert len(chemins(bd)) == 6 and raisons(bd) == {}


def test_motif_couvrant_toute_la_source_ne_bloque_pas_la_passe(bd, index, faux_modeles):
    exclure_a_la_main(bd, "coffre-maison", "*")
    bd.execute("DELETE FROM documents")  # fait par « exclure »
    bd.commit()
    reindexer_coffre(bd, index, faux_modeles)  # pas de CoffrePartielError
    assert chemins(bd) == set()
    assert len(raisons(bd)) == 6 and set(raisons(bd).values()) == {("manuelle", False)}


def test_documents_exclus_a_la_main_non_comptes_comme_absents(bd, index, faux_modeles):
    # même si « exclure » n'a pas encore supprimé les documents : ils partent, sans refus
    exclure_a_la_main(bd, "coffre-maison", "*")
    b = reindexer_coffre(bd, index, faux_modeles)
    assert chemins(bd) == set() and b.supprimes == 6


def exclure_pendant_les_embeddings(faux_modeles, chemin):
    """L'exclusion arrive (autre connexion) pendant le calcul des vecteurs du document."""
    embed = faux_modeles.embed_documents

    def embed_et_exclure(textes, modele):
        with connexion(URL) as autre:
            exclure_a_la_main(autre, "coffre-maison", chemin)
        return embed(textes, modele)

    faux_modeles.embed_documents = embed_et_exclure


def test_exclusion_posee_pendant_la_passe_n_est_pas_ecrite(bd, index, faux_modeles):
    nouvelle = "04 - Archives/Nouvelle.md"
    (index.coffre / nouvelle).parent.mkdir(parents=True)
    (index.coffre / nouvelle).write_text("# Nouvelle\n\nAjoutée après la première passe.\n")
    exclure_pendant_les_embeddings(faux_modeles, nouvelle)
    b = reindexer_coffre(bd, index, faux_modeles)
    assert nouvelle not in chemins(bd) and b.nouveaux == 0
    assert raisons(bd)[nouvelle] == ("manuelle", False)


def test_exclusion_pendant_la_passe_retire_le_document_existant(bd, index, faux_modeles):
    chemin = "Accueil.md"
    (index.coffre / chemin).write_text("# Accueil\n\nModifiée.\n")
    exclure_pendant_les_embeddings(faux_modeles, chemin)
    b = reindexer_coffre(bd, index, faux_modeles)
    assert chemin not in chemins(bd) and b.modifies == 0
    assert raisons(bd)[chemin] == ("manuelle", False)


def test_exclusion_apres_le_listage_jamais_lue_ni_vectorisee(bd, index, faux_modeles):
    premiere, suivante = "04 - Archives/A.md", "04 - Archives/B.md"
    (index.coffre / "04 - Archives").mkdir()
    (index.coffre / premiere).write_text("# A\n\nPremière note.\n")
    (index.coffre / suivante).write_text("# B\n\nContenu confidentiel zanzibar.\n")
    lus, textes = [], []
    lec = LecteurDossier(index.coffre)
    lire = lec.lire

    def lire_et_exclure(c):
        lus.append(c)
        if c == premiere:  # B est déjà listée ; son exclusion arrive pendant la passe
            with connexion(URL) as autre:
                exclure_a_la_main(autre, "coffre-maison", suivante)
        return lire(c)

    lec.lire = lire_et_exclure
    embed = faux_modeles.embed_documents
    faux_modeles.embed_documents = lambda t, m: textes.extend(t) or embed(t, m)
    b = reindexer_coffre(bd, index, faux_modeles, lec)
    assert suivante not in lus and not any("zanzibar" in t for t in textes)
    assert b.nouveaux == 1 and suivante not in chemins(bd)
    assert raisons(bd)[suivante] == ("manuelle", False)


def test_chemin_exact_avec_crochets_exclu(bd, index, faux_modeles):
    chemin = "Notes/[2024] bilan.md"
    (index.coffre / chemin).parent.mkdir(parents=True)
    (index.coffre / chemin).write_text("# Bilan 2024\n\nL'année en revue.\n")
    exclure_a_la_main(bd, "coffre-maison", chemin)  # « [2024] » serait une classe pour fnmatch
    reindexer_coffre(bd, index, faux_modeles)
    assert chemin not in chemins(bd)
    assert raisons(bd)[chemin] == ("manuelle", False)


def test_motif_nfd_couvre_un_chemin_nfc(bd, index, faux_modeles):
    assert correspond(nfc("Pâté chinois.md"), [nfd("Pâté chinois.md")])
    assert correspond(nfd("Recettes/Pâté.md"), [nfc("Recettes/*")])
    exclure_a_la_main(bd, "coffre-maison", nfd("02 - Domaines/Cuisine/Recettes/Pâté chinois.md"))
    bd.execute("DELETE FROM documents WHERE chemin = %s",
               (nfc("02 - Domaines/Cuisine/Recettes/Pâté chinois.md"),))
    bd.commit()
    reindexer_coffre(bd, index, faux_modeles)
    assert nfc("02 - Domaines/Cuisine/Recettes/Pâté chinois.md") not in chemins(bd)


def test_copie_pour_graphify_sans_fichier_nfd_exclu_en_nfc(depot, tmp_path):
    (depot / nfd("Équipe.py")).write_text("x = 1\n")
    git(depot, "add", "-A")
    git(depot, "commit", "-qm", "nfd")
    copie = tmp_path / "copie"
    _copier_sans_exclus(depot, copie, [nfc("Équipe.py")])
    assert (copie / "a.py").exists()
    assert not any(nfc(p.name) == nfc("Équipe.py") for p in copie.rglob("*"))


def test_exclusion_annulee_ne_laisse_pas_de_ligne_perimee(bd, index, faux_modeles):
    # le fichier n'est plus exclu mais reporté par le délai de repos : sa ligne lu=false part
    exclure_a_la_main(bd, "coffre-maison", "03 - Ressources/**")
    bd.execute("DELETE FROM documents WHERE chemin LIKE '03 - Ressources/%'")
    bd.commit()
    reindexer_coffre(bd, index, faux_modeles)
    assert RESSOURCES <= raisons(bd).keys()
    bd.execute("DELETE FROM exclusions_manuelles")
    bd.commit()
    b = indexer_source(bd, source_coffre(index, repos_minutes=10**6),
                       LecteurDossier(index.coffre), CONF, faux_modeles)
    assert b.reportes == 2 and RESSOURCES.isdisjoint(chemins(bd))
    assert RESSOURCES.isdisjoint(raisons(bd))


def test_graphify_ne_voit_pas_les_fichiers_exclus(bd, index_code, monkeypatch, tmp_path):
    exclure_a_la_main(bd, "depot-test", "a.py")
    vus = []

    def extr(clone, travail, **k):
        vus.append(k["exclure"])
        return {"noeuds": [], "liens": []}

    monkeypatch.setattr("rag.graphe.extraire_structure", extr)
    src = SourceConf(visibilite="public-maison", nom="depot-test", type="code", emplacement="x",
                     exclure=["docs/**"])
    reglages = SimpleNamespace(donnees_dir=tmp_path, graphify_commande="graphify")
    maj_graphe(bd, src, index_code, reglages, forcer=True)
    assert len(vus) == 1 and set(vus[0]) == {"docs/**", "a.py"}


def test_sources_compte_indexes_et_exclus(bd, index):
    (s,) = sources(bd, niveaux=PUBLIC)
    assert s["nom"] == "coffre-maison" and s["documents"] == 6
    assert s["exclus_auto"] == 0 and s["exclus_manuels"] == 0
    assert s["demande_en_attente"] is False and s["visibilite"] == "public-maison"


def test_sources_distingue_exclus_auto_et_manuels(bd, index):
    sid = bd.execute("SELECT id FROM sources").fetchone()["id"]
    for chemin, raison in (("a.bin", "binaire"), ("b.key", "secret"), ("c.md", "manuelle")):
        bd.execute("INSERT INTO exclus VALUES (%s, %s, 's', 'e', %s)", (sid, chemin, raison))
    bd.commit()
    (s,) = sources(bd, niveaux=PUBLIC)
    assert (s["exclus_auto"], s["exclus_manuels"]) == (2, 1)


def test_passe_en_cours(bd, index):
    assert passe_en_cours(bd) is None
    bd.execute("INSERT INTO passes (genre, fin) VALUES ('nocturne', now())")
    bd.execute("INSERT INTO passes (genre) VALUES ('demandee')")
    bd.commit()
    p = passe_en_cours(bd)
    assert p["genre"] == "demandee" and p["debut"] is not None and set(p) == {"genre", "debut"}


def test_sources_cache_une_source_privee_au_jeton_public(bd, index):
    bd.execute("UPDATE sources SET visibilite='prive'")
    bd.commit()
    assert sources(bd, niveaux=PUBLIC) == [] and len(sources(bd, niveaux=TOUT)) == 1


def test_fichiers_union_indexes_et_exclus_pagine(bd, index_code):
    r = fichiers(bd, "depot-test", niveaux=PUBLIC, par_page=2)
    total = bd.execute("SELECT (SELECT count(*) FROM documents) + (SELECT count(*) FROM exclus)"
                       " AS n").fetchone()["n"]
    assert r["total"] == total == 6 and r["pages"] == 3
    assert [f["chemin"] for f in r["fichiers"]] == [".env.example", "a.py"]
    exclus = fichiers(bd, "depot-test", niveaux=PUBLIC, etat="exclu_auto")["fichiers"]
    assert exclus and {f["etat"] for f in exclus} == {"exclu_auto"}
    assert all(f["raison"] and f["titre"] is None and f["morceaux"] == 0 for f in exclus)
    dernier = fichiers(bd, "depot-test", niveaux=PUBLIC, par_page=2, page=3)
    assert len(dernier["fichiers"]) == 2
    assert fichiers(bd, "depot-test", niveaux=PUBLIC, par_page=2, page=9)["fichiers"] == []


def test_fichiers_filtre_etat_indexe_et_manuel(bd, index):
    sid = bd.execute("SELECT id FROM sources").fetchone()["id"]
    bd.execute("INSERT INTO exclus VALUES (%s, 'm.md', 's', 'e', 'manuelle')", (sid,))
    bd.commit()
    r = fichiers(bd, "coffre-maison", niveaux=PUBLIC, etat="exclu_manuel")
    assert [(f["chemin"], f["raison"]) for f in r["fichiers"]] == [("m.md", "manuelle")]
    idx = fichiers(bd, "coffre-maison", niveaux=PUBLIC, etat="indexe")
    assert idx["total"] == 6 and all(f["morceaux"] > 0 and f["titre"] for f in idx["fichiers"])


def test_fichiers_fichier_present_des_deux_cotes_compte_une_fois(bd, index):
    sid = bd.execute("SELECT id FROM sources").fetchone()["id"]
    bd.execute("INSERT INTO exclus VALUES (%s, 'Accueil.md', 's', 'e', 'manuelle')", (sid,))
    bd.commit()
    r = fichiers(bd, "coffre-maison", niveaux=PUBLIC)
    assert r["total"] == 6 and {f["etat"] for f in r["fichiers"]} == {"indexe"}


def test_fichiers_filtre_contient(bd, index):
    r = fichiers(bd, "coffre-maison", niveaux=PUBLIC, contient="POULET")
    assert r["total"] == 1 and "poulet" in r["fichiers"][0]["chemin"].lower()
    assert fichiers(bd, "coffre-maison", niveaux=PUBLIC, contient="%")["total"] == 0


def test_fichiers_source_privee_ou_inconnue(bd, index):
    bd.execute("UPDATE sources SET visibilite='prive'")
    bd.commit()
    for nom in ("coffre-maison", "nexiste-pas"):
        with pytest.raises(SourceInconnue):
            fichiers(bd, nom, niveaux=PUBLIC)
    assert fichiers(bd, "coffre-maison", niveaux=TOUT)["total"] == 6


def test_fichier_detail_avec_morceaux(bd, index):
    v = bd.execute("SELECT max(version_id) AS v FROM morceaux").fetchone()["v"]
    f = fichier(bd, "coffre-maison", "Accueil.md", niveaux=PUBLIC, version_id=v)
    assert f["morceaux"] and f["morceaux"][0]["fil"].startswith("Accueil")
    assert [m["ordre"] for m in f["morceaux"]] == sorted(m["ordre"] for m in f["morceaux"])
    assert {"titre", "type", "tags", "alias", "origine", "frontmatter", "liens_sortants",
            "modifie_le"} <= f.keys()
    assert fichier(bd, "coffre-maison", "Accueil.md", niveaux=PUBLIC,
                   version_id=None)["morceaux"] == []


def test_fichier_absent_ou_invisible(bd, index):
    with pytest.raises(SourceInconnue):
        fichier(bd, "coffre-maison", "nexiste.md", niveaux=PUBLIC, version_id=None)
    bd.execute("UPDATE sources SET visibilite='prive'")
    bd.commit()
    with pytest.raises(SourceInconnue):
        fichier(bd, "coffre-maison", "Accueil.md", niveaux=PUBLIC, version_id=None)


PATE = "02 - Domaines/Cuisine/Recettes/Pâté chinois.md"


def test_apercu_et_exclure_immediat(bd, index):
    a = apercu(bd, "coffre-maison", "02 - Domaines/**")
    assert a["total"] == 2 and a["chemins"] == sorted(a["chemins"]) and PATE in a["chemins"]
    assert exclure(bd, "coffre-maison", "02 - Domaines/**", "essai") == 2
    assert PATE not in chemins(bd)
    assert raisons(bd)[PATE] == ("manuelle", False)
    assert exclusions(bd)[0]["raison"] == "essai"
    assert apercu(bd, "coffre-maison", "02 - Domaines/**")["total"] == 0


def test_exclure_deux_fois_ne_duplique_pas(bd, index):
    exclure(bd, "coffre-maison", "Accueil.md")
    assert exclure(bd, "coffre-maison", "Accueil.md") == 0
    assert len(exclusions(bd)) == 1


def test_exclure_retire_les_noeuds_du_graphe(bd, index):
    charger_wikiliens(bd, "coffre-maison")
    bd.commit()
    assert bd.execute("SELECT 1 FROM noeuds WHERE cle = 'Accueil.md'").fetchone()
    exclure(bd, "coffre-maison", "Accueil.md")
    assert bd.execute("SELECT 1 FROM noeuds WHERE cle = 'Accueil.md'").fetchone() is None


def test_exclure_source_inconnue(bd, index):
    with pytest.raises(SourceInconnue):
        exclure(bd, "nexiste-pas", "*")
    with pytest.raises(SourceInconnue):
        apercu(bd, "nexiste-pas", "*")
    assert exclusions(bd) == []


def test_motif_vide_ou_trop_long(bd, index):
    for motif in ("", "  ", "x" * 1001):
        with pytest.raises(MotifInvalide):
            exclure(bd, "coffre-maison", motif)
    with pytest.raises(MotifInvalide):
        exclure(bd, "coffre-maison", "Accueil.md", "r" * 501)
    assert exclusions(bd) == [] and "Accueil.md" in chemins(bd)


def test_motif_valide_nettoye_et_nfc(bd, index):
    assert valider_motif("  " + nfd("Pâté.md") + " ") == nfc("Pâté.md")
    assert valider_motif("x" * 1000) == "x" * 1000
    assert exclure(bd, "coffre-maison", nfd(PATE), "   ") == 1
    (e,) = exclusions(bd)
    assert e["motif"] == nfc(PATE) and e["raison"] is None


def test_annuler_demande_la_reindexation(bd, index, faux_modeles):
    exclure(bd, "coffre-maison", "Accueil.md")
    assert annuler(bd, "coffre-maison", "Accueil.md") is True
    assert exclusions(bd) == [] and "Accueil.md" not in raisons(bd)
    assert bd.execute("SELECT demande_le FROM sources").fetchone()["demande_le"] is not None
    reindexer_coffre(bd, index, faux_modeles)
    assert "Accueil.md" in chemins(bd)


def test_annuler_garde_les_lignes_encore_couvertes(bd, index):
    exclure(bd, "coffre-maison", "02 - Domaines/**")
    exclure(bd, "coffre-maison", PATE)
    assert annuler(bd, "coffre-maison", "02 - Domaines/**") is True
    assert PATE in raisons(bd) and len([r for r in raisons(bd).values()
                                        if r == ("manuelle", False)]) == 1


def test_annuler_motif_absent(bd, index):
    assert annuler(bd, "coffre-maison", "zzz") is False
    assert bd.execute("SELECT demande_le FROM sources").fetchone()["demande_le"] is None


def test_annuler_sur_source_de_code_force_le_graphe(bd, index_code):
    bd.execute("UPDATE sources SET graphe_revision = 'abc'")
    bd.commit()
    exclure(bd, "depot-test", "a.py")
    assert annuler(bd, "depot-test", "a.py") is True
    assert bd.execute("SELECT graphe_revision FROM sources").fetchone()["graphe_revision"] is None


def test_export_import_aller_retour(bd, index):
    exclure(bd, "coffre-maison", "Accueil.md", "bruit")
    t = exporter(bd)
    assert yaml.safe_load(t) == [{"source": "coffre-maison", "motif": "Accueil.md",
                                  "raison": "bruit"}]
    bd.execute("DELETE FROM exclusions_manuelles")
    bd.commit()
    assert importer(bd, t) == {"ajoutees": 1, "ignorees": 0, "retires": 0}
    assert importer(bd, t) == {"ajoutees": 0, "ignorees": 1, "retires": 0}
    assert [(e["source"], e["motif"], e["raison"]) for e in exclusions(bd)] == [
        ("coffre-maison", "Accueil.md", "bruit")]


def test_import_applique_l_exclusion_et_garde_une_source_inconnue(bd, index):
    t = "- {source: coffre-maison, motif: Accueil.md}\n- {source: future, motif: '*.csv'}\n"
    assert importer(bd, t) == {"ajoutees": 2, "ignorees": 0, "retires": 1}
    assert "Accueil.md" not in chemins(bd)
    assert {e["source"] for e in exclusions(bd)} == {"coffre-maison", "future"}


@pytest.mark.parametrize("texte", [
    "pas une liste", "- {source: a}", "- {source: a, motif: '', raison: x}",
    "- {source: a, motif: b, autre: 1}", "- {source: a, motif: b}\n- 3",
    "- {source: a, motif: b, raison: 3}", "- {source: 1, motif: b}", ": [",
    "- {source: a, motif: b, raison: " + "r" * 501 + "}",
    "- {source: " + "s" * 501 + ", motif: b}"])
def test_import_malforme_n_ecrit_rien(bd, index, texte):
    with pytest.raises(MotifInvalide):
        importer(bd, texte)
    assert exclusions(bd) == []


def test_lignes_exclus_du_listage_non_verrouillees_pendant_la_lecture(bd, index, faux_modeles):
    # les exclus décidés au listage sont validés avant le travail lent (lecture, embeddings)
    exclure_a_la_main(bd, "coffre-maison", "03 - Ressources/**")
    reindexer_coffre(bd, index, faux_modeles)  # lignes exclus déjà là : la passe les met à jour
    (index.coffre / "Accueil.md").write_text("# Accueil\n\nModifiée.\n")
    lec = LecteurDossier(index.coffre)
    lire, lus, bloques = lec.lire, [], []

    def lire_et_verrouiller(c):
        lus.append(c)
        with connexion(URL) as autre:
            try:
                autre.execute("SELECT 1 FROM exclus WHERE raison = 'manuelle' FOR UPDATE NOWAIT")
            except psycopg.errors.LockNotAvailable:
                bloques.append(c)
        return lire(c)

    lec.lire = lire_et_verrouiller
    reindexer_coffre(bd, index, faux_modeles, lec)
    assert lus == ["Accueil.md"] and bloques == []
    assert raisons(bd)["03 - Ressources/Grafana.md"] == ("manuelle", False)


# --- Verrou par source : une exclusion ne croise pas l'écriture d'un document ou de nœuds ------

def en_fond(f, *a):
    """Lance `f(conn, *a)` sur une autre connexion, dans un fil ; le résultat dans `fil.r`."""
    fil = threading.Thread(target=lambda: setattr(fil, "r", _avec_autre(f, *a)), daemon=True)
    fil.start()
    return fil


def _avec_autre(f, *a):
    with connexion(URL) as autre:
        r = f(autre, *a)
        autre.commit()
        return r


def bloque_par_le_verrou(source, f, *a):
    """Vrai si `f` attend qu'une autre transaction lâche le verrou de `source`, puis finit."""
    with connexion(URL) as tenant:
        verrouiller_source(tenant, source)
        fil = en_fond(f, *a)
        time.sleep(0.3)
        bloque = fil.is_alive()
        tenant.rollback()
    fil.join(5)
    return bloque and not fil.is_alive()


def test_exclusion_attend_l_ecriture_du_document_puis_le_retire(bd, index, faux_modeles,
                                                                 monkeypatch):
    nouvelle = "04 - Archives/Nouvelle.md"
    (index.coffre / nouvelle).parent.mkdir(parents=True)
    (index.coffre / nouvelle).write_text("# Nouvelle\n\nAjoutée après la première passe.\n")
    import rag.indexeur
    ecrire, vu = rag.indexeur._ecrire, {}

    def ecrire_puis_exclure(conn, source_id, chemin, *a):
        n = ecrire(conn, source_id, chemin, *a)
        if chemin == nouvelle:  # écrit, pas encore validé : l'exclusion doit attendre
            vu["fil"] = en_fond(exclure, "coffre-maison", nouvelle)
            time.sleep(0.3)
            vu["attend"] = vu["fil"].is_alive()
        return n

    monkeypatch.setattr(rag.indexeur, "_ecrire", ecrire_puis_exclure)
    b = reindexer_coffre(bd, index, faux_modeles)
    vu["fil"].join(5)
    assert b.nouveaux == 1 and vu["attend"] is True and vu["fil"].r == 1
    assert nouvelle not in chemins(bd) and raisons(bd)[nouvelle] == ("manuelle", False)


def test_exclure_et_importer_prennent_le_verrou_de_la_source(bd, index):
    assert bloque_par_le_verrou("coffre-maison", exclure, "coffre-maison", "Accueil.md")
    t = "- {source: coffre-maison, motif: '05 - Journal/**'}\n- {source: future, motif: x}\n"
    assert bloque_par_le_verrou("coffre-maison", importer, t)
    assert {"Accueil.md", "05 - Journal/2026-11-20.md"}.isdisjoint(chemins(bd))


def test_charger_wikiliens_prend_le_verrou_de_la_source(bd, index):
    assert bloque_par_le_verrou("coffre-maison", charger_wikiliens, "coffre-maison")
    assert bd.execute("SELECT 1 FROM noeuds WHERE cle = 'Accueil.md'").fetchone()


def test_charger_graphe_ignore_les_fichiers_exclus_entre_temps(bd, index_code):
    # graphify a tourné avant l'exclusion ; ses nœuds de a.py ne reviennent pas
    exclure(bd, "depot-test", "a.py")
    g = {"noeuds": [{"cle": "a", "libelle": "aide", "type": "fonction", "fichier": "a.py"},
                    {"cle": "g", "libelle": "Guide", "type": "fichier",
                     "fichier": "docs/guide.md"}],
         "liens": [{"de": "g", "vers": "a", "type": "cite", "confiance": "EXTRAIT"}]}
    assert bloque_par_le_verrou("depot-test", charger_graphe, "depot-test", g)
    assert {r["cle"] for r in bd.execute("SELECT cle FROM noeuds")} == {"g"}


def test_import_de_milliers_de_sources_inconnues_sans_verrou(bd, index, monkeypatch):
    # une source inconnue n'est indexée par personne : pas de verrou (la table des verrous
    # partagée de Postgres est bornée par max_locks_per_transaction)
    import rag.gestion
    verrous, verrouiller = [], rag.gestion.verrouiller_source
    monkeypatch.setattr(rag.gestion, "verrouiller_source",
                        lambda conn, s: verrous.append(s) or verrouiller(conn, s))
    t = "".join(f"- {{source: inconnue-{i}, motif: x}}\n" for i in range(2000))
    t += "- {source: coffre-maison, motif: Accueil.md}\n"
    assert importer(bd, t) == {"ajoutees": 2001, "ignorees": 0, "retires": 1}
    assert verrous == ["coffre-maison"]
