from pathlib import Path
from types import SimpleNamespace

from rag.config import SourceConf, charger_config
from rag.db import connexion
from rag.gestion import correspond
from rag.graphe import maj_graphe
from rag.indexeur import indexer_source
from rag.sources import LecteurDossier
from tests.conftest import URL

CONF = charger_config(Path("config/rag.yaml")).indexation
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


def test_exclusion_posee_pendant_la_passe_n_est_pas_ecrite(bd, index, faux_modeles):
    nouvelle = "04 - Archives/Nouvelle.md"
    (index.coffre / nouvelle).parent.mkdir(parents=True)
    (index.coffre / nouvelle).write_text("# Nouvelle\n\nAjoutée après la première passe.\n")
    lec = LecteurDossier(index.coffre)
    lire = lec.lire

    def lire_et_exclure(chemin):
        if chemin == nouvelle:
            with connexion(URL) as autre:
                exclure_a_la_main(autre, "coffre-maison", nouvelle)
        return lire(chemin)

    lec.lire = lire_et_exclure
    b = reindexer_coffre(bd, index, faux_modeles, lec)
    assert nouvelle not in chemins(bd) and b.nouveaux == 0
    assert raisons(bd)[nouvelle] == ("manuelle", False)


def test_exclusion_pendant_la_passe_retire_le_document_existant(bd, index, faux_modeles):
    chemin = "Accueil.md"
    (index.coffre / chemin).write_text("# Accueil\n\nModifiée.\n")
    lec = LecteurDossier(index.coffre)
    lire = lec.lire

    def lire_et_exclure(c):
        if c == chemin:
            with connexion(URL) as autre:
                exclure_a_la_main(autre, "coffre-maison", chemin)
        return lire(c)

    lec.lire = lire_et_exclure
    b = reindexer_coffre(bd, index, faux_modeles, lec)
    assert chemin not in chemins(bd) and b.modifies == 0
    assert raisons(bd)[chemin] == ("manuelle", False)


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
