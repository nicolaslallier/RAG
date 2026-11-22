import pytest

from rag.config import PUBLIC, TOUT
from rag.graphe import chemin as _chemin
from rag.graphe import expliquer as _expliquer


def chemin(bd, a, b, niveaux=PUBLIC, **k):
    return _chemin(bd, a, b, niveaux=niveaux, **k)


def expliquer(bd, x, niveaux=PUBLIC, **k):
    return _expliquer(bd, x, niveaux=niveaux, **k)


def _noeud(bd, sid, cle, libelle, communaute=None):
    from rag.recherche import normaliser
    return bd.execute("INSERT INTO noeuds (source_id, cle, libelle, libelle_norm, type, communaute)"
                      " VALUES (%s,%s,%s,%s,'fonction',%s) RETURNING id",
                      (sid, cle, libelle, normaliser(libelle), communaute)).fetchone()["id"]


@pytest.fixture
def graphe(bd):
    """A—B—C—D (B dans une source « publique » à part), E isolé, deux « config », « Indexeur »."""
    def source(nom, vis):
        return bd.execute("INSERT INTO sources (nom, type, emplacement, visibilite) VALUES "
                          "(%s, 'code', 'x', %s) RETURNING id", (nom, vis)).fetchone()["id"]
    s, s2 = source("pub", "public-maison"), source("autre", "public-maison")
    ids = {x: _noeud(bd, s2 if x == "B" else s, x, x, 1) for x in "ABCDE"}
    ids["config1"] = _noeud(bd, s, "c1", "config")
    ids["config2"] = _noeud(bd, s, "c2", "config")
    ids["Indexeur"] = _noeud(bd, s, "idx", "Indexeur")
    for de, vers, conf in (("A", "B", "EXTRAIT"), ("C", "B", "INFERE"), ("C", "D", "EXTRAIT"),
                           ("Indexeur", "A", "AMBIGU")):
        bd.execute("INSERT INTO liens (de, vers, type, confiance, origine) VALUES "
                   "(%s,%s,'appelle',%s,'structure')", (ids[de], ids[vers], conf))
    bd.commit()
    return ids


def test_plus_court_chemin_non_oriente(bd, graphe):
    r = chemin(bd, "A", "D")
    assert r["etat"] == "ok" and [e["noeud"]["libelle"] for e in r["etapes"]] == ["A", "B", "C", "D"]
    assert chemin(bd, "D", "A")["etat"] == "ok" and r["etapes"][1]["lien"]["confiance"] == "EXTRAIT"
    assert r["etapes"][0]["lien"] is None and r["etapes"][2]["lien"]["sens"] == "entrant"


def test_aucun_chemin_et_limite_de_sauts(bd, graphe):
    assert chemin(bd, "A", "E")["etat"] == "aucun_chemin"
    assert chemin(bd, "A", "D", max_sauts=2)["etat"] == "aucun_chemin"
    assert chemin(bd, "A", "D", max_sauts=3)["etat"] == "ok"


def test_nom_ambigu_rend_les_candidats(bd, graphe):
    r = chemin(bd, "config", "A")
    assert r["etat"] == "ambigu" and len(r["candidats"]["a"]) == 2 and r["etapes"] == []
    assert chemin(bd, f"id:{r['candidats']['a'][0]['id']}", "A")["etat"] in ("ok", "aucun_chemin")


def test_nom_inconnu(bd, graphe):
    assert chemin(bd, "zzzqqq", "A")["etat"] == "introuvable"
    assert chemin(bd, "", "A")["etat"] == "introuvable"


def test_faute_de_frappe_unique_acceptee(bd, graphe):
    r = chemin(bd, "Indexeurr", "A")
    assert r["etat"] == "ok" and r["etapes"][0]["noeud"]["libelle"] == "Indexeur"


def test_expliquer(bd, graphe):
    r = expliquer(bd, "B")
    assert r["etat"] == "ok" and {v["libelle"] for v in r["voisins"]} == {"A", "C"}
    assert r["n_voisins"] == 2 and r["voisins"][0]["confiance"] == "EXTRAIT"
    assert {v["sens"] for v in r["voisins"]} == {"entrant"}
    assert expliquer(bd, "A")["communaute"]["taille"] == 4  # A, C, D, E (B est dans une autre source)


def test_expliquer_ambigu_et_inconnu(bd, graphe):
    r = expliquer(bd, "config")
    assert r["etat"] == "ambigu" and len(r["candidats"]) == 2
    assert expliquer(bd, "zzz")["etat"] == "introuvable"


def test_un_chemin_ne_traverse_pas_une_source_privee(bd, graphe):
    bd.execute("UPDATE sources SET visibilite='prive' WHERE nom='autre'")
    bd.commit()
    assert chemin(bd, "A", "D", niveaux=PUBLIC)["etat"] == "aucun_chemin"
    assert chemin(bd, "A", "D", niveaux=TOUT)["etat"] == "ok"
    assert chemin(bd, "B", "A", niveaux=PUBLIC)["etat"] == "introuvable"
    assert "B" not in {v["libelle"] for v in expliquer(bd, "A", niveaux=PUBLIC)["voisins"]}
    assert chemin(bd, "A", "D", niveaux=frozenset())["etat"] == "introuvable"
