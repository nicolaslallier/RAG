from pathlib import Path

import pytest

from rag.config import SourceConf, charger_config
from rag.graphe import charger_graphe, charger_wikiliens
from rag.indexeur import indexer_source
from rag.modeles import ErreurModele
from rag.semantique import concepts_de_note, mentions_de_code, passe_semantique
from rag.sources import LecteurDossier
from tests.conftest import indexer_coffre, inscrire_source

CONFIG = charger_config(Path("config/rag.yaml"))
NOTES = {"deploiement.md": "# Déploiement\n\nVoir [[outils]].",
         "outils.md": "# Outils\n\nGrafana et Prometheus.",
         "seule.md": "# Seule\n\nRien."}
N_NOTES = len(NOTES)


class FauxGenerateur:
    def __init__(self, reponses=None, repete=None, tombe_sur=None):
        self.reponses, self.repete, self.tombe_sur, self.appels = reponses or [], repete, \
            tombe_sur, []

    def generer(self, modele, messages, temperature, json_mode=False, delai=None):
        self.appels.append(messages)
        assert temperature == 0 and json_mode
        if self.tombe_sur and self.tombe_sur in messages[-1]["content"]:
            raise ErreurModele("panne")
        return self.reponses.pop(0) if self.reponses else self.repete


def _code(bd, tmp_path, faux_modeles, nom, fichiers, noeuds):
    d = tmp_path / nom
    for chemin, texte in fichiers.items():
        (d / chemin).parent.mkdir(parents=True, exist_ok=True)
        (d / chemin).write_text(texte)
    src = SourceConf(visibilite="public-maison", nom=nom, type="code", emplacement="x")
    inscrire_source(bd, nom)
    indexer_source(bd, src, LecteurDossier(d), CONFIG.indexation, faux_modeles)
    charger_graphe(bd, nom, {"noeuds": noeuds, "liens": []})


def _n(cle, libelle, type_="fonction", fichier=None):
    return {"cle": cle, "libelle": libelle, "type": type_, "fichier": fichier}


@pytest.fixture
def index_mixte(bd, tmp_path, faux_modeles):
    _code(bd, tmp_path, faux_modeles, "RAG", {"rag/indexeur.py": "def indexer_source(): pass\n"},
          [_n("rag_indexeur", "indexeur.py", "fichier", "rag/indexeur.py"),
           _n("rag_is", "indexer_source", fichier="rag/indexeur.py"),
           _n("rag_app", "create_app"), _n("rag_run", "run")])
    _code(bd, tmp_path, faux_modeles, "Infra", {"litellm/config.yaml": "model_list: []\n"},
          [_n("infra_app", "create_app")])
    indexer_coffre(bd, tmp_path, faux_modeles, NOTES)
    charger_wikiliens(bd, "coffre-maison")
    return bd.execute("SELECT id FROM documents WHERE chemin='deploiement.md'").fetchone()["id"]


def cibles(bd, liens):
    return sorted(bd.execute("SELECT libelle FROM noeuds WHERE id=%s", (lk["vers_id"],)
                             ).fetchone()["libelle"] for lk in liens)


def test_mentions_exactes_sans_modele(bd, index_mixte):
    m = mentions_de_code(bd, index_mixte, "Le déploiement passe par indexer_source dans le dépôt "
                                          "RAG, voir litellm/config.yaml.")
    assert {(x["type"], x["confiance"]) for x in m} == {("mentionne", "EXTRAIT")} and len(m) == 3
    assert cibles(bd, m) == ["RAG", "indexer_source", "litellm/config.yaml"]


def test_mot_entier_seulement(bd, index_mixte):
    assert mentions_de_code(bd, index_mixte, "indexer_sources_bis et run, RAGOUT") == []


def test_symbole_homonyme_ambigu(bd, index_mixte):
    m = mentions_de_code(bd, index_mixte, "Voir create_app.")
    assert len(m) == 2 and {x["confiance"] for x in m} == {"AMBIGU"}


def test_concepts_reponse_invalide_donne_rien():
    assert concepts_de_note(FauxGenerateur(["pas json"]), "g", "T", "x") == []
    assert concepts_de_note(FauxGenerateur(['{"concepts": "x"}']), "g", "T", "x") == []
    r = concepts_de_note(FauxGenerateur([('{"concepts": [{"nom": "x", "relation": "concerne"}, '
                                         '{"nom": "Grafana", "relation": "utilise"}]}')]),
                         "g", "T", "x")
    assert r == [{"nom": "Grafana", "relation": "utilise"}]


def test_passe_incrementale(bd, index_mixte):
    m = FauxGenerateur(repete='{"concepts": [{"nom": "Grafana", "relation": "utilise"}]}')
    assert passe_semantique(bd, CONFIG, m)["traites"] == N_NOTES
    n = len(m.appels)
    assert passe_semantique(bd, CONFIG, m)["traites"] == 0 and len(m.appels) == n
    assert bd.execute("SELECT count(*) n FROM noeuds WHERE type='concept'").fetchone()["n"] == 1
    assert bd.execute("SELECT count(*) n FROM liens WHERE origine='semantique' AND "
                      "confiance='INFERE'").fetchone()["n"] == N_NOTES


def test_maximum(bd, index_mixte):
    m = FauxGenerateur(repete='{"concepts": []}')
    assert passe_semantique(bd, CONFIG, m, maximum=1)["traites"] == 1


def test_note_modifiee_remplace_ses_liens(bd, index_mixte, tmp_path, faux_modeles):
    passe_semantique(bd, CONFIG, FauxGenerateur(
        repete='{"concepts": [{"nom": "Grafana", "relation": "utilise"}]}'))
    indexer_coffre(bd, tmp_path, faux_modeles, {"seule.md": "# Seule\n\nVoir indexer_source."})
    r = passe_semantique(bd, CONFIG, FauxGenerateur(
        repete='{"concepts": [{"nom": "Loki", "relation": "decrit"}]}'))
    assert r["traites"] == 1
    lib = {x["libelle"] for x in bd.execute(
        "SELECT b.libelle FROM liens l JOIN noeuds a ON a.id=l.de JOIN noeuds b ON b.id=l.vers "
        "WHERE a.cle='seule.md' AND l.origine='semantique'")}
    assert lib == {"Loki", "indexer_source"}


def test_echec_du_modele_n_arrete_pas_la_passe(bd, index_mixte):
    m = FauxGenerateur(repete='{"concepts": []}', tombe_sur="# outils")
    r = passe_semantique(bd, CONFIG, m)
    assert r == {"traites": 2, "liens": 0, "echecs": 1}
    assert bd.execute("SELECT semantique_sha FROM documents WHERE chemin='outils.md'"
                      ).fetchone()["semantique_sha"] is None


def test_les_liens_de_structure_et_wikiliens_survivent(bd, index_mixte):
    avant = bd.execute("SELECT count(*) n FROM liens").fetchone()["n"]
    passe_semantique(bd, CONFIG, FauxGenerateur(repete='{"concepts": []}'))
    charger_wikiliens(bd, "coffre-maison")
    assert bd.execute("SELECT count(*) n FROM liens WHERE origine='wikilien'").fetchone()["n"] == 1
    assert bd.execute("SELECT count(*) n FROM liens").fetchone()["n"] == avant


def test_mot_courant_seulement_entre_backticks(bd, index_mixte):
    charger_graphe(bd, "RAG", {"noeuds": [_n("rag_fmt", "format")], "liens": []})
    assert mentions_de_code(bd, index_mixte, "Le format du fichier.") == []
    assert len(mentions_de_code(bd, index_mixte, "Voir `format`.")) == 1
