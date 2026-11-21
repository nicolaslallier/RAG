from dataclasses import dataclass
from pathlib import Path

import pytest

from rag.banc.synthetique import generer_jeu, tirer
from rag.config import SourceConf, charger_config
from rag.indexeur import indexer_source, version_courante
from rag.sources import LecteurDossier

CONFIG = charger_config(Path("config/rag.yaml"))
BON = '{"question": "Quels légumes accompagnent le poulet au cidre ?"}'


@dataclass
class Appel:
    modele: str


class FauxGenerateur:
    def __init__(self, reponses, juge='{"ok": true}'):
        self.reponses, self.juge, self.appels = list(reponses), juge, []

    def generer(self, modele, messages, temperature, json_mode=False):
        self.appels.append(Appel(modele))
        if modele == CONFIG.banc.modele_juge:
            return self.juge
        return self.reponses.pop(0) if self.reponses else "bruit"


@pytest.fixture
def long_index(bd, index, faux_modeles):
    for i in range(4):
        (index.coffre / f"Long{i}.md").write_text(
            f"# Long {i}\n\n" + " ".join(f"mot{i}x{j}" for j in range(120)) + "\n")
    indexer_source(bd, SourceConf(nom="coffre-maison", type="obsidian", emplacement="x"),
                   LecteurDossier(index.coffre), CONFIG.indexation, faux_modeles)
    return version_courante(bd, CONFIG.indexation)


def test_filtre_et_forme(bd, long_index):
    m = FauxGenerateur(["pas du json", '{"question": "court ?"}', BON])
    cas = generer_jeu(bd, m, long_index, CONFIG.banc, n=1)
    assert len(cas) == 1 and cas[0].id.startswith("syn-")
    assert cas[0].attendus[0].startswith("coffre-maison:Long")
    assert cas[0].nature == "factuel" and cas[0].statut == "valide"


def test_rejette_la_recopie(bd, long_index):
    texte = bd.execute("SELECT texte FROM morceaux WHERE version_id=%s AND texte LIKE '%%mot0x%%'",
                       (long_index,)).fetchone()["texte"]
    m = FauxGenerateur(['{"question": "Que veut dire mot0x1 mot0x2 mot0x3 mot0x4 ici ?"}'])
    assert "mot0x1 mot0x2 mot0x3 mot0x4" in texte
    assert generer_jeu(bd, m, long_index, CONFIG.banc, n=1) == []


def test_rejette_doublon(bd, long_index):
    m = FauxGenerateur([BON, BON.upper().replace("QUESTION", "question")])
    assert len(generer_jeu(bd, m, long_index, CONFIG.banc, n=2)) == 1


def test_rejette_si_le_juge_dit_non(bd, long_index):
    m = FauxGenerateur([BON] * 3, juge='{"ok": false}')
    assert generer_jeu(bd, m, long_index, CONFIG.banc, n=1) == []


def test_s_arrete_apres_3n_essais(bd, long_index):
    m = FauxGenerateur([])
    assert generer_jeu(bd, m, long_index, CONFIG.banc, n=1) == [] and len(m.appels) == 3


def test_graine_reproductible(bd, long_index):
    a = [x[0] for x in tirer(bd, long_index, 1)]
    assert a == [x[0] for x in tirer(bd, long_index, 1)] and len(a) == 4


def test_au_plus_deux_par_document(bd, index, faux_modeles):
    (index.coffre / "Gros.md").write_text(
        "\n\n".join(f"# S{i}\n\n" + " ".join(f"g{i}m{j}" for j in range(400)) for i in range(4)))
    indexer_source(bd, SourceConf(nom="coffre-maison", type="obsidian", emplacement="x"),
                   LecteurDossier(index.coffre), CONFIG.indexation, faux_modeles)
    cles = [x[2] for x in tirer(bd, version_courante(bd, CONFIG.indexation), 1)]
    assert cles.count("coffre-maison:Gros.md") == 2


def test_generateur_et_juge_sont_deux_modeles(bd, long_index):
    m = FauxGenerateur([BON])
    generer_jeu(bd, m, long_index, CONFIG.banc, n=1)
    assert {a.modele for a in m.appels} == {"gemma4:26b-a4b-it-qat", "qwen3.8:27b-mlx"}
