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
    indexer_source(bd, SourceConf(visibilite="public-maison", nom="coffre-maison", type="obsidian", emplacement="x"),
                   LecteurDossier(index.coffre), CONFIG.indexation, faux_modeles)
    return version_courante(bd, CONFIG.indexation)


def test_filtre_et_forme(bd, long_index):
    m = FauxGenerateur(["pas du json", '{"question": "court ?"}', BON])
    cas = generer_jeu(bd, m, long_index, CONFIG.banc, n=1)
    assert len(cas) == 1 and cas[0].id.startswith("syn-")
    assert cas[0].attendus[0].startswith("coffre-maison:Long")
    assert cas[0].nature == "factuel" and cas[0].statut == "valide"


def test_rejette_la_recopie(bd, long_index):
    texte = next(tirer(bd, long_index, 1))[1]
    quatre = " ".join(texte.split()[-4:])
    m = FauxGenerateur([f'{{"question": "Que veut dire {quatre} ici ?"}}'])
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
    indexer_source(bd, SourceConf(visibilite="public-maison", nom="coffre-maison", type="obsidian", emplacement="x"),
                   LecteurDossier(index.coffre), CONFIG.indexation, faux_modeles)
    cles = [x[2] for x in tirer(bd, version_courante(bd, CONFIG.indexation), 1)]
    assert cles.count("coffre-maison:Gros.md") == 2


def test_generateur_et_juge_sont_deux_modeles(bd, long_index):
    m = FauxGenerateur([BON])
    generer_jeu(bd, m, long_index, CONFIG.banc, n=1)
    assert {a.modele for a in m.appels} == {"gemma4:26b-a4b-it-qat", "qwen3.8:27b-mlx"}


def test_erreur_modele_est_un_rejet(bd, long_index):
    from rag.modeles import ErreurModele

    class Panne(FauxGenerateur):
        def generer(self, *a, **k):
            raise ErreurModele("ReadTimeout")
    assert generer_jeu(bd, Panne([]), long_index, CONFIG.banc, n=1) == []


def test_reprise_garde_les_cas_et_saute_leurs_morceaux(bd, long_index):
    premier = generer_jeu(bd, FauxGenerateur([BON]), long_index, CONFIG.banc, n=1)
    m = FauxGenerateur(['{"question": "Quelle est la couleur du ciel le soir ?"}'])
    cas = generer_jeu(bd, m, long_index, CONFIG.banc, n=2, deja=premier)
    assert cas[0] == premier[0] and len(cas) == 2 and cas[1].id != cas[0].id


def test_rejette_les_questions_vagues(bd, long_index):
    m = FauxGenerateur(['{"question": "Quels outils sont décrits dans cette stack ?"}',
                        '{"question": "Que contient ces notes au sujet du souper ?"}'])
    rejets = __import__("collections").Counter()
    assert generer_jeu(bd, m, long_index, CONFIG.banc, n=1, rejets=rejets) == []
    assert rejets["vague"] == 2


def test_juge_demande_une_seule_note():
    from rag.banc.synthetique import CONSIGNE, CONSIGNE_JUGE
    assert "une seule note" in CONSIGNE_JUGE and "date" in CONSIGNE


def test_source_privee_exclue_du_tirage(bd, long_index):
    bd.execute("UPDATE sources SET visibilite = 'prive'")
    assert list(tirer(bd, long_index, 1)) == []


def test_plafond_compte_les_cas_deja_presents(bd, long_index):
    from rag.banc.execution import Cas
    cle = next(tirer(bd, long_index, 1))[2]
    deja = [Cas(id=f"syn-x{i}", question=f"q{i} ?", nature="factuel", attendus=[cle]) for i in (1, 2)]
    assert cle not in [x[2] for x in tirer(bd, long_index, 1, deja=deja)]


def test_commande_ecrit_au_fil_de_l_eau_et_reprend(bd, long_index, tmp_path, monkeypatch):
    from argparse import Namespace

    from rag.banc import synthetique as s
    sortie = tmp_path / "sous" / "synthetique.jsonl"
    monkeypatch.setattr(s, "SORTIE", sortie)
    monkeypatch.setattr(s, "version_courante", lambda conn, conf: long_index)
    a = Namespace(n=1, graine=1, source=["coffre-maison"], reprendre=False, ecraser=False)
    assert s.commande_synthetique(a, bd, CONFIG, FauxGenerateur([BON])) == 0
    assert len(sortie.read_text().splitlines()) == 1
    assert s.commande_synthetique(a, bd, CONFIG, FauxGenerateur([BON])) == 2  # pas d'écrasement
    a.reprendre, a.n = True, 2
    q2 = '{"question": "Quelle est la couleur du ciel le soir à Québec ?"}'
    assert s.commande_synthetique(a, bd, CONFIG, FauxGenerateur([q2])) == 0
    assert len(sortie.read_text().splitlines()) == 2
    a.reprendre, a.ecraser, a.n = False, True, 1
    assert s.commande_synthetique(a, bd, CONFIG, FauxGenerateur([q2])) == 0
    assert len(sortie.read_text().splitlines()) == 1
