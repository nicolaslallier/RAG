import subprocess
import sys
from pathlib import Path

import pytest

from rag.config import PUBLIC, ErreurConfig, charger_config
from rag.recherche import chercher

CONF = charger_config(Path("config/rag.yaml"))
M = CONF.indexation.modele_embedding


def avec_rerank(actif=True, candidats=30, garder=8):
    rr = CONF.recherche.rerank.model_copy(update={"actif": actif, "candidats": candidats,
                                                  "garder": garder})
    return CONF.recherche.model_copy(update={"rerank": rr})


class FauxReranker:  # note 1.0 un texte qui contient « cidre », 0.1 sinon
    vu = None

    def noter(self, q, textes):
        self.vu = len(textes)
        return [1.0 if "cidre" in t else 0.1 for t in textes]


def test_rerank_reordonne_et_garde(bd, index, faux_modeles):
    rr = FauxReranker()
    r = chercher(bd, "souper", avec_rerank(candidats=30, garder=3), faux_modeles, index.version,
                 M, niveaux=PUBLIC, reranker=rr, limite=10)
    assert len(r) <= 3 and "cidre" in r[0].texte and r[0].rerank == 1.0 and rr.vu <= 30
    assert [x.rerank for x in r] == sorted((x.rerank for x in r), reverse=True)


def test_rerank_garde_au_plus_limite(bd, index, faux_modeles):
    r = chercher(bd, "souper", avec_rerank(garder=8), faux_modeles, index.version, M,
                 niveaux=PUBLIC, reranker=FauxReranker(), limite=2)
    assert len(r) == 2


def test_rerank_inactif_ne_note_rien(bd, index, faux_modeles):
    rr = FauxReranker()
    r = chercher(bd, "souper", avec_rerank(actif=False), faux_modeles, index.version, M,
                 niveaux=PUBLIC, reranker=rr)
    assert r and all(x.rerank is None for x in r) and rr.vu is None


def test_rerank_actif_sans_reranker_leve(bd, index, faux_modeles):
    with pytest.raises(ErreurConfig):
        chercher(bd, "x", avec_rerank(), faux_modeles, index.version, M, niveaux=PUBLIC)


def test_le_modele_n_est_pas_charge_a_l_import():
    code = "import rag.rerank, sys; assert 'torch' not in sys.modules"
    subprocess.run([sys.executable, "-c", code], check=True)


def test_reranker_depuis(tmp_path):
    from rag.config import Reglages
    from rag.rerank import reranker_depuis
    reg = Reglages(database_url="x", litellm_url="x", donnees_dir=tmp_path)
    assert reranker_depuis(CONF, reg) is None
    c = CONF.model_copy(update={"recherche": avec_rerank()})
    rr = reranker_depuis(c, reg)
    assert rr.modele == "BAAI/bge-reranker-v2-m3" and rr.cache == tmp_path / "modeles"
    assert rr.noter("q", []) == []


@pytest.mark.lent  # télécharge ~2 Go la première fois ; hors CI
def test_vrai_reranker_en_francais():
    from rag.rerank import Reranker
    s = Reranker("BAAI/bge-reranker-v2-m3").noter(
        "Quelle recette contient du poulet ?",
        ["Poulet cidre et Boursin : 2 poitrines de poulet, cidre de pomme.",
         "Tableau de bord Grafana du homelab."])
    assert 0 <= s[1] < s[0] <= 1


def test_bonus_de_source(bd, index, index_code, faux_modeles):
    conf = CONF.recherche.model_copy(update={"bonus_source": 10.0})
    q = "déployer le service guide poulet"
    sans = chercher(bd, q, conf, faux_modeles, index.version, M, niveaux=PUBLIC)
    avec = chercher(bd, q, conf, faux_modeles, index.version, M, niveaux=PUBLIC,
                    bonus_sources=["coffre-maison"])
    assert avec[0].source == "coffre-maison"
    assert {r.source for r in avec} == {r.source for r in sans} | {r.source for r in avec}
    nul = chercher(bd, q, CONF.recherche, faux_modeles, index.version, M, niveaux=PUBLIC,
                   bonus_sources=["coffre-maison"])
    assert [r.cle for r in nul] == [r.cle for r in sans]  # bonus 0 : rien ne change
