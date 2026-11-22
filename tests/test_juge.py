import json
from pathlib import Path

import pytest

from rag.banc.execution import Cas
from rag.banc.juge import ecrire_relecture, executer_generation, juger
from rag.config import ErreurConfig, charger_config
from rag.refus import REFUS
from tests.test_cli import env_cli  # noqa: F401 - fixture réutilisée

BASE = charger_config(Path("config/rag.yaml"))
CONFIG = BASE.model_copy(update={"reponse": BASE.reponse.model_copy(
    update={"seuil_refus_cosinus": None, "seuil_refus_rerank": None})})


class Scripte:
    """Embeddings factices ; génération selon la question ; juge selon le modèle."""

    def __init__(self, base, juge=('{"fidelite": 5, "pertinence": 4}',
                                   '{"fidelite": 4, "pertinence": 4}')):
        self.base, self.juge, self.appels_juge = base, list(juge), []

    def embed_requete(self, *a):
        return self.base.embed_requete(*a)

    def embed_documents(self, *a):
        return self.base.embed_documents(*a)

    def generer(self, modele, messages, temperature, json_mode=False):
        if modele == CONFIG.banc.modele_juge:
            self.appels_juge.append(modele)
            return self.juge.pop(0)
        if json_mode:
            return '{"mode": "repondre"}'
        return REFUS if "bitcoin" in messages[-1]["content"] else "Du cidre [1]."


def test_juger_lit_le_json_et_borne():
    class G:
        def __init__(self, t):
            self.t = t

        def generer(self, modele, messages, temperature, json_mode=False):
            assert temperature == 0 and json_mode
            return self.t
    assert juger(G('{"fidelite": 5, "pertinence": 4}'), "j", "q", ["p"], "r") == {
        "fidelite": 5, "pertinence": 4}
    for t in ('{"fidelite": 7, "pertinence": 4}', '{"fidelite": "x", "pertinence": 4}',
              "texte libre", '{"fidelite": true, "pertinence": 4}'):
        assert juger(G(t), "j", "q", ["p"], "r") == {"fidelite": None, "pertinence": None}


def test_le_juge_n_est_pas_le_generateur(bd, faux_modeles):
    meme = CONFIG.model_copy(update={"banc": CONFIG.banc.model_copy(
        update={"modele_juge": CONFIG.reponse.modele})})
    with pytest.raises(ErreurConfig):
        executer_generation(bd, [], meme, faux_modeles, 1)


CAS = [Cas(id="a", question="Quelle est la recette du poulet au cidre ?", nature="factuel",
           attendus=["x:y"], mode_attendu="repondre"),
       Cas(id="b", question="Comment faire le pâté chinois ?", nature="synthetiser",
           attendus=["x:y"]),
       Cas(id="c", question="Quel est le prix du bitcoin ?", nature="sans_reponse"),
       Cas(id="d", question="Quelles recettes j'ai ?", nature="lister", attendus=["x:y"],
           mode_attendu="lister")]


def test_resume_de_generation(bd, index, faux_modeles):
    m = Scripte(faux_modeles)
    r = executer_generation(bd, CAS, CONFIG, m, index.version)
    s = r["resume"]
    assert s["refus_justes"] == 1.0 and s["faux_refus"] == 0.0 and s["fidelite_moy"] == 4.5
    assert s["pertinence_moy"] == 4.0 and s["exactitude_mode"] == 1.0
    assert s["citations_invalides"] == 0 and s["juges_illisibles"] == 0
    assert set(m.appels_juge) == {"qwen3.8:27b-mlx"} and len(m.appels_juge) == 2
    par_id = {c["id"]: c for c in r["cas"]}
    assert par_id["c"]["raison"] == "modele_refuse" and par_id["d"]["mode"] == "lister"
    assert par_id["a"]["passages"] and par_id["a"]["reponse"] == "Du cidre [1]."


def test_fichier_de_relecture(tmp_path, bd, index, faux_modeles):
    r = executer_generation(bd, CAS, CONFIG, Scripte(faux_modeles), index.version)
    f = ecrire_relecture(r, tmp_path / "x-relecture.md", 3)
    t = f.read_text()
    assert t.count("[ ] d'accord avec le juge") == 3 and t.count("\n## ") == 3
    assert ecrire_relecture(r, tmp_path / "y.md", 3).read_text() == t  # graine fixe
    json.dumps(r)  # sérialisable


def test_cli_banc_generation(env_cli, tmp_path, monkeypatch, faux_modeles):  # noqa: F811
    from rag import cli
    from tests.test_juge import Scripte
    monkeypatch.setattr(cli, "modeles_depuis", lambda r: Scripte(faux_modeles, juge=[
        '{"fidelite": 5, "pertinence": 5}'] * 10))
    jeu = tmp_path / "jeu.yaml"
    jeu.write_text("- {id: a, question: 'Comment faire le poulet au cidre ?', nature: factuel, "
                   "attendus: ['x:y']}\n")
    monkeypatch.chdir(tmp_path)
    assert cli.main(["indexer"]) == 0
    assert cli.main(["banc", "--jeu", str(jeu), "--generation", "--sans-indexer",
                     "--echantillon", "1"]) == 0
    assert list((tmp_path / "banc/resultats").glob("*-relecture.md"))


def test_une_panne_du_modele_n_arrete_pas_le_banc(bd, index, faux_modeles):
    from rag.modeles import ErreurModele

    class Lent(Scripte):
        def generer(self, modele, messages, temperature, json_mode=False):
            if modele != CONFIG.banc.modele_juge and not json_mode and "pâté" in str(messages):
                raise ErreurModele("ReadTimeout")
            return super().generer(modele, messages, temperature, json_mode)
    r = executer_generation(bd, CAS, CONFIG, Lent(faux_modeles), index.version)
    assert r["resume"]["erreurs_modele"] == 1 and r["resume"]["n"] == 4
    assert r["resume"]["fidelite_moy"] == 5.0
