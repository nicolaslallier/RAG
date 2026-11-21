import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

from rag import cli
from rag.config import SourceConf
from rag.graphe import ErreurGraphe, charger_graphe, depuis_graphify, extraire_structure, maj_graphe

SRC_CODE = SourceConf(visibilite="public-maison", nom="depot-test", type="code", emplacement="x")
G = {"noeuds": [{"cle": "a.py", "libelle": "a.py", "type": "fichier", "fichier": "a.py"},
                {"cle": "a.py::aide", "libelle": "aide", "type": "fonction", "fichier": "a.py",
                 "ligne": 3, "communaute": 0},
                {"cle": "x::autre", "libelle": "autre", "type": "fonction", "fichier": None}],
     "liens": [{"de": "a.py", "vers": "a.py::aide", "type": "contains", "confiance": "EXTRAIT"},
               {"de": "a.py::aide", "vers": "x::autre", "type": "calls", "confiance": "INFERE"}]}
G_SANS_UN_NOEUD = {"noeuds": G["noeuds"][:2], "liens": G["liens"][:1]}


def compte(bd, table):
    return bd.execute(f"SELECT count(*) n FROM {table}").fetchone()["n"]


def test_depuis_graphify_sur_la_sortie_reelle():
    g = depuis_graphify(json.loads(Path("tests/fixtures/graphify/graph.json").read_text()))
    assert g["noeuds"] and g["liens"]
    assert {lk["confiance"] for lk in g["liens"]} <= {"EXTRAIT", "INFERE", "AMBIGU"}
    cles = {n["cle"] for n in g["noeuds"]}
    assert all(lk["de"] in cles and lk["vers"] in cles for lk in g["liens"])
    assert any(n["fichier"] for n in g["noeuds"])
    bc = next(n for n in g["noeuds"] if n["libelle"] == "BancConf")
    assert (bc["type"], bc["ligne"], bc["fichier"]) == ("classe", 101, "rag/config.py")


def test_depuis_graphify_confiances_et_liens_orphelins():
    g = depuis_graphify({"nodes": [{"id": "a", "label": "a"}, {"id": "b", "label": "b"}],
                         "links": [{"source": "a", "target": "b", "relation": "calls",
                                    "confidence": "AMBIGUOUS"},
                                   {"source": "a", "target": "b", "relation": "uses"},
                                   {"source": "a", "target": "zz", "relation": "calls"}]})
    assert [lk["confiance"] for lk in g["liens"]] == ["AMBIGU", "EXTRAIT"]


def test_charger_relie_aux_documents(bd, index_code):
    assert charger_graphe(bd, "depot-test", G) == (3, 2)
    assert bd.execute("SELECT d.chemin FROM noeuds n JOIN documents d ON d.id=n.document_id "
                      "WHERE n.cle='a.py::aide'").fetchone()["chemin"] == "a.py"


def test_recharger_remplace_sans_doublon(bd, index_code):
    charger_graphe(bd, "depot-test", G)
    charger_graphe(bd, "depot-test", G_SANS_UN_NOEUD)
    assert compte(bd, "noeuds") == 2 and compte(bd, "liens") == 1


def test_recharger_la_structure_garde_les_autres_origines(bd, index_code):
    charger_graphe(bd, "depot-test", G)
    charger_graphe(bd, "depot-test", {"noeuds": G["noeuds"][1:], "liens": [
        {"de": "x::autre", "vers": "a.py::aide", "type": "lien", "confiance": "EXTRAIT"}]},
        origine="wikilien")
    charger_graphe(bd, "depot-test", G_SANS_UN_NOEUD)
    assert {r["origine"] for r in bd.execute("SELECT origine FROM liens")} == {
        "structure", "wikilien"}
    assert compte(bd, "noeuds") == 3


def test_document_supprime_garde_le_noeud_sans_lien_mort(bd, index_code):
    charger_graphe(bd, "depot-test", G)
    bd.execute("DELETE FROM documents WHERE chemin = 'a.py'")
    bd.commit()
    assert bd.execute("SELECT document_id FROM noeuds WHERE cle='a.py::aide'"
                      ).fetchone()["document_id"] is None


REGLAGES = SimpleNamespace(donnees_dir=Path("/tmp/inutilise"), graphify_commande="graphify")


def test_echec_de_l_extracteur_garde_l_ancien_graphe(bd, index_code, monkeypatch):
    charger_graphe(bd, "depot-test", G)

    def tombe(*a, **k):
        raise ErreurGraphe("délai dépassé")
    monkeypatch.setattr("rag.graphe.extraire_structure", tombe)
    bilan = maj_graphe(bd, SRC_CODE, index_code, REGLAGES)
    assert "ÉCHEC" in bilan and "délai dépassé" in bilan and compte(bd, "noeuds") == 3


def test_graphe_recharge_seulement_si_la_revision_change(bd, index_code, monkeypatch):
    appels = []
    monkeypatch.setattr("rag.graphe.extraire_structure", lambda *a, **k: appels.append(1) or G)
    assert maj_graphe(bd, SRC_CODE, index_code, REGLAGES) == "graphe : 3 nœuds, 2 liens"
    assert maj_graphe(bd, SRC_CODE, index_code, REGLAGES) == "graphe : à jour" and appels == [1]


class FauxProc:
    def __init__(self, args):
        out = Path(args[-1]) / "graphify-out"
        out.mkdir(parents=True)
        (out / "graph.json").write_text(json.dumps({"nodes": [{"id": "n", "label": "n"}],
                                                    "links": []}))
        self.returncode, self.stderr = 0, b""


def test_extraire_structure_sans_cle_infonuagique_ni_secret(monkeypatch, tmp_path, depot):
    monkeypatch.setenv("GEMINI_API_KEY", "x")
    monkeypatch.setenv("GOOGLE_API_KEY", "x")
    monkeypatch.setenv("RAG_LITELLM_CLE", "x")
    vu = {}

    def faux_run(args, **k):
        vu.update(k)
        vu["fichiers"] = {p.name for p in Path(args[-1]).rglob("*")}
        return FauxProc(args)
    monkeypatch.setattr(subprocess, "run", faux_run)
    g = extraire_structure(depot, tmp_path / "t")
    assert g["noeuds"][0]["cle"] == "n"
    assert not {"GEMINI_API_KEY", "GOOGLE_API_KEY", "RAG_LITELLM_CLE"} & vu["env"].keys()
    assert vu["timeout"] == 600
    assert "a.py" in vu["fichiers"] and not {".env.example", "uv.lock", ".git"} & vu["fichiers"]


def test_extraire_structure_code_non_nul(monkeypatch, tmp_path, depot):
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: SimpleNamespace(returncode=3,
                                                                          stderr=b"boum"))
    try:
        extraire_structure(depot, tmp_path / "t")
        raise AssertionError
    except ErreurGraphe as e:
        assert "boum" in str(e)


def test_rag_indexer_continue_apres_un_echec_de_graphe(bd, depot, tmp_path, monkeypatch,
                                                       faux_modeles, capsys):
    from rag.config import charger_config
    from rag.sources import LecteurGit

    config = charger_config(Path("config/rag.yaml")).model_copy(update={"sources": [
        SRC_CODE, SRC_CODE.model_copy(update={"nom": "depot-2"})]})
    monkeypatch.setattr(cli, "lecteur_pour", lambda s, r: LecteurGit(str(depot),
                                                                      tmp_path / s.nom))
    vus = []

    def extr(clone, *a, **k):
        vus.append(clone)
        if len(vus) == 1:
            raise ErreurGraphe("graphify a planté")
        return G
    monkeypatch.setattr("rag.graphe.extraire_structure", extr)
    assert cli._indexer(bd, REGLAGES, config, faux_modeles) == 0
    out = capsys.readouterr().out
    assert "ÉCHEC graphify a planté" in out and "graphe : 3 nœuds" in out
    assert {r["nom"] for r in bd.execute(
        "SELECT DISTINCT s.nom FROM documents d JOIN sources s ON s.id=d.source_id")} == {
        "depot-test", "depot-2"}
