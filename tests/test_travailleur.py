import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import yaml
from fastapi.testclient import TestClient

from rag import travailleur
from rag.api import create_app
from rag.config import Config, Reglages, charger_config
from rag.db import connexion
from rag.indexeur import CoffrePartielError
from rag.sources import LecteurGit
from rag.sources_gerees import modifier
from rag.travailleur import fermer_interrompues, menage_dossiers, passe, prochaine, tour
from tests.conftest import NOTES_TEST, URL, commit, git, inscrire_source

BASE = charger_config(Path("config/rag.yaml"))


def compte(bd, table):
    return bd.execute(f"SELECT count(*) n FROM {table}").fetchone()["n"]


def ecrire(dossier: Path, notes: dict) -> Path:
    for chemin, texte in notes.items():
        (dossier / chemin).parent.mkdir(parents=True, exist_ok=True)
        (dossier / chemin).write_text(texte)
    return dossier


def config_pour(*dossiers: tuple[str, Path]) -> Config:
    return BASE.model_copy(update={"sources": [
        BASE.sources[0].model_copy(update={"nom": nom, "emplacement": str(d), "repos_minutes": 10})
        for nom, d in dossiers]})


@pytest.fixture
def reglages(tmp_path):
    return Reglages(database_url=URL, litellm_url="http://x", donnees_dir=tmp_path / "donnees")


@pytest.fixture
def coffre(tmp_path):
    return ecrire(tmp_path / "coffre", dict(list(NOTES_TEST.items())[:4]))


def test_prochaine():
    mtl = ZoneInfo("America/Montreal")

    def j(h, m=0):
        return datetime(2026, 11, 22, h, m, tzinfo=mtl)
    assert prochaine(j(2, 59), None, []) == ("incrementale", None)
    assert prochaine(j(3, 1), j(3, 0) - timedelta(days=1), []) == ("nocturne", None)
    assert prochaine(j(15), j(3, 2), []) == ("incrementale", None)
    assert prochaine(j(3, 1), None, ["coffre-maison"]) == ("demandee", ["coffre-maison"])


def test_passe_enregistre_et_isole_les_erreurs(bd, tmp_path, coffre, reglages, faux_modeles,
                                               monkeypatch):
    b2 = ecrire(tmp_path / "b", {"x.md": "# X\n\nun\n", "y.md": "# Y\n\ndeux\n"})
    conf = config_pour(("source-a", coffre), ("source-b", b2))
    vrai = travailleur.indexer_source

    def indexer(conn, src, *a, **k):
        if src.nom == "source-a":
            raise CoffrePartielError("coffre partiel")
        return vrai(conn, src, *a, **k)
    monkeypatch.setattr(travailleur, "indexer_source", indexer)
    passe(bd, "demandee", conf, reglages, faux_modeles)
    p = bd.execute("SELECT * FROM passes").fetchone()
    assert p["ok"] is False and p["fin"] is not None and p["genre"] == "demandee"
    assert "erreur" in p["bilan"]["source-a"] and p["bilan"]["source-b"]["nouveaux"] == 2


def test_passe_demandee_ignore_le_repos(bd, coffre, reglages, faux_modeles):
    conf = config_pour(("coffre-maison", coffre))
    b = passe(bd, "incrementale", conf, reglages, faux_modeles)
    assert b["coffre-maison"]["reportes"] == 4 and b["coffre-maison"]["nouveaux"] == 0
    b = passe(bd, "demandee", conf, reglages, faux_modeles)
    assert b["coffre-maison"]["nouveaux"] == 4


def test_deux_passes_en_meme_temps(bd, coffre, reglages, faux_modeles):
    with connexion(URL) as bd2:
        bd2.execute("SELECT pg_advisory_lock(7241)")
        conf = config_pour(("coffre-maison", coffre))
        assert passe(bd, "incrementale", conf, reglages, faux_modeles) == {
            "ignoree": "passe en cours"}
        assert compte(bd, "passes") == 0
        bd2.execute("SELECT pg_advisory_unlock(7241)")
    assert "coffre-maison" in passe(bd, "incrementale", conf, reglages, faux_modeles)


def test_verrou_libere_apres_une_passe(bd, coffre, reglages, faux_modeles):
    passe(bd, "demandee", config_pour(("coffre-maison", coffre)), reglages, faux_modeles)
    with connexion(URL) as bd2:
        assert bd2.execute("SELECT pg_try_advisory_lock(7241) ok").fetchone()["ok"]


def test_demande_arrivee_pendant_la_passe_n_est_pas_perdue(bd, coffre, reglages, faux_modeles,
                                                          monkeypatch):
    conf = config_pour(("coffre-maison", coffre))
    passe(bd, "demandee", conf, reglages, faux_modeles)
    bd.execute("UPDATE sources SET demande_le = now()")
    bd.commit()
    vraie = travailleur.passe
    vu = []

    def pendant(conn, genre, *a, **k):
        vu.append(genre)
        time.sleep(0.01)
        with connexion(URL) as autre:  # nouvelle demande pendant la passe
            autre.execute("UPDATE sources SET demande_le = now()")
            autre.commit()
        return vraie(conn, genre, *a, **k)
    monkeypatch.setattr(travailleur, "passe", pendant)
    tour(bd, conf, reglages, faux_modeles)
    assert vu == ["demandee"]
    assert bd.execute("SELECT demande_le FROM sources").fetchone()["demande_le"] is not None
    monkeypatch.setattr(travailleur, "passe", vraie)
    tour(bd, conf, reglages, faux_modeles)
    assert bd.execute("SELECT demande_le FROM sources").fetchone()["demande_le"] is None


def test_passe_interrompue_fermee_au_demarrage(bd):
    bd.execute("INSERT INTO passes (genre) VALUES ('incrementale')")
    bd.commit()
    assert fermer_interrompues(bd) == 1
    p = bd.execute("SELECT * FROM passes").fetchone()
    assert p["ok"] is False and p["fin"] and p["bilan"]["erreur"] == "interrompue"


def test_nocturne_lance_la_semantique(bd, coffre, reglages, faux_modeles, monkeypatch):
    appels = []
    monkeypatch.setattr(travailleur, "passe_semantique",
                        lambda *a, **k: appels.append(1) or {"notes": 0})
    conf = config_pour(("coffre-maison", coffre))
    passe(bd, "incrementale", conf, reglages, faux_modeles)
    passe(bd, "demandee", conf, reglages, faux_modeles)
    assert appels == []
    b = passe(bd, "nocturne", conf, reglages, faux_modeles)
    assert appels == [1] and b["semantique"] == {"notes": 0}


# --- API --------------------------------------------------------------------------------------

auth = {"Authorization": "Bearer pub"}
auth_reindexer = {"Authorization": "Bearer reindex"}


@pytest.fixture
def cfg(tmp_path, coffre):
    d = yaml.safe_load(Path("config/rag.yaml").read_text())
    d["sources"] = [{"nom": "coffre-maison", "type": "obsidian", "visibilite": "public-maison",
                     "emplacement": str(coffre)}]
    f = tmp_path / "rag.yaml"
    f.write_text(yaml.safe_dump(d))
    return f


def faire(cfg, faux_modeles, **k):
    r = Reglages(database_url=URL, litellm_url="http://x", jeton="pub", config=cfg, **k)
    return TestClient(create_app(r, faux_modeles))


@pytest.fixture
def client(bd, cfg, faux_modeles):
    return faire(cfg, faux_modeles, jeton_reindexer="reindex")


def test_reindexer_exige_son_jeton(client, bd, cfg, reglages, faux_modeles):
    passe(bd, "demandee", charger_config(cfg), reglages, faux_modeles)
    assert client.post("/reindexer", json={}, headers=auth).status_code == 403
    assert client.post("/reindexer", json={}).status_code == 401
    assert client.post("/reindexer", json={},
                       headers={"Authorization": "Bearer faux"}).status_code == 401
    r = client.post("/reindexer", json={"source": "coffre-maison"}, headers=auth_reindexer)
    assert r.status_code == 202 and r.json() == {"demande": 1}
    assert bd.execute("SELECT demande_le FROM sources").fetchone()["demande_le"]
    r = client.post("/reindexer", json={"source": "inconnue"}, headers=auth_reindexer)
    assert r.status_code == 202 and r.json() == {"demande": 0}  # pas d'oracle des noms
    assert client.post("/reindexer", json={}, headers=auth_reindexer).json() == {"demande": 1}
    assert client.post("/chercher", json={"question": "x"},
                       headers=auth_reindexer).status_code == 401  # ce jeton ne lit rien


def test_reindexer_sans_jeton_configure_503(bd, cfg, faux_modeles):
    c = faire(cfg, faux_modeles)
    assert c.post("/reindexer", json={}, headers=auth).status_code == 503


def test_etat_signale_la_fin_de_la_nocturne(client, bd, cfg, reglages, faux_modeles,
                                            monkeypatch):
    monkeypatch.setattr(travailleur, "passe_semantique", lambda *a, **k: {})
    assert client.get("/etat").status_code == 401
    e = client.get("/etat", headers=auth).json()
    assert e["derniere_nocturne"] is None and e["en_cours"] is None
    passe(bd, "nocturne", charger_config(cfg), reglages, faux_modeles)
    e = client.get("/etat", headers=auth).json()
    assert e["derniere_nocturne"]["ok"] is True and e["derniere_nocturne"]["fin"]
    assert e["en_cours"] is None and e["sources"][0]["documents"] == 4
    assert e["sources"][0]["demande_en_attente"] is False


def test_etat_cache_les_sources_privees(client, bd, cfg, reglages, faux_modeles, monkeypatch):
    monkeypatch.setattr(travailleur, "passe_semantique", lambda *a, **k: {})
    passe(bd, "nocturne", charger_config(cfg), reglages, faux_modeles)
    bd.execute("UPDATE sources SET visibilite = 'prive'")
    bd.commit()
    e = client.get("/etat", headers=auth).json()
    assert e["sources"] == [] and "coffre-maison" not in e["derniere_nocturne"]["bilan"]


def test_etat_en_cours(client, bd):
    bd.execute("INSERT INTO passes (genre) VALUES ('demandee')")
    bd.commit()
    assert client.get("/etat", headers=auth).json()["en_cours"]["genre"] == "demandee"


BILAN = {"coffre-maison": {"nouveaux": 1}, "secret-supprime": {"supprimee": True},
         "prive": {"erreur": "echec", "x": 1}, "semantique": {"traites": 3, "liens": 1, "echecs": 0},
         "erreur": "interrompue"}


def test_bilan_visible_liste_blanche():
    from rag.api import bilan_visible

    b = BILAN | {"pub": {"nouveaux": 1}}
    assert bilan_visible(b, {"pub"}, complet=False) == {"pub": {"nouveaux": 1},
                                                        "erreur": "interrompue"}
    assert bilan_visible(b, {"pub"}, complet=True) == {
        "pub": {"nouveaux": 1}, "erreur": "interrompue", "semantique": BILAN["semantique"]}
    for complet in (False, True):
        r = bilan_visible(b | {"semantique": {"erreur": "boum /srv/x"}}, {"pub"}, complet=complet)
        assert r["semantique"] == {"erreur": "echec"}
    assert bilan_visible(b, {"prive"}, complet=False)["prive"] == {"erreur": "echec"}
    # une source privée nommée « erreur » : son dict n'est jamais rendu
    assert "erreur" not in bilan_visible({"erreur": {"nouveaux": 2}}, {"pub"}, complet=True)


def test_etat_ne_nomme_pas_une_source_supprimee(client, bd, cfg, faux_modeles):
    from psycopg.types.json import Jsonb

    inscrire_source(bd, "coffre-maison")
    bd.execute("INSERT INTO passes (genre, fin, ok, bilan) VALUES ('nocturne', now(), true, %s)",
               (Jsonb(BILAN),))
    bd.commit()
    r = client.get("/etat", headers=auth)
    assert "secret-supprime" not in r.text
    assert set(r.json()["derniere_nocturne"]["bilan"]) == {"coffre-maison", "erreur"}
    r = faire(cfg, faux_modeles, jeton_prive="priv").get(
        "/etat", headers={"Authorization": "Bearer priv"})
    assert "secret-supprime" not in r.text and "semantique" in r.json()["derniere_nocturne"]["bilan"]




def test_etat_n_expose_aucun_message_d_exception(client, bd, cfg, reglages, faux_modeles,
                                                 monkeypatch):
    def boum(*a, **k):
        raise RuntimeError("échec sur /srv/secret/chemin https://user:mdp@hote.interne/x")
    monkeypatch.setattr(travailleur, "passe_semantique", boum)
    monkeypatch.setattr(travailleur, "indexer_source", boum)
    passe(bd, "nocturne", charger_config(cfg), reglages, faux_modeles)
    brut = client.get("/etat", headers=auth).text
    assert "/srv/secret" not in brut and "hote.interne" not in brut and "RuntimeError" not in brut
    e = client.get("/etat", headers=auth).json()["derniere_nocturne"]
    assert e["ok"] is False and e["bilan"]["coffre-maison"] == {"erreur": "echec"}
    assert "/srv/secret" not in str(bd.execute("SELECT bilan FROM passes").fetchone())


def test_echec_du_graphe_sans_message(bd, coffre, reglages, faux_modeles, monkeypatch):
    monkeypatch.setattr(travailleur, "maj_graphe", lambda *a, **k: "graphe : ÉCHEC /srv/secret")
    passe(bd, "demandee", config_pour(("coffre-maison", coffre)), reglages, faux_modeles)
    p = bd.execute("SELECT * FROM passes").fetchone()
    assert p["ok"] is False and p["bilan"]["coffre-maison"]["erreur"] == "echec"
    assert "/srv/secret" not in str(p["bilan"])


def test_passe_ferme_les_orphelines(bd, coffre, reglages, faux_modeles):
    bd.execute("INSERT INTO passes (genre) VALUES ('incrementale')")
    bd.commit()
    passe(bd, "demandee", config_pour(("coffre-maison", coffre)), reglages, faux_modeles)
    assert compte(bd, "passes") == 2 and bd.execute(
        "SELECT count(*) n FROM passes WHERE fin IS NULL").fetchone()["n"] == 0


def test_demande_d_une_autre_source_non_servie_reste(bd, tmp_path, coffre, reglages,
                                                     faux_modeles):
    b2 = ecrire(tmp_path / "b", {"x.md": "# X\n\nun\n"})
    conf = config_pour(("coffre-maison", coffre), ("source-b", b2))
    passe(bd, "demandee", conf, reglages, faux_modeles)
    bd.execute("UPDATE sources SET demande_le = now() WHERE nom = 'coffre-maison'")
    bd.commit()
    vraie = travailleur.prochaine
    try:  # le tour ne sert que coffre-maison ; source-b demandée « pendant » reste
        travailleur.prochaine = lambda m, d, dem: (bd.execute(
            "UPDATE sources SET demande_le = now() - interval '1 hour' WHERE nom = 'source-b'"),
            bd.commit(), vraie(m, d, dem))[2]
        tour(bd, conf, reglages, faux_modeles)
    finally:
        travailleur.prochaine = vraie
    restes = [r["nom"] for r in bd.execute("SELECT nom FROM sources WHERE demande_le IS NOT NULL")]
    assert restes == ["source-b"]


def test_boucle_rouvre_l_ecoute_apres_une_erreur(bd, reglages, monkeypatch):
    ouvertures, tours = [], []

    class Ecoute:
        closed = False

        def __init__(self):
            ouvertures.append(self)

        def notifies(self, timeout, stop_after=None):
            if len(ouvertures) == 1:
                raise OSError("connexion perdue")
            return iter(())

        def close(self):
            self.closed = True
    monkeypatch.setattr(travailleur, "_ecouter", lambda url: Ecoute())
    monkeypatch.setattr(travailleur, "tour", lambda *a: tours.append(1) or {})
    travailleur.boucle(reglages, BASE, None, tours=2, pause_s=0)
    assert len(ouvertures) == 2 and ouvertures[0].closed and tours == [1]


def test_indexer_cli_respecte_le_verrou(bd, coffre, reglages, faux_modeles):
    from rag.cli import _indexer
    with connexion(URL) as bd2:
        bd2.execute("SELECT pg_advisory_lock(7241)")
        assert _indexer(bd, reglages, config_pour(("coffre-maison", coffre)), faux_modeles) == 3
        bd2.execute("SELECT pg_advisory_unlock(7241)")
    assert _indexer(bd, reglages, config_pour(("coffre-maison", coffre)), faux_modeles,
                    complet=True) == 0


def test_menage_dossiers(tmp_path):
    for d in ("clones/a", "clones/b", "graphe/b", "graphe/a"):
        (tmp_path / d).mkdir(parents=True)
    (tmp_path / "clones" / "fichier").write_text("x")
    (tmp_path / "autre").mkdir()
    assert menage_dossiers(tmp_path, {"a"}) == ["b"]
    assert (tmp_path / "clones/a").exists() and (tmp_path / "graphe/a").exists()
    assert not (tmp_path / "clones/b").exists() and not (tmp_path / "graphe/b").exists()
    assert (tmp_path / "clones" / "fichier").exists() and (tmp_path / "autre").exists()


def _brancher_git_local(monkeypatch):
    monkeypatch.setattr(travailleur, "lecteur_pour", lambda s, r, j=None, u="x": LecteurGit(
        s.emplacement, Path(r.donnees_dir) / "clones" / s.nom, s.branche))
    monkeypatch.setattr(travailleur, "maj_graphe", lambda *a, **k: "ok")


def test_passe_source_supprimee_sans_erreur(bd, coffre, reglages, faux_modeles, monkeypatch):
    conf = config_pour(("coffre-maison", coffre))
    seule = conf.model_copy(update={"sources": []})
    inscrire_source(bd, "fantome")
    (reglages.donnees_dir / "clones" / "orpheline").mkdir(parents=True)
    _brancher_git_local(monkeypatch)
    vrai = travailleur.sources_actives
    actives = vrai(bd, seule, None)
    bd.execute("DELETE FROM sources WHERE nom = 'fantome'")
    bd.commit()  # supprimée entre la lecture de la liste et son tour
    monkeypatch.setattr(travailleur, "sources_actives", lambda *a: actives)
    b = passe(bd, "demandee", seule, reglages, faux_modeles)
    assert b["fantome"] == {"supprimee": True}
    assert bd.execute("SELECT ok FROM passes").fetchone()["ok"] is True
    assert compte(bd, "sources") == 0
    assert not (reglages.donnees_dir / "clones" / "orpheline").exists()


def test_sans_garde_apres_changement_d_url(bd, depot, tmp_path, reglages, faux_modeles,
                                           monkeypatch):
    _brancher_git_local(monkeypatch)
    conf = BASE.model_copy(update={"sources": []})
    inscrire_source(bd, "depot-x", emplacement=str(depot))
    passe(bd, "demandee", conf, reglages, faux_modeles)
    avant = compte(bd, "documents")
    assert avant > 1
    d2 = tmp_path / "depot2"
    d2.mkdir()
    git(d2, "init", "-q", "-b", "main")
    git(d2, "config", "user.email", "t@t")
    git(d2, "config", "user.name", "t")
    commit(d2, "seul.py", "def seul():\n    return 1\n")
    from rag.config import SourceConf
    nouvelle = SourceConf(visibilite="public-maison", nom="depot-x", type="code",
                          emplacement=str(d2), branche="main", exclure=[])
    changes = modifier(bd, "depot-x", nouvelle, utilisateur="x-access-token", jeton=None,
                       retirer_jeton=False, coffret=None)
    assert "emplacement" in changes
    assert bd.execute("SELECT sans_garde FROM sources").fetchone()["sans_garde"] is True
    b = tour(bd, conf, reglages, faux_modeles)
    assert "erreur" not in b["depot-x"]
    assert compte(bd, "documents") == 1
    r = bd.execute("SELECT sans_garde, demande_le FROM sources").fetchone()
    assert r["sans_garde"] is False and r["demande_le"] is None


def test_cle_etrangere_en_cours_de_passe_est_une_suppression(bd, coffre, reglages, faux_modeles,
                                                             monkeypatch, caplog):
    import psycopg.errors
    conf = config_pour(("coffre-maison", coffre)).model_copy(update={"sources": []})
    inscrire_source(bd, "fantome")
    _brancher_git_local(monkeypatch)

    def casse(*a, **k):
        raise psycopg.errors.ForeignKeyViolation("documents_source_id_fkey")
    monkeypatch.setattr(travailleur, "indexer_source", casse)
    b = passe(bd, "demandee", conf, reglages, faux_modeles)
    assert b["fantome"] == {"supprimee": True}
    assert "passe en échec" not in caplog.text
    assert bd.execute("SELECT ok FROM passes").fetchone()["ok"] is True


def test_sans_garde_conserve_apres_une_passe_en_echec(bd, depot, reglages, faux_modeles,
                                                       monkeypatch):
    _brancher_git_local(monkeypatch)
    conf = BASE.model_copy(update={"sources": []})
    inscrire_source(bd, "depot-x", emplacement=str(depot))
    inscrire_source(bd, "depot-y", emplacement=str(depot))
    bd.execute("UPDATE sources SET demande_le = now(), sans_garde = true")
    bd.commit()
    vrai = travailleur.indexer_source

    def selon(conn, src, *a, **k):
        if src.nom == "depot-x":
            raise RuntimeError("boum")
        return vrai(conn, src, *a, **k)
    monkeypatch.setattr(travailleur, "indexer_source", selon)
    b = tour(bd, conf, reglages, faux_modeles)
    assert b["depot-x"] == {"erreur": "echec"} and "erreur" not in b["depot-y"]
    lignes = {r["nom"]: r for r in bd.execute("SELECT nom, sans_garde, demande_le FROM sources")}
    assert lignes["depot-x"]["sans_garde"] is True and lignes["depot-x"]["demande_le"] is None
    assert lignes["depot-y"]["sans_garde"] is False
