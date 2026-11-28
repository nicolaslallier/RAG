from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from rag.config import ErreurConfig, Reglages, SourceConf, charger_config
from rag.sources_gerees import Coffret, coffret_depuis, sources_actives
from tests.conftest import URL

BASE = charger_config(Path("config/rag.yaml"))


def _config():
    coffre = BASE.sources[0].model_copy(update={"nom": "coffre-maison"})
    vieux = SourceConf(nom="vieux", type="code", emplacement="https://x/vieux.git",
                       visibilite="public-maison")
    return BASE.model_copy(update={"sources": [vieux, coffre]})


def _inserer(bd, nom, geree, exclure=(), jeton=None):
    bd.execute(
        "INSERT INTO sources (nom, type, emplacement, visibilite, branche, exclure, jeton_chiffre,"
        " geree) VALUES (%s, 'code', %s, 'prive', 'dev', %s, %s, %s)",
        (nom, f"https://x/{nom}.git", list(exclure), jeton, geree))
    bd.commit()


def test_coffret_aller_retour():
    c = Coffret(Fernet.generate_key().decode())
    assert c.dechiffrer(c.chiffrer("ghp_x")) == "ghp_x" and b"ghp_x" not in c.chiffrer("ghp_x")


def test_coffret_depuis_cle_invalide():
    with pytest.raises(ErreurConfig, match="RAG_CLE_SECRETS"):
        coffret_depuis(Reglages(database_url=URL, litellm_url="x", cle_secrets="court"))


def test_coffret_depuis_cle_vide_ou_valide():
    assert coffret_depuis(Reglages(database_url=URL, litellm_url="x")) is None
    cle = Fernet.generate_key().decode()
    assert coffret_depuis(Reglages(database_url=URL, litellm_url="x", cle_secrets=cle)) is not None


def test_sources_actives_coffre_puis_gerees(bd):
    _inserer(bd, "b-depot", True)
    _inserer(bd, "a-depot", True, ["banc/**"])
    _inserer(bd, "hors", False)
    actives = sources_actives(bd, _config(), None)
    assert [s.conf.nom for s in actives] == ["coffre-maison", "a-depot", "b-depot"]
    a = actives[1]
    assert a.conf.exclure == ["banc/**"] and a.conf.type == "code"
    assert a.conf.branche == "dev" and a.conf.visibilite == "prive" and a.utilisateur == "x-access-token"


def test_jeton_dechiffre(bd):
    c = Coffret(Fernet.generate_key().decode())
    _inserer(bd, "a-depot", True, jeton=c.chiffrer("ghp_secret"))
    [s] = sources_actives(bd, _config(), c)[1:]
    assert s.jeton == "ghp_secret"


def test_jeton_illisible_ignore_avec_avertissement(bd, caplog):
    cle_a, cle_b = Fernet.generate_key().decode(), Fernet.generate_key().decode()
    _inserer(bd, "a-depot", True, jeton=Coffret(cle_a).chiffrer("ghp_secret"))
    [s] = [s for s in sources_actives(bd, _config(), Coffret(cle_b)) if s.conf.nom == "a-depot"]
    assert s.jeton is None and "a-depot" in caplog.text and "ghp_" not in caplog.text


def test_jeton_sans_cle_ignore_avec_avertissement(bd, caplog):
    cle = Fernet.generate_key().decode()
    _inserer(bd, "a-depot", True, jeton=Coffret(cle).chiffrer("ghp_secret"))
    [s] = [s for s in sources_actives(bd, _config(), None) if s.conf.nom == "a-depot"]
    assert s.jeton is None and "a-depot" in caplog.text and "ghp_" not in caplog.text


# --- validation, ajout, modification, suppression ---

from rag import gestion
from rag.sources_gerees import (
    JetonImpossible,
    SourceInvalide,
    ajouter,
    bilan_suppression,
    modifier,
    supprimer,
    valider,
)

RESERVES = {"coffre-maison"}
URL_OK = "https://github.com/a/b.git"


def _valider(nom="x", url=URL_OK, branche="main", visi="public-maison", exclure=(),
             utilisateur="x-access-token"):
    return valider(nom, url, branche, visi, list(exclure), utilisateur, noms_reserves=RESERVES)


@pytest.mark.parametrize("nom", ["", ".", "..", "a/b", "coffre-maison", "a b"])
def test_nom_refuse(nom):
    with pytest.raises(SourceInvalide):
        _valider(nom=nom)


@pytest.mark.parametrize("url", ["http://h/x.git", "file:///etc", "ext::sh", "/srv/x",
                                 "https://u:p@h/x.git", "https:///x"])
def test_url_refusee(url):
    with pytest.raises(SourceInvalide):
        _valider(url=url)


def test_url_du_reseau_local_acceptee():
    assert valider("x", "https://192.168.2.10/x.git", "main", "public-maison", [],
                   "x-access-token", noms_reserves=set())


@pytest.mark.parametrize("branche", ["", "-x"])
def test_branche_refusee(branche):
    with pytest.raises(SourceInvalide):
        _valider(branche=branche)


def test_motif_invalide_devient_source_invalide():
    with pytest.raises(SourceInvalide):
        _valider(exclure=["   "])


def test_valider_rend_une_conf_propre():
    c = _valider(exclure=[" a/** "])
    assert c.type == "code" and c.exclure == ["a/**"] and c.emplacement == URL_OK


def _coffret():
    return Coffret(Fernet.generate_key().decode())


def _ligne(bd, nom):
    return bd.execute("SELECT * FROM sources WHERE nom = %s", (nom,)).fetchone()


def test_ajouter_demande_l_indexation(bd):
    ajouter(bd, _valider(nom="neuf"), utilisateur="x-access-token", jeton=None, coffret=None)
    r = _ligne(bd, "neuf")
    assert r["demande_le"] is not None and r["geree"] and r["jeton_chiffre"] is None


def test_ajouter_chiffre_le_jeton(bd):
    c = _coffret()
    ajouter(bd, _valider(nom="neuf"), utilisateur="u", jeton="ghp_secret", coffret=c)
    r = _ligne(bd, "neuf")
    assert c.dechiffrer(r["jeton_chiffre"]) == "ghp_secret" and r["utilisateur"] == "u"


def test_ajouter_nom_deja_pris(bd):
    ajouter(bd, _valider(nom="neuf"), utilisateur="x", jeton=None, coffret=None)
    with pytest.raises(SourceInvalide, match="déjà pris"):
        ajouter(bd, _valider(nom="neuf"), utilisateur="x", jeton=None, coffret=None)


def test_ajouter_jeton_sans_coffret_refuse(bd):
    with pytest.raises(JetonImpossible):
        ajouter(bd, _valider(nom="neuf"), utilisateur="x", jeton="ghp_x", coffret=None)
    assert _ligne(bd, "neuf") is None


def _modif(bd, nom, **kw):
    r = _ligne(bd, nom)
    champs = {"nom": nom, "type": "code", "emplacement": r["emplacement"],
              "branche": r["branche"], "exclure": list(r["exclure"]),
              "visibilite": r["visibilite"]}
    champs.update({k: v for k, v in kw.items() if k in champs})
    args = {"utilisateur": kw.get("utilisateur", r["utilisateur"]), "jeton": kw.get("jeton"),
            "retirer_jeton": kw.get("retirer_jeton", False), "coffret": kw.get("coffret")}
    return modifier(bd, nom, SourceConf(**champs), **args)


def _gerer(bd, nom="g", jeton=None):
    _inserer(bd, nom, True, jeton=jeton)
    bd.execute("UPDATE sources SET demande_le = NULL WHERE nom = %s", (nom,))
    bd.commit()


def test_modifier_sans_jeton_garde_l_ancien(bd):
    _gerer(bd, jeton=b"opaque")
    _modif(bd, "g", branche="autre")
    assert bytes(_ligne(bd, "g")["jeton_chiffre"]) == b"opaque"


def test_modifier_retirer_jeton(bd):
    _gerer(bd, jeton=b"opaque")
    assert "jeton" in _modif(bd, "g", retirer_jeton=True)
    assert _ligne(bd, "g")["jeton_chiffre"] is None


def test_modifier_nouveau_jeton(bd):
    _gerer(bd)
    c = _coffret()
    assert _modif(bd, "g", jeton="ghp_n", coffret=c) == {"jeton"}
    r = _ligne(bd, "g")
    assert c.dechiffrer(r["jeton_chiffre"]) == "ghp_n" and r["demande_le"] is not None


def test_modifier_jeton_sans_coffret_refuse(bd):
    _gerer(bd)
    with pytest.raises(JetonImpossible):
        _modif(bd, "g", jeton="ghp_n", branche="autre")
    assert _ligne(bd, "g")["branche"] == "dev"


def test_modifier_visibilite_seule_ne_reindexe_pas(bd):
    _gerer(bd)
    assert _modif(bd, "g", visibilite="public-maison") == {"visibilite"}
    r = _ligne(bd, "g")
    assert r["demande_le"] is None and r["visibilite"] == "public-maison" and not r["sans_garde"]


def test_modifier_rien_ne_change(bd):
    _gerer(bd)
    assert _modif(bd, "g") == set()
    assert _ligne(bd, "g")["demande_le"] is None


def test_modifier_url_efface_les_documents(bd, index_code):
    compte = "SELECT count(*) AS n FROM {} WHERE source_id = %s"
    sid = _ligne(bd, "depot-test")["id"]
    bd.execute("UPDATE sources SET graphe_revision = 'abc' WHERE id = %s", (sid,))
    bd.commit()
    assert bd.execute(compte.format("documents"), (sid,)).fetchone()["n"] > 0
    ch = _modif(bd, "depot-test", emplacement="https://github.com/a/autre.git")
    assert ch == {"emplacement"}
    for t in ("documents", "noeuds", "exclus"):
        assert bd.execute(compte.format(t), (sid,)).fetchone()["n"] == 0
    r = _ligne(bd, "depot-test")
    assert r["revision"] is None and r["graphe_revision"] is None
    assert r["sans_garde"] and r["demande_le"] is not None


def test_modifier_branche_efface_aussi(bd, index_code):
    sid = _ligne(bd, "depot-test")["id"]
    _modif(bd, "depot-test", branche="dev")
    assert bd.execute("SELECT count(*) AS n FROM documents WHERE source_id = %s",
                      (sid,)).fetchone()["n"] == 0


def test_modifier_exclure_pose_sans_garde(bd):
    _gerer(bd)
    assert _modif(bd, "g", exclure=["*.csv"]) == {"exclure"}
    r = _ligne(bd, "g")
    assert r["sans_garde"] and r["demande_le"] is not None and r["exclure"] == ["*.csv"]


def test_modifier_le_coffre_refuse(bd):
    _inserer(bd, "coffre-maison", False)
    with pytest.raises(gestion.SourceInconnue):
        _modif(bd, "coffre-maison", branche="x")


def test_modifier_source_absente(bd):
    with pytest.raises(gestion.SourceInconnue):
        modifier(bd, "nulle", SourceConf(nom="nulle", type="code", emplacement=URL_OK,
                                         visibilite="prive"),
                 utilisateur="x", jeton=None, retirer_jeton=False, coffret=None)


def _compter(bd, nom):
    sid = bd.execute("SELECT id FROM sources WHERE nom = %s", (nom,)).fetchone()
    q = lambda sql, *a: bd.execute(sql, a).fetchone()["n"]
    return {
        "sources": q("SELECT count(*) AS n FROM sources WHERE nom = %s", nom),
        "documents": q("SELECT count(*) AS n FROM documents WHERE source_id = %s",
                       sid["id"]) if sid else 0,
        "morceaux": q("SELECT count(*) AS n FROM morceaux m JOIN documents d ON d.id = m.document_id"
                      " WHERE d.source_id = %s", sid["id"]) if sid else 0,
        "noeuds": q("SELECT count(*) AS n FROM noeuds WHERE source_id = %s",
                    sid["id"]) if sid else 0,
        "liens": q("SELECT count(*) AS n FROM liens l JOIN noeuds a ON a.id = l.de"
                   " WHERE a.source_id = %s", sid["id"]) if sid else 0,
        "exclus": q("SELECT count(*) AS n FROM exclus WHERE source_id = %s",
                    sid["id"]) if sid else 0,
        "manuelles": q("SELECT count(*) AS n FROM exclusions_manuelles WHERE source = %s", nom),
    }


def test_supprimer_efface_tout(bd, index_code):
    gestion.exclure(bd, "depot-test", "docs/guide.md")
    assert _compter(bd, "depot-test")["documents"] > 0
    supprimer(bd, "depot-test")
    assert set(_compter(bd, "depot-test").values()) == {0}


def test_supprimer_le_coffre_refuse(bd):
    _inserer(bd, "coffre-maison", False)
    with pytest.raises(gestion.SourceInconnue):
        supprimer(bd, "coffre-maison")
    assert _compter(bd, "coffre-maison")["sources"] == 1


def test_bilan_suppression_compte(bd, index_code):
    gestion.exclure(bd, "depot-test", "docs/guide.md")
    b = bilan_suppression(bd, "depot-test")
    assert b["nom"] == "depot-test" and b["emplacement"] == "x"
    assert b["documents"] > 0 and b["exclus"] >= 1 and b["exclusions_manuelles"] == 1


def test_bilan_suppression_source_inconnue(bd):
    _inserer(bd, "coffre-maison", False)
    with pytest.raises(gestion.SourceInconnue):
        bilan_suppression(bd, "coffre-maison")


def test_changement_d_hote_retire_le_jeton(bd):
    _gerer(bd, jeton=b"opaque")
    ch = _modif(bd, "g", emplacement="https://autre.example/x.git")
    assert ch == {"emplacement", "jeton"}
    assert _ligne(bd, "g")["jeton_chiffre"] is None


def test_changement_de_port_retire_le_jeton(bd):
    _gerer(bd, jeton=b"opaque")
    bd.execute("UPDATE sources SET emplacement = 'https://h.example/x.git' WHERE nom = 'g'")
    bd.commit()
    assert "jeton" in _modif(bd, "g", emplacement="https://h.example:8443/x.git")
    assert _ligne(bd, "g")["jeton_chiffre"] is None


def test_changement_d_hote_avec_nouveau_jeton(bd):
    _gerer(bd, jeton=b"opaque")
    c = _coffret()
    ch = _modif(bd, "g", emplacement="https://autre.example/x.git", jeton="ghp_n", coffret=c)
    assert ch == {"emplacement", "jeton"}
    assert c.dechiffrer(_ligne(bd, "g")["jeton_chiffre"]) == "ghp_n"


def test_changement_de_chemin_seul_garde_le_jeton(bd):
    _gerer(bd, jeton=b"opaque")
    ch = _modif(bd, "g", emplacement="https://x/autre.git")
    assert ch == {"emplacement"}
    assert bytes(_ligne(bd, "g")["jeton_chiffre"]) == b"opaque"


# --- export / import ---

import yaml

from rag.sources_gerees import exporter, importer, lire_export


def _nb(bd):
    return bd.execute("SELECT count(*) AS n FROM sources").fetchone()["n"]


def test_export_sans_jeton(bd):
    ajouter(bd, _valider(nom="b", exclure=["*.md"]), utilisateur="u", jeton="ghp_secret",
            coffret=_coffret())
    ajouter(bd, _valider(nom="a"), utilisateur="x-access-token", jeton=None, coffret=None)
    texte = exporter(bd)
    assert "jeton" not in texte and "ghp_secret" not in texte
    donnees = yaml.safe_load(texte)
    assert [d["nom"] for d in donnees] == ["a", "b"]
    assert set(donnees[0]) == {"nom", "emplacement", "branche", "visibilite", "exclure",
                               "utilisateur"}
    assert lire_export(texte) == donnees


def test_export_ignore_les_sources_du_yaml(bd):
    _inserer(bd, "yaml", False)
    assert yaml.safe_load(exporter(bd)) == []


@pytest.mark.parametrize("texte", [
    "- {nom: a, emplacement: https://h/a.git, jeton: x}",
    "- {nom: a, emplacement: file:///etc}",
    ("- {nom: a, emplacement: https://h/a.git, visibilite: prive}\n"
     "- {nom: '..', emplacement: https://h/b.git, visibilite: prive}"),
    ("- {nom: a, emplacement: https://h/a.git, visibilite: prive}\n"
     "- {nom: coffre-maison, emplacement: https://h/b.git, visibilite: prive}"),
    "- {nom: a, emplacement: https://h/a.git}",
    "- {nom: a}",
    "- {emplacement: https://h/a.git}",
    "- pas un dictionnaire",
    "pas une liste",
    "[",
])
def test_import_refuse_tout_ou_rien(bd, texte):
    with pytest.raises(SourceInvalide):
        importer(bd, texte, noms_reserves=RESERVES)
    assert _nb(bd) == 0


def test_import_nomme_l_entree(bd):
    with pytest.raises(SourceInvalide, match="entrée 2"):
        importer(bd, "- {nom: a, emplacement: https://h/a.git, visibilite: prive}\n- {nom: b}",
                 noms_reserves=RESERVES)


def test_import_ajoute_et_modifie(bd):
    ajouter(bd, _valider(nom="vieux"), utilisateur="x-access-token", jeton=None, coffret=None)
    bd.execute("UPDATE sources SET demande_le = NULL")
    bd.commit()
    bilan = importer(bd, "- {nom: vieux, emplacement: https://github.com/a/b.git, branche: dev,"
                         " visibilite: public-maison}\n"
                         "- {nom: neuf, emplacement: https://github.com/a/c.git,"
                         " visibilite: public-maison}",
                     noms_reserves=RESERVES)
    assert bilan == {"ajoutees": 1, "modifiees": 1, "inchangees": 0, "niveaux": []}
    assert _ligne(bd, "vieux")["branche"] == "dev" and _ligne(bd, "vieux")["demande_le"]
    assert _ligne(bd, "neuf")["geree"] and _ligne(bd, "neuf")["demande_le"]


def test_import_inchangees(bd):
    ajouter(bd, _valider(nom="a"), utilisateur="x-access-token", jeton=None, coffret=None)
    bilan = importer(bd, exporter(bd), noms_reserves=RESERVES)
    assert bilan == {"ajoutees": 0, "modifiees": 0, "inchangees": 1, "niveaux": []}


def test_import_garde_le_jeton(bd):
    c = _coffret()
    ajouter(bd, _valider(nom="a"), utilisateur="x-access-token", jeton="ghp_x", coffret=c)
    importer(bd, "- {nom: a, emplacement: https://github.com/a/b.git, branche: dev,"
                 " visibilite: public-maison}",
             noms_reserves=RESERVES)
    assert c.dechiffrer(_ligne(bd, "a")["jeton_chiffre"]) == "ghp_x"


def test_import_changement_d_hote_retire_le_jeton(bd):
    ajouter(bd, _valider(nom="a"), utilisateur="x-access-token", jeton="ghp_x",
            coffret=_coffret())
    importer(bd, "- {nom: a, emplacement: https://autre.example/a/b.git, visibilite: prive}",
             noms_reserves=RESERVES)
    assert _ligne(bd, "a")["jeton_chiffre"] is None


def test_import_echec_en_cours_annule_tout(bd, monkeypatch):
    from rag import sources_gerees
    appels = []
    reel = sources_gerees._inserer_source

    def casse(*a, **k):
        appels.append(1)
        if len(appels) == 2:
            raise RuntimeError("boum")
        return reel(*a, **k)
    monkeypatch.setattr(sources_gerees, "_inserer_source", casse)
    with pytest.raises(RuntimeError):
        importer(bd, "- {nom: a, emplacement: https://h/a.git, visibilite: prive}\n"
                     "- {nom: b, emplacement: https://h/b.git, visibilite: prive}",
                 noms_reserves=RESERVES)
    assert _nb(bd) == 0


def test_import_nom_non_gere_refuse(bd):
    _inserer(bd, "yaml", False)
    with pytest.raises(SourceInvalide, match="déjà pris"):
        importer(bd, "- {nom: yaml, emplacement: https://h/a.git, visibilite: prive}",
                 noms_reserves=RESERVES)


def test_import_sans_visibilite_refuse(bd):
    with pytest.raises(SourceInvalide, match="visibilite"):
        importer(bd, "- {nom: a, emplacement: https://h/a.git}", noms_reserves=RESERVES)
    assert _nb(bd) == 0


def test_import_sur_source_travail_sans_le_champ_refuse(bd):
    ajouter(bd, _valider(nom="a", visi="travail"), utilisateur="x-access-token", jeton=None,
            coffret=None)
    with pytest.raises(SourceInvalide, match="visibilite"):
        importer(bd, "- {nom: a, emplacement: https://github.com/a/b.git}",
                 noms_reserves=RESERVES)
    assert _ligne(bd, "a")["visibilite"] == "travail"


def test_import_nomme_les_changements_de_niveau(bd):
    ajouter(bd, _valider(nom="a", visi="travail"), utilisateur="x-access-token", jeton=None,
            coffret=None)
    avant = _ligne(bd, "a")["demande_le"]
    bilan = importer(bd, "- {nom: a, emplacement: https://github.com/a/b.git,"
                         " visibilite: public-maison}", noms_reserves=RESERVES)
    assert bilan["niveaux"] == [{"nom": "a", "de": "travail", "vers": "public-maison"}]
    assert bilan["modifiees"] == 1
    assert _ligne(bd, "a")["demande_le"] == avant


@pytest.mark.parametrize("visi", ["", None])
def test_valider_sans_visibilite(visi):
    with pytest.raises(SourceInvalide, match="visibilité"):
        _valider(visi=visi)


# --- amorçage unique -------------------------------------------------------------------

def test_amorcer_garde_les_lignes(bd, index_code, tmp_path):
    from rag.sources_gerees import amorcer
    bd.execute("UPDATE sources SET geree = false WHERE nom = 'depot-test'")   # ligne non gérée
    bd.commit()
    avant = bd.execute("SELECT id, revision FROM sources WHERE nom='depot-test'").fetchone()
    docs = bd.execute("SELECT count(*) AS n FROM documents").fetchone()["n"]
    assert docs > 0
    f = tmp_path / "amorce.yaml"
    f.write_text('- {nom: depot-test, emplacement: "https://x/d.git", exclure: ["banc/**"],'
                 ' visibilite: public-maison}\n')
    assert amorcer(bd, f) == 1
    apres = bd.execute("SELECT * FROM sources WHERE nom='depot-test'").fetchone()
    assert (apres["id"], apres["revision"]) == (avant["id"], avant["revision"])
    assert bd.execute("SELECT count(*) AS n FROM documents").fetchone()["n"] == docs
    assert apres["geree"] and apres["exclure"] == ["banc/**"]
    assert apres["emplacement"] == "https://x/d.git" and apres["sans_garde"] is False
    assert apres["demande_le"] is None
    assert amorcer(bd, f) == 0


def test_amorcer_instance_neuve(bd, tmp_path):
    from rag.sources_gerees import amorcer
    f = tmp_path / "vide.yaml"
    f.write_text("")
    assert amorcer(bd, f) == 0
    assert bd.execute("SELECT count(*) AS n FROM sources").fetchone()["n"] == 0
    assert bd.execute("SELECT 1 FROM instance WHERE cle = 'sources_amorcees'").fetchone()


def test_amorcer_insere_une_source_neuve_sans_reindexation(bd, tmp_path):
    from rag.sources_gerees import amorcer
    f = tmp_path / "a.yaml"
    f.write_text('- {nom: neuve, emplacement: "https://x/n.git", branche: master,'
                 ' visibilite: prive}\n')
    assert amorcer(bd, f) == 1
    r = bd.execute("SELECT * FROM sources WHERE nom='neuve'").fetchone()
    assert r["geree"] and r["type"] == "code" and r["branche"] == "master"
    assert r["demande_le"] is None


def test_amorcer_journalise_les_lignes_absentes(bd, tmp_path, caplog):
    from rag.sources_gerees import amorcer
    bd.execute("INSERT INTO sources (nom, type, emplacement, visibilite, branche)"
               " VALUES ('orpheline', 'code', 'x', 'public-maison', 'main')")
    bd.commit()
    f = tmp_path / "a.yaml"
    f.write_text("")
    with caplog.at_level("WARNING"):
        amorcer(bd, f)
    assert "orpheline" in caplog.text


def test_config_principale_sans_source_de_code():
    for f in ("config/rag.yaml", "config/dev.yaml"):
        assert all(s.type != "code" for s in charger_config(Path(f)).sources)


def test_sources_maison_valides():
    from rag.sources_gerees import lire_export
    # sources `code` de config/rag.yaml au 2026-11-23
    assert len(lire_export(Path("config/sources-maison.yaml").read_text())) == 50


@pytest.mark.parametrize("branche", ["*", "ma*", "a?b", "a[b", "a:b", "a..b", "a~1", "a^", "a\\b",
                                     "dev/", "dev.lock", "/dev", "a//b", "a@{1}", "x.", "a\x01"])
def test_branche_a_motif_ou_reservee_refusee(branche):
    with pytest.raises(SourceInvalide):
        _valider(branche=branche)


def test_branche_hierarchique_acceptee():
    assert _valider(branche="feature/main-2.1").branche == "feature/main-2.1"


def test_modifier_exclure_remet_le_graphe_a_zero(bd):
    _gerer(bd)
    bd.execute("UPDATE sources SET graphe_revision = 'abc' WHERE nom = 'g'")
    bd.commit()
    assert _modif(bd, "g", exclure=["*.csv"]) == {"exclure"}
    assert _ligne(bd, "g")["graphe_revision"] is None


def test_ajouter_pose_sans_garde(bd):
    ajouter(bd, _valider(nom="neuf"), utilisateur="x", jeton=None, coffret=None)
    assert _ligne(bd, "neuf")["sans_garde"] is True


def test_import_export_garde_travail(bd):
    importer(bd, "- {nom: a, emplacement: https://github.com/a/b.git, visibilite: travail}",
             noms_reserves=RESERVES)
    assert _ligne(bd, "a")["visibilite"] == "travail"
    assert "visibilite: travail" in exporter(bd)
