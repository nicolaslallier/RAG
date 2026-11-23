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
