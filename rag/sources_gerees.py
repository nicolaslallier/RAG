"""Sources git gérées en base : chiffrement des jetons (Coffret) et liste des sources actives."""

import logging
from dataclasses import dataclass

from cryptography.fernet import Fernet, InvalidToken

from rag.config import Config, ErreurConfig, Reglages, SourceConf

log = logging.getLogger(__name__)


class JetonIllisible(Exception):
    """Le jeton stocké n'est pas déchiffrable avec la clé courante."""


class Coffret:
    """Chiffre les jetons d'accès avec une clé Fernet (RAG_CLE_SECRETS)."""

    def __init__(self, cle: str):
        self._fernet = Fernet(cle.encode())

    def chiffrer(self, jeton: str) -> bytes:
        return self._fernet.encrypt(jeton.encode())

    def dechiffrer(self, brut: bytes) -> str:
        try:
            return self._fernet.decrypt(bytes(brut)).decode()
        except InvalidToken as e:
            raise JetonIllisible("jeton illisible avec la clé courante") from e


def coffret_depuis(reglages: Reglages) -> Coffret | None:
    if not reglages.cle_secrets:
        return None
    try:
        return Coffret(reglages.cle_secrets)
    except ValueError as e:
        raise ErreurConfig("RAG_CLE_SECRETS invalide") from e


@dataclass(frozen=True)
class SourceActive:
    conf: SourceConf
    jeton: str | None = None
    utilisateur: str = "x-access-token"


def sources_actives(conn, config: Config, coffret: Coffret | None) -> list[SourceActive]:
    """Sources obsidian du YAML, puis sources git gérées en base (par nom).

    Les sources `code` du YAML sont ignorées : la base les remplace.
    """
    actives = [SourceActive(s) for s in config.sources if s.type == "obsidian"]
    for r in conn.execute("SELECT * FROM sources WHERE geree ORDER BY nom"):
        jeton = None
        if r["jeton_chiffre"] is not None:
            if coffret is None:
                log.warning("jeton ignoré pour la source %s : RAG_CLE_SECRETS absente", r["nom"])
            else:
                try:
                    jeton = coffret.dechiffrer(r["jeton_chiffre"])
                except JetonIllisible:
                    log.warning("jeton illisible pour la source %s : ignoré", r["nom"])
        conf = SourceConf(nom=r["nom"], type="code", emplacement=r["emplacement"],
                          branche=r["branche"] or "main", exclure=list(r["exclure"]),
                          visibilite=r["visibilite"])
        actives.append(SourceActive(conf, jeton, r["utilisateur"]))
    return actives
