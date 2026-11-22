"""Contrôles de démarrage : réglages refusés avant de servir, attente de la base."""

import sys
import time
from typing import Literal

from rag.config import ErreurConfig, Reglages, charger_config

MIN_JETON = 32


def verifier_reglages(r: Reglages, role: Literal["api", "travailleur"]) -> None:
    """Lève ErreurConfig en nommant la variable, jamais sa valeur."""
    jetons = {"RAG_JETON": r.jeton, "RAG_JETON_PRIVE": r.jeton_prive,
              "RAG_JETON_REINDEXER": r.jeton_reindexer}
    for nom, v in jetons.items():
        if (v or nom != "RAG_JETON_PRIVE") and len(v) < MIN_JETON:
            raise ErreurConfig(f"{nom} trop court ({MIN_JETON} caractères au moins)")
    vus: dict[str, str] = {}
    for nom, v in jetons.items():
        if v and v in vus:
            raise ErreurConfig(f"{nom} égal à {vus[v]}")
        vus[v] = nom
    if not r.litellm_cle:
        raise ErreurConfig("RAG_LITELLM_CLE vide")
    if role == "travailleur":
        sources = charger_config(r.config).sources
        if any(s.type == "obsidian" and s.emplacement == "s3" for s in sources):
            if not r.s3_secret_key:
                raise ErreurConfig("RAG_S3_SECRET_KEY vide alors qu'une source lit S3")
            if not r.s3_endpoint.startswith("https://"):   # le coffre ne passe pas en clair
                raise ErreurConfig("RAG_S3_ENDPOINT doit être en https://")
        if any(s.type == "code" and s.emplacement.startswith("https://github.com/")
               for s in sources) and not r.github_jeton:
            raise ErreurConfig("RAG_GITHUB_JETON vide alors qu'une source clone GitHub")


def attendre_base(ouvrir, pause: float = 30, dormir=None):
    """Réessaie `ouvrir()` jusqu'au succès, une ligne de journal par échec, sans sortir."""
    while True:
        try:
            return ouvrir()
        except Exception as e:  # noqa: BLE001 - Postgres pas encore prêt, on réessaie
            # Première ligne du message psycopg (ex. échec d'authentification) : jamais de mot de passe.
            detail = (str(e).strip().splitlines() or [""])[0][:200]
            print(f"base injoignable ({type(e).__name__}: {detail}), nouvel essai dans {pause:.0f} s",
                  file=sys.stderr, flush=True)
            (dormir or time.sleep)(pause)
