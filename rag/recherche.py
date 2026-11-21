"""Recherche hybride : vecteur + plein texte + flou, fusion RRF, filtres et niveaux."""

import unicodedata


def normaliser(texte: str) -> str:
    t = unicodedata.normalize("NFKD", texte.lower())
    return unicodedata.normalize("NFC", "".join(c for c in t if not unicodedata.combining(c)))
