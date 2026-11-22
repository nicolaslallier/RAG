"""Gestion de l'index : logique partagée par l'interface, le MCP et la CLI.

Pour l'instant : les exclusions manuelles (motifs fnmatch par source, comme `exclure:`).
"""

from collections.abc import Iterable
from fnmatch import fnmatch


def motifs_manuels(conn, source: str) -> list[str]:
    """Motifs exclus à la main pour la source `source` (son nom)."""
    return [r["motif"] for r in conn.execute(
        "SELECT motif FROM exclusions_manuelles WHERE source = %s ORDER BY motif", (source,))]


def correspond(chemin: str, motifs: Iterable[str]) -> bool:
    """Vrai si un motif couvre le chemin ; `*` traverse les `/`, comme `exclure:`."""
    return any(fnmatch(chemin, m) for m in motifs)
