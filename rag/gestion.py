"""Gestion de l'index : logique partagée par l'interface, le MCP et la CLI.

Exclusions manuelles (motifs fnmatch par source, comme `exclure:`) et lecture des sources et fichiers.
"""

from collections.abc import Iterable
from fnmatch import fnmatch
from typing import Literal

from rag.sources import nfc


def motifs_manuels(conn, source: str) -> list[str]:
    """Motifs exclus à la main pour la source `source` (son nom)."""
    return [r["motif"] for r in conn.execute(
        "SELECT motif FROM exclusions_manuelles WHERE source = %s ORDER BY motif", (source,))]


def correspond(chemin: str, motifs: Iterable[str]) -> bool:
    """Vrai si un motif couvre le chemin ; `*` traverse les `/`, comme `exclure:`.

    L'égalité exacte prime (un chemin exclu tel quel peut contenir `[`, `?` ou `*`) ;
    chemin et motifs sont comparés en NFC.
    """
    chemin = nfc(chemin)
    return any(chemin == m or fnmatch(chemin, m) for m in map(nfc, motifs))


class SourceInconnue(Exception):
    """Source absente ou invisible pour ces niveaux (même exception : on ne distingue pas)."""


def sources(conn, *, niveaux: frozenset[str]) -> list[dict]:
    """Sources visibles, triées par nom, avec leurs comptes de fichiers indexés et exclus."""
    return conn.execute(
        """SELECT s.nom, s.type, s.visibilite, s.revision, s.indexe_le,
                  (SELECT count(*) FROM documents d WHERE d.source_id = s.id) AS documents,
                  (SELECT count(*) FROM exclus e
                    WHERE e.source_id = s.id AND e.raison <> 'manuelle') AS exclus_auto,
                  (SELECT count(*) FROM exclus e
                    WHERE e.source_id = s.id AND e.raison = 'manuelle') AS exclus_manuels,
                  s.demande_le IS NOT NULL AS demande_en_attente
             FROM sources s WHERE s.visibilite = ANY(%s) ORDER BY s.nom""",
        (sorted(niveaux),)).fetchall()


def _source_visible(conn, source: str, niveaux: frozenset[str]) -> int:
    r = conn.execute("SELECT id FROM sources WHERE nom = %s AND visibilite = ANY(%s)",
                     (source, sorted(niveaux))).fetchone()
    if r is None:
        raise SourceInconnue(source)
    return r["id"]


def _echapper_like(texte: str) -> str:
    return texte.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


# Indexés puis exclus ; un fichier présent des deux côtés (ne devrait pas arriver) compte une fois.
_TOUS = """WITH tous AS (
  SELECT d.chemin, 'indexe' AS etat, NULL::text AS raison, d.titre, d.type, d.tags,
         (SELECT count(*) FROM morceaux m WHERE m.document_id = d.id
             AND m.version_id = (SELECT max(version_id) FROM morceaux
                                  WHERE document_id = d.id)) AS morceaux,
         d.modifie_le
    FROM documents d WHERE d.source_id = %(s)s
  UNION ALL
  SELECT e.chemin,
         CASE WHEN e.raison = 'manuelle' THEN 'exclu_manuel' ELSE 'exclu_auto' END,
         e.raison, NULL, NULL, '{}'::text[], 0, NULL
    FROM exclus e
   WHERE e.source_id = %(s)s
     AND NOT EXISTS (SELECT 1 FROM documents d
                      WHERE d.source_id = e.source_id AND d.chemin = e.chemin))
"""


def fichiers(conn, source: str, *, niveaux: frozenset[str],
             etat: Literal["indexe", "exclu_auto", "exclu_manuel"] | None = None,
             contient: str | None = None, page: int = 1, par_page: int = 50) -> dict:
    """Fichiers d'une source, indexés ou exclus, triés par chemin, paginés."""
    source_id = _source_visible(conn, source, niveaux)
    page = max(page, 1)
    params = {"s": source_id, "etat": etat,
              "contient": f"%{_echapper_like(contient)}%" if contient else None}
    filtre = """FROM tous WHERE (%(etat)s::text IS NULL OR etat = %(etat)s)
                  AND (%(contient)s::text IS NULL OR chemin ILIKE %(contient)s ESCAPE '\\')"""
    total = conn.execute(_TOUS + "SELECT count(*) AS n " + filtre, params).fetchone()["n"]
    lignes = conn.execute(
        _TOUS + "SELECT * " + filtre + " ORDER BY chemin COLLATE \"C\" LIMIT %(lim)s OFFSET %(off)s",
        {**params, "lim": par_page, "off": (page - 1) * par_page}).fetchall()
    return {"source": source, "total": total, "page": page,
            "pages": -(-total // par_page), "fichiers": lignes}


def fichier(conn, source: str, chemin: str, *, niveaux: frozenset[str],
            version_id: int | None) -> dict:
    """Métadonnées d'un document indexé et ses morceaux (de `version_id`, parents compris)."""
    source_id = _source_visible(conn, source, niveaux)
    doc = conn.execute(
        """SELECT id, titre, type, tags, alias, origine, frontmatter, liens_sortants, modifie_le
             FROM documents WHERE source_id = %s AND chemin = %s""",
        (source_id, chemin)).fetchone()
    if doc is None:
        raise SourceInconnue(source)
    doc_id = doc.pop("id")
    morceaux = [] if version_id is None else conn.execute(
        """SELECT ordre, fil, texte, symbole, ligne FROM morceaux
            WHERE document_id = %s AND version_id = %s ORDER BY ordre""",
        (doc_id, version_id)).fetchall()
    return {"source": source, "chemin": chemin, **doc, "morceaux": morceaux}
