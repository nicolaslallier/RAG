"""Gestion de l'index : logique partagée par l'interface, le MCP et la CLI.

Exclusions manuelles (motifs fnmatch par source, comme `exclure:`) et lecture des sources et fichiers.
"""

from collections.abc import Iterable
from fnmatch import fnmatch
from typing import Literal

import yaml

from rag.config import NOM_MAX
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


_VERROUS_SOURCES = 0x52414753  # « RAGS » : espace des verrous consultatifs par source


def verrouiller_source(conn, source: str) -> None:
    """Verrou consultatif de la transaction en cours sur `source` (son nom), lâché au commit.

    Pris par l'exclusion (exclure, importer) et par l'écriture d'un document ou de nœuds de la
    source : une exclusion validée ne croise jamais une de ces écritures.
    """
    conn.execute("SELECT pg_advisory_xact_lock(%s::int, hashtext(%s))",
                 (_VERROUS_SOURCES, source))


class SourceSupprimee(Exception):
    """La source code n'existe plus en base, ou a changé (URL, branche) : rien à écrire."""


class SourceInconnue(Exception):
    """Source absente ou invisible pour ces niveaux (même exception : on ne distingue pas)."""


def sources(conn, *, niveaux: frozenset[str]) -> list[dict]:
    """Sources visibles, triées par nom, avec leurs comptes de fichiers indexés et exclus."""
    return conn.execute(
        """SELECT s.nom, s.type, s.visibilite, s.revision, s.indexe_le, s.geree,
                  (SELECT count(*) FROM documents d WHERE d.source_id = s.id) AS documents,
                  (SELECT count(*) FROM exclus e
                    WHERE e.source_id = s.id AND e.raison <> 'manuelle') AS exclus_auto,
                  (SELECT count(*) FROM exclus e
                    WHERE e.source_id = s.id AND e.raison = 'manuelle') AS exclus_manuels,
                  s.demande_le IS NOT NULL AS demande_en_attente
             FROM sources s WHERE s.visibilite = ANY(%s) ORDER BY s.nom""",
        (sorted(niveaux),)).fetchall()


def passe_en_cours(conn) -> dict | None:
    """Passe d'indexation commencée et pas finie (la plus récente) : genre et début, sinon `None`."""
    return conn.execute("SELECT genre, debut FROM passes WHERE fin IS NULL "
                        "ORDER BY debut DESC LIMIT 1").fetchone()


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


PAGE_MAX = 1_000_000  # garde-fou avant la requête (OFFSET), pour l'interface et le MCP


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


# --- Écriture : exclure, annuler, exporter, importer ---------------------------------------

MOTIF_MAX = 1000
RAISON_MAX = 500
APERCU_MAX = 50


class MotifInvalide(ValueError):
    """Motif, raison ou fichier d'import refusé ; rien n'est écrit."""


def valider_motif(motif: str) -> str:
    """Motif nettoyé (`strip`) et en NFC ; 1 à 1000 caractères, sinon `MotifInvalide`."""
    if not isinstance(motif, str):
        raise MotifInvalide("le motif doit être un texte")
    motif = nfc(motif.strip())
    if not 1 <= len(motif) <= MOTIF_MAX:
        raise MotifInvalide(f"le motif doit faire de 1 à {MOTIF_MAX} caractères")
    return motif


def _valider_raison(raison: str | None) -> str | None:
    if raison is None:
        return None
    if not isinstance(raison, str):
        raise MotifInvalide("la raison doit être un texte")
    raison = raison.strip()
    if len(raison) > RAISON_MAX:
        raise MotifInvalide(f"la raison fait au plus {RAISON_MAX} caractères")
    return raison or None


def _source(conn, source: str) -> dict | None:
    return conn.execute("SELECT id, type FROM sources WHERE nom = %s", (source,)).fetchone()


def _source_connue(conn, source: str) -> dict:
    if (s := _source(conn, source)) is None:
        raise SourceInconnue(source)
    return s


def _documents_couverts(conn, source_id: int, motif: str) -> list[dict]:
    return [d for d in conn.execute(
        "SELECT id, chemin, signature FROM documents WHERE source_id = %s", (source_id,))
        if correspond(d["chemin"], [motif])]


def apercu(conn, source: str, motif: str) -> dict:
    """Documents indexés que `motif` retirerait : leur nombre et les 50 premiers chemins."""
    motif = valider_motif(motif)
    s = _source_connue(conn, source)
    touches = sorted(d["chemin"] for d in _documents_couverts(conn, s["id"], motif))
    conn.commit()
    return {"total": len(touches), "chemins": touches[:APERCU_MAX]}


def _inserer(conn, source: str, motif: str, raison: str | None) -> bool:
    return conn.execute(
        """INSERT INTO exclusions_manuelles (source, motif, raison) VALUES (%s, %s, %s)
           ON CONFLICT (source, motif) DO NOTHING""", (source, motif, raison)).rowcount == 1


def _retirer_documents(conn, source_id: int, motif: str) -> int:
    """Supprime les documents couverts (morceaux en cascade, nœuds du graphe) ; sans commit."""
    docs = _documents_couverts(conn, source_id, motif)
    if not docs:
        return 0
    # l'empreinte n'est relue que pour les verdicts lus (lu = true) : ici, indicative
    v = conn.execute("SELECT empreinte FROM index_versions ORDER BY id DESC LIMIT 1").fetchone()
    empreinte = v["empreinte"] if v else ""
    ids = [d["id"] for d in docs]
    conn.execute("DELETE FROM noeuds WHERE document_id = ANY(%s)", (ids,))
    conn.execute("DELETE FROM documents WHERE id = ANY(%s)", (ids,))
    for d in docs:
        conn.execute(
            """INSERT INTO exclus (source_id, chemin, signature, empreinte, raison, lu)
               VALUES (%s, %s, %s, %s, 'manuelle', false) ON CONFLICT (source_id, chemin)
               DO UPDATE SET signature = EXCLUDED.signature, empreinte = EXCLUDED.empreinte,
                             raison = EXCLUDED.raison, lu = false""",
            (source_id, d["chemin"], d["signature"], empreinte))
    return len(docs)


def exclure(conn, source: str, motif: str, raison: str | None = None) -> int:
    """Exclut `motif` de `source` tout de suite ; retourne le nombre de documents retirés."""
    motif, raison = valider_motif(motif), _valider_raison(raison)
    try:
        verrouiller_source(conn, source)  # attend l'écriture en cours d'un document de la source
        s = _source_connue(conn, source)
        _inserer(conn, source, motif, raison)
        n = _retirer_documents(conn, s["id"], motif)
        if s["type"] == "code":  # graphify repasse : aucun nœud du fichier exclu ne reste
            conn.execute("UPDATE sources SET graphe_revision = NULL WHERE id = %s", (s["id"],))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return n


def demander_reindexation(conn, source: str | None) -> int:
    """Demande une passe au travailleur (toutes les sources si `None`) ; nombre de sources visées.

    Un compte seulement, sans erreur pour un nom inconnu : ni liste ni oracle des noms.
    """
    n = conn.execute(
        "UPDATE sources SET demande_le = now() WHERE %(s)s::text IS NULL OR nom = %(s)s",
        {"s": source}).rowcount
    conn.execute("NOTIFY rag_reindexer")
    conn.commit()
    return n


def annuler(conn, source: str, motif: str) -> bool:
    """Lève le motif et demande la réindexation de la source ; `False` s'il n'existait pas."""
    motif = valider_motif(motif)
    try:
        if not conn.execute("DELETE FROM exclusions_manuelles WHERE source = %s AND motif = %s",
                            (source, motif)).rowcount:
            conn.rollback()
            return False
        if s := _source(conn, source):
            restants = motifs_manuels(conn, source)
            liberes = [r["chemin"] for r in conn.execute(
                "SELECT chemin FROM exclus WHERE source_id = %s AND raison = 'manuelle'",
                (s["id"],)) if not correspond(r["chemin"], restants)]
            conn.execute("DELETE FROM exclus WHERE source_id = %s AND raison = 'manuelle' "
                         "AND chemin = ANY(%s)", (s["id"], liberes))
            if s["type"] == "code":  # graphify doit revoir les fichiers rendus
                conn.execute("UPDATE sources SET graphe_revision = NULL WHERE id = %s",
                             (s["id"],))
        demander_reindexation(conn, source)  # valide le tout
    except BaseException:
        conn.rollback()
        raise
    return True


def exclusions(conn) -> list[dict]:
    """Toutes les exclusions manuelles, triées par source puis motif."""
    return conn.execute("SELECT source, motif, raison, cree_le FROM exclusions_manuelles "
                        "ORDER BY source, motif").fetchall()


def exporter(conn) -> str:
    """Exclusions manuelles en YAML : liste de `{source, motif, raison}`."""
    return yaml.safe_dump([{"source": e["source"], "motif": e["motif"], "raison": e["raison"]}
                           for e in exclusions(conn)], allow_unicode=True, sort_keys=False)


def _lignes_import(texte: str) -> list[tuple[str, str, str | None]]:
    try:
        donnees = yaml.safe_load(texte)
    except yaml.YAMLError as e:
        raise MotifInvalide("YAML illisible") from e
    if not isinstance(donnees, list):
        raise MotifInvalide("une liste d'exclusions est attendue")
    lignes = []
    for i, d in enumerate(donnees, 1):
        if not isinstance(d, dict) or not {"source", "motif"} <= d.keys() <= {
                "source", "motif", "raison"}:
            raise MotifInvalide(f"entrée {i} : clés source, motif et raison (facultative)")
        if not isinstance(d["source"], str) or not 1 <= len(d["source"].strip()) <= NOM_MAX:
            raise MotifInvalide(f"entrée {i} : source de 1 à {NOM_MAX} caractères attendue")
        try:
            lignes.append((d["source"].strip(), valider_motif(d["motif"]),
                           _valider_raison(d.get("raison"))))
        except MotifInvalide as e:
            raise MotifInvalide(f"entrée {i} : {e}") from e
    return lignes


def importer(conn, texte: str) -> dict:
    """Importe un export YAML ; tout est validé avant d'écrire, en une transaction.

    Source connue : appliquée comme `exclure` ; source inconnue : gardée pour plus tard.
    Retourne les motifs ajoutés, ceux déjà présents et le nombre de documents retirés.
    """
    lignes = _lignes_import(texte)
    ajoutees = retires = 0
    try:
        # sources connues seulement (une inconnue n'est indexée par personne, et des milliers de
        # verrous rempliraient la table partagée), dans le même ordre : pas d'interblocage
        connues = conn.execute("SELECT nom FROM sources WHERE nom = ANY(%s) ORDER BY nom",
                               (sorted({source for source, _, _ in lignes}),)).fetchall()
        verrouillees = {r["nom"] for r in connues}
        for nom in sorted(verrouillees):
            verrouiller_source(conn, nom)
        for source, motif, raison in lignes:
            ajoutees += _inserer(conn, source, motif, raison)
            # retrait sous verrou seulement : une source apparue depuis garde son motif, que
            # l'indexeur applique au listage et avant chaque écriture
            if source in verrouillees and (s := _source(conn, source)):
                retires += _retirer_documents(conn, s["id"], motif)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return {"ajoutees": ajoutees, "ignorees": len(lignes) - ajoutees, "retires": retires}
