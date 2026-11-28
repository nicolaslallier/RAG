"""Quarantaine : décisions à la main sur les fichiers écartés par le filtre (secret, genere).

Accepter lève une règle pour un fichier et une signature ; refuser sort le fichier de la file.
Lectures filtrées par `niveaux` ; écritures réservées à l'interface (écrivain) et à la CLI."""

from typing import Literal

from rag.exclusions import RAISONS_ACCEPTABLES, REGLES_ACCEPTABLES
from rag.gestion import _source_connue, demander_reindexation, verrouiller_source
from rag.sources import nfc


class DecisionInvalide(ValueError):
    """Décision refusée (fichier hors quarantaine, signature périmée, import invalide) ; rien n'est écrit."""


def levees(conn, source: str) -> dict[str, tuple[str, frozenset[str]]]:
    """Acceptations de `source` : chemin -> (signature acceptée, règles levées)."""
    return {r["chemin"]: (r["signature"], frozenset(r["regles"])) for r in conn.execute(
        "SELECT chemin, signature, regles FROM quarantaine_decisions "
        "WHERE source = %s AND decision = 'accepte'", (source,))}


def levees_indexees(conn, source: str) -> dict[str, frozenset[str]]:
    """Règles levées des fichiers acceptés et indexés sous la signature acceptée (pour graphify)."""
    return {r["chemin"]: frozenset(r["regles"]) for r in conn.execute(
        """SELECT q.chemin, q.regles FROM quarantaine_decisions q
             JOIN sources s ON s.nom = q.source
             JOIN documents d ON d.source_id = s.id AND d.chemin = q.chemin
                             AND d.signature = q.signature
            WHERE q.source = %s AND q.decision = 'accepte'""", (source,))}


# Fichiers écartés pour une raison acceptable, puis fichiers acceptés déjà indexés.
_FILE = """WITH file AS (
  SELECT s.nom AS source, e.chemin, e.raison, e.signature, e.detail, q.cree_le AS decide_le,
         CASE WHEN q.decision = 'refuse' THEN 'refuse'
              WHEN q.decision = 'accepte' AND q.signature = e.signature
                   AND (e.detail->>'regle') = ANY(q.regles) THEN 'accepte'
              ELSE 'a_traiter' END AS etat,
         false AS indexe,
         COALESCE(q.decision = 'accepte' AND q.signature <> e.signature, false) AS modifie,
         CASE WHEN q.decision = 'accepte' AND q.signature = e.signature THEN q.regles
              ELSE '{}'::text[] END AS regles_levees
    FROM exclus e JOIN sources s ON s.id = e.source_id
    LEFT JOIN quarantaine_decisions q ON q.source = s.nom AND q.chemin = e.chemin
   WHERE s.visibilite = ANY(%(niveaux)s) AND e.raison = ANY(%(raisons)s)
     AND NOT EXISTS (SELECT 1 FROM documents d
                      WHERE d.source_id = e.source_id AND d.chemin = e.chemin)
  UNION ALL
  SELECT s.nom, q.chemin, CASE WHEN q.regles = ARRAY['genere'] THEN 'genere' ELSE 'secret' END,
         q.signature, NULL::jsonb, q.cree_le, 'accepte', true, false, q.regles
    FROM quarantaine_decisions q JOIN sources s ON s.nom = q.source
    JOIN documents d ON d.source_id = s.id AND d.chemin = q.chemin AND d.signature = q.signature
   WHERE s.visibilite = ANY(%(niveaux)s) AND q.decision = 'accepte')
"""


def file(conn, *, niveaux: frozenset[str], source: str | None = None,
         raison: Literal["secret", "genere"] | None = None,
         etat: Literal["a_traiter", "accepte", "refuse"] = "a_traiter",
         page: int = 1, par_page: int = 50) -> dict:
    """File de quarantaine des sources visibles, triée par source puis chemin, paginée.
    Une source inconnue ou invisible donne une liste vide (rien ne dit si elle existe)."""
    page = max(page, 1)
    params = {"niveaux": sorted(niveaux), "raisons": list(RAISONS_ACCEPTABLES),
              "source": source, "raison": raison, "etat": etat}
    filtre = """FROM file WHERE etat = %(etat)s
                  AND (%(source)s::text IS NULL OR source = %(source)s)
                  AND (%(raison)s::text IS NULL OR raison = %(raison)s)"""
    total = conn.execute(_FILE + "SELECT count(*) AS n " + filtre, params).fetchone()["n"]
    lignes = conn.execute(
        _FILE + "SELECT * " + filtre
        + ' ORDER BY source, chemin COLLATE "C" LIMIT %(lim)s OFFSET %(off)s',
        {**params, "lim": par_page, "off": (page - 1) * par_page}).fetchall()
    return {"total": total, "page": page, "pages": -(-total // par_page), "fichiers": lignes}


def comptes(conn, *, niveaux: frozenset[str]) -> dict[str, int]:
    """Nombre de fichiers à traiter, par raison."""
    n = dict.fromkeys(RAISONS_ACCEPTABLES, 0)
    for r in conn.execute(
            _FILE + "SELECT raison, count(*) AS n FROM file WHERE etat = 'a_traiter' "
            "GROUP BY raison",
            {"niveaux": sorted(niveaux), "raisons": list(RAISONS_ACCEPTABLES)}):
        n[r["raison"]] = r["n"]
    return n


def decrire(detail: dict | None) -> str:
    """Le déclencheur en une ligne de texte (CLI) ; jamais de contenu, les extraits sont masqués."""
    if not detail:
        return "—"
    if detail["regle"] == "nom_secret":
        return f"nom de fichier : {detail['motif']}"
    if detail["regle"] == "genere":
        return f"fichier généré : {detail['motif']}"
    lignes = " ; ".join(f"ligne {c['ligne']} {c['extrait']}" for c in detail["correspondances"])
    return f"contenu : {lignes}" + (" ; et d'autres" if detail.get("tronque") else "")


def _exclu(conn, source_id: int, chemin: str) -> dict | None:
    return conn.execute("SELECT raison, signature, detail FROM exclus "
                        "WHERE source_id = %s AND chemin = %s", (source_id, chemin)).fetchone()


def accepter(conn, source: str, chemin: str, signature: str | None = None) -> dict:
    """Lève, pour ce fichier et sa signature, la règle qui l'a écarté ; demande une passe.

    `signature` : celle que l'appelant a vue (interface) ; `None` : la courante (CLI).
    Rend `{"regle": règle levée, "regles": toutes les règles levées}`."""
    chemin = nfc(chemin)
    try:
        verrouiller_source(conn, source)
        s = _source_connue(conn, source)
        e = _exclu(conn, s["id"], chemin)
        if e is None or e["raison"] not in RAISONS_ACCEPTABLES:
            raise DecisionInvalide("ce fichier n'est pas en quarantaine")
        regle = (e["detail"] or {}).get("regle")
        if regle not in REGLES_ACCEPTABLES:
            raise DecisionInvalide("verdict pas encore détaillé : attendre la prochaine passe")
        if signature is not None and signature != e["signature"]:
            raise DecisionInvalide("le fichier a changé depuis l'affichage : recharger la page")
        regles = conn.execute(
            """INSERT INTO quarantaine_decisions AS q (source, chemin, decision, signature, regles)
               VALUES (%s, %s, 'accepte', %s, %s)
               ON CONFLICT (source, chemin) DO UPDATE SET
                 regles = CASE WHEN q.decision = 'accepte' AND q.signature = EXCLUDED.signature
                               THEN ARRAY(SELECT DISTINCT r FROM unnest(q.regles || EXCLUDED.regles) r
                                           ORDER BY r)
                               ELSE EXCLUDED.regles END,
                 decision = 'accepte', signature = EXCLUDED.signature, cree_le = now()
               RETURNING regles""", (source, chemin, e["signature"], [regle])).fetchone()["regles"]
        if s["type"] == "code":  # graphify doit voir le fichier rendu
            conn.execute("UPDATE sources SET graphe_revision = NULL WHERE id = %s", (s["id"],))
        demander_reindexation(conn, source)  # valide le tout
    except BaseException:
        conn.rollback()
        raise
    return {"regle": regle, "regles": regles}
