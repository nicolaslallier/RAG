"""Quarantaine : décisions à la main sur les fichiers écartés par le filtre (secret, genere).

Accepter lève une règle pour un fichier et une signature ; refuser sort le fichier de la file.
Lectures filtrées par `niveaux` ; écritures réservées à l'interface (écrivain) et à la CLI."""

import re
from dataclasses import dataclass
from typing import Literal

import yaml

from rag.config import NOM_MAX
from rag.exclusions import RAISONS_ACCEPTABLES, REGLES_ACCEPTABLES
from rag.gestion import (
    MOTIF_MAX,
    _source,
    _source_connue,
    demander_reindexation,
    verrouiller_source,
)
from rag.sources import nfc


class DecisionInvalide(ValueError):
    """Décision refusée (fichier hors quarantaine, signature périmée, import invalide) ; rien n'est écrit."""


@dataclass(frozen=True)
class Levee:
    """Une acceptation : la signature et, pour `contenu_secret`, le contenu (sha256) acceptés."""
    signature: str
    regles: frozenset[str]
    contenu_sha256: str | None = None
    indexe_sha256: str | None = None  # levees_indexees : sha256 du document indexé grâce à elle

    def pour(self, signature: str | None = None, sha256: str | None = None) -> frozenset[str]:
        """Règles levées pour le fichier vu : aucune si sa signature a changé ; `contenu_secret`
        seulement si le contenu effectivement lu (`sha256`) est celui qui a été accepté."""
        if signature is not None and signature != self.signature:
            return frozenset()
        if sha256 is not None and sha256 != self.contenu_sha256:
            return self.regles - {"contenu_secret"}
        return self.regles


def _levee(r) -> Levee:
    return Levee(r["signature"], frozenset(r["regles"]), r["contenu_sha256"],
                 r.get("indexe_sha256"))


def levees(conn, source: str) -> dict[str, Levee]:
    """Acceptations de `source`, par chemin."""
    return {r["chemin"]: _levee(r) for r in conn.execute(
        "SELECT chemin, signature, regles, contenu_sha256 FROM quarantaine_decisions "
        "WHERE source = %s AND decision = 'accepte'", (source,))}


def levees_indexees(conn, source: str) -> dict[str, Levee]:
    """Acceptations des fichiers indexés sous la signature acceptée (pour graphify), avec le
    sha256 du document indexé : la copie n'en lève les règles que pour ce contenu-là."""
    return {r["chemin"]: _levee(r) for r in conn.execute(
        """SELECT q.chemin, q.signature, q.regles, q.contenu_sha256, d.sha256 AS indexe_sha256
             FROM quarantaine_decisions q
             JOIN sources s ON s.nom = q.source
             JOIN documents d ON d.source_id = s.id AND d.chemin = q.chemin
                             AND d.signature = q.signature
            WHERE q.source = %s AND q.decision = 'accepte'
              AND (NOT 'contenu_secret' = ANY(q.regles) OR d.sha256 = q.contenu_sha256)""",
        (source,))}


# Fichiers écartés pour une raison acceptable, puis fichiers acceptés déjà indexés.
_FILE = """WITH file AS (
  SELECT s.nom AS source, e.chemin, e.raison, e.signature, e.detail, q.cree_le AS decide_le,
         CASE WHEN q.decision = 'refuse' THEN 'refuse'
              WHEN q.decision = 'accepte' AND q.signature = e.signature
                   AND (e.detail->>'regle') = ANY(q.regles)
                   AND (e.detail->>'regle' <> 'contenu_secret'
                        OR e.contenu_sha256 IS NOT DISTINCT FROM q.contenu_sha256) THEN 'accepte'
              ELSE 'a_traiter' END AS etat,
         false AS indexe,
         COALESCE(q.decision = 'accepte' AND (q.signature <> e.signature
                  OR (e.detail->>'regle' = 'contenu_secret' AND 'contenu_secret' = ANY(q.regles)
                      AND e.contenu_sha256 IS DISTINCT FROM q.contenu_sha256)), false) AS modifie,
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
    return conn.execute("SELECT raison, signature, detail, contenu_sha256 FROM exclus "
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
        if regle == "contenu_secret" and not e["contenu_sha256"]:
            raise DecisionInvalide("verdict pas encore détaillé : attendre la prochaine passe")
        if signature is not None and signature != e["signature"]:
            raise DecisionInvalide("le fichier a changé depuis l'affichage : recharger la page")
        # contenu_secret : liée au contenu lu (sha256), pas seulement à la signature
        sha = e["contenu_sha256"] if regle == "contenu_secret" else None
        regles = conn.execute(
            """INSERT INTO quarantaine_decisions AS q
                 (source, chemin, decision, signature, regles, contenu_sha256)
               VALUES (%s, %s, 'accepte', %s, %s, %s)
               ON CONFLICT (source, chemin) DO UPDATE SET
                 regles = CASE WHEN q.decision = 'accepte' AND q.signature = EXCLUDED.signature
                               THEN ARRAY(SELECT DISTINCT r FROM unnest(q.regles || EXCLUDED.regles) r
                                           ORDER BY r)
                               ELSE EXCLUDED.regles END,
                 contenu_sha256 = CASE WHEN q.decision = 'accepte'
                                        AND q.signature = EXCLUDED.signature
                                       THEN COALESCE(EXCLUDED.contenu_sha256, q.contenu_sha256)
                                       ELSE EXCLUDED.contenu_sha256 END,
                 decision = 'accepte', signature = EXCLUDED.signature, cree_le = now()
               RETURNING regles""",
            (source, chemin, e["signature"], [regle], sha)).fetchone()["regles"]
        if s["type"] == "code":  # graphify doit voir le fichier rendu
            conn.execute("UPDATE sources SET graphe_revision = NULL WHERE id = %s", (s["id"],))
        demander_reindexation(conn, source)  # valide le tout
    except BaseException:
        conn.rollback()
        raise
    return {"regle": regle, "regles": regles}


def _decision(conn, source: str, chemin: str) -> dict | None:
    return conn.execute("SELECT decision, signature, regles FROM quarantaine_decisions "
                        "WHERE source = %s AND chemin = %s", (source, chemin)).fetchone()


def _retirer_document_accepte(conn, s: dict, source: str, chemin: str) -> bool:
    """Retire le document indexé grâce à une acceptation (même signature), ses nœuds, et le
    rend à `exclus` ; sans commit. `empreinte` vide : la prochaine passe reprend le verdict."""
    q = _decision(conn, source, chemin)
    if q is None or q["decision"] != "accepte":
        return False
    d = conn.execute("SELECT id FROM documents WHERE source_id = %s AND chemin = %s "
                     "AND signature = %s", (s["id"], chemin, q["signature"])).fetchone()
    if d is None:
        return False
    conn.execute("DELETE FROM noeuds WHERE document_id = %s", (d["id"],))
    conn.execute("DELETE FROM documents WHERE id = %s", (d["id"],))
    raison = "genere" if q["regles"] == ["genere"] else "secret"
    conn.execute(
        """INSERT INTO exclus (source_id, chemin, signature, empreinte, raison, lu, detail,
                              contenu_sha256)
           VALUES (%s, %s, %s, '', %s, true, NULL, NULL) ON CONFLICT (source_id, chemin)
           DO UPDATE SET signature = EXCLUDED.signature, empreinte = '',
                         raison = EXCLUDED.raison, lu = true, detail = NULL,
                         contenu_sha256 = NULL""",
        (s["id"], chemin, q["signature"], raison))
    if s["type"] == "code":
        conn.execute("UPDATE sources SET graphe_revision = NULL WHERE id = %s", (s["id"],))
    return True


def refuser(conn, source: str, chemin: str) -> None:
    """Le fichier reste exclu et sort de la file ; un document accepté et indexé est retiré."""
    chemin = nfc(chemin)
    try:
        verrouiller_source(conn, source)
        s = _source_connue(conn, source)
        _retirer_document_accepte(conn, s, source, chemin)
        e = _exclu(conn, s["id"], chemin)
        if e is None or e["raison"] not in RAISONS_ACCEPTABLES:
            raise DecisionInvalide("ce fichier n'est pas en quarantaine")
        regle = (e["detail"] or {}).get("regle")
        conn.execute(
            """INSERT INTO quarantaine_decisions (source, chemin, decision, signature, regles)
               VALUES (%s, %s, 'refuse', %s, %s)
               ON CONFLICT (source, chemin) DO UPDATE SET decision = 'refuse',
                 signature = EXCLUDED.signature, regles = EXCLUDED.regles, cree_le = now()""",
            (source, chemin, e["signature"], [regle] if regle in REGLES_ACCEPTABLES else []))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def annuler(conn, source: str, chemin: str) -> bool:
    """Oublie la décision ; `False` s'il n'y en avait pas. Une acceptation annulée retire le
    document indexé grâce à elle et demande une passe (le verdict est repris)."""
    chemin = nfc(chemin)
    try:
        verrouiller_source(conn, source)
        q = _decision(conn, source, chemin)
        if q is None:
            conn.rollback()
            return False
        if s := _source(conn, source):
            _retirer_document_accepte(conn, s, source, chemin)
        conn.execute("DELETE FROM quarantaine_decisions WHERE source = %s AND chemin = %s",
                     (source, chemin))
        if q["decision"] == "accepte":
            demander_reindexation(conn, source)  # valide le tout
        else:
            conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return True


SIGNATURE_MAX = 200
_CLES = {"source", "chemin", "decision", "signature", "regles"}
_SHA256 = re.compile(r"[0-9a-f]{64}")


def exporter(conn) -> str:
    """Décisions en YAML : liste de `{source, chemin, decision, signature, regles,
    contenu_sha256}` (`contenu_sha256` : le contenu accepté pour `contenu_secret`, sinon nul)."""
    return yaml.safe_dump(
        [dict(r) for r in conn.execute(
            "SELECT source, chemin, decision, signature, regles, contenu_sha256 "
            'FROM quarantaine_decisions ORDER BY source, chemin COLLATE "C"')],
        allow_unicode=True, sort_keys=False)


def _texte(valeur, maxi: int) -> bool:
    return isinstance(valeur, str) and 1 <= len(valeur.strip()) <= maxi


def _lignes_import(texte: str) -> list[tuple]:
    try:
        donnees = yaml.safe_load(texte)
    except yaml.YAMLError as e:
        raise DecisionInvalide("YAML illisible") from e
    if not isinstance(donnees, list):
        raise DecisionInvalide("une liste de décisions est attendue")
    lignes = []
    for i, d in enumerate(donnees, 1):
        if not isinstance(d, dict) or not _CLES <= d.keys() <= _CLES | {"contenu_sha256"}:
            raise DecisionInvalide(
                f"entrée {i} : clés source, chemin, decision, signature, regles (contenu_sha256)")
        regles, sha = d["regles"], d.get("contenu_sha256")
        if (not _texte(d["source"], NOM_MAX) or not _texte(d["chemin"], MOTIF_MAX)
                or not _texte(d["signature"], SIGNATURE_MAX)
                or d["decision"] not in ("accepte", "refuse")
                or not isinstance(regles, list)
                or not all(isinstance(r, str) and r in REGLES_ACCEPTABLES for r in regles)
                or (d["decision"] == "accepte" and not regles)
                or not (sha is None or (isinstance(sha, str) and _SHA256.fullmatch(sha)))
                or (d["decision"] == "accepte" and "contenu_secret" in regles and sha is None)):
            raise DecisionInvalide(f"entrée {i} : valeur refusée")
        lignes.append((d["source"].strip(), nfc(d["chemin"].strip()), d["decision"],
                       d["signature"].strip(), sorted(set(regles)), sha))
    return lignes


def importer(conn, texte: str) -> dict:
    """Importe un export ; tout est validé avant d'écrire, en une transaction. Fusion : une
    décision déjà en place est gardée. Une acceptation ajoutée demande une passe de sa source."""
    lignes = _lignes_import(texte)
    ajoutees, a_reindexer = 0, set()
    try:
        for source, chemin, decision, signature, regles, sha in lignes:
            if conn.execute(
                    """INSERT INTO quarantaine_decisions
                         (source, chemin, decision, signature, regles, contenu_sha256)
                       VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (source, chemin) DO NOTHING""",
                    (source, chemin, decision, signature, regles, sha)).rowcount:
                ajoutees += 1
                if decision == "accepte":
                    a_reindexer.add(source)
        conn.execute("UPDATE sources SET graphe_revision = NULL "
                     "WHERE type = 'code' AND nom = ANY(%s)", (sorted(a_reindexer),))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    for source in sorted(a_reindexer):
        demander_reindexation(conn, source)  # sans effet pour une source inconnue
    return {"ajoutees": ajoutees, "ignorees": len(lignes) - ajoutees}
