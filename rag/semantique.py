"""Passe sémantique de nuit : mentions de code (sans modèle) et concepts (modèle local)."""

import json
import logging
import re

from rag.config import Config
from rag.indexeur import version_courante
from rag.modeles import ErreurModele
from rag.recherche import normaliser

log = logging.getLogger(__name__)

# Sémantique « maison » (spike : graphify choisit son fournisseur selon l'environnement et retombe
# sur api.openai.com sans OPENAI_BASE_URL) : un seul client, Modeles, qui ne parle qu'à LiteLLM.
CONSIGNE = (
    "Liste au plus 8 concepts, outils, personnes ou projets dont parle cette note. Réponds "
    'uniquement en JSON : {"concepts": [{"nom": "…", "relation": "concerne" | "utilise" | '
    '"decrit"}]}')
_RELATIONS = {"concerne", "utilise", "decrit"}
_MOT = re.compile(r"[\w][\w.-]*(?:/[\w.-]+)*")


def _noeud_depot(conn, source_id: int, nom: str) -> int:
    return conn.execute(
        """INSERT INTO noeuds (source_id, cle, libelle, libelle_norm, type)
           VALUES (%s, 'depot', %s, %s, 'depot') ON CONFLICT (source_id, cle)
           DO UPDATE SET libelle = EXCLUDED.libelle RETURNING id""",
        (source_id, nom, normaliser(nom))).fetchone()["id"]


def _noeud_fichier(conn, doc: dict) -> int:
    r = conn.execute("""SELECT id FROM noeuds WHERE document_id = %s
                         ORDER BY (type = 'fichier') DESC, ligne NULLS FIRST, id LIMIT 1""",
                     (doc["id"],)).fetchone()
    if r:
        return r["id"]
    return conn.execute(
        """INSERT INTO noeuds (source_id, cle, libelle, libelle_norm, type, document_id)
           VALUES (%s, %s, %s, %s, 'fichier', %s) ON CONFLICT (source_id, cle)
           DO UPDATE SET document_id = EXCLUDED.document_id RETURNING id""",
        (doc["source_id"], doc["chemin"], doc["chemin"], normaliser(doc["chemin"]), doc["id"])
    ).fetchone()["id"]


def mentions_de_code(conn, document_id: int, texte: str) -> list[dict]:
    """Liens `mentionne` d'une note vers le code : nom de dépôt, chemin indexé, symbole unique."""
    mots = sorted({m.rstrip(".") for m in _MOT.findall(texte)})
    out = []
    for s in conn.execute("SELECT id, nom FROM sources WHERE type = 'code' AND nom = ANY(%s)",
                          (mots,)):
        out.append({"vers_id": _noeud_depot(conn, s["id"], s["nom"]), "confiance": "EXTRAIT"})
    for d in conn.execute("""SELECT d.id, d.source_id, d.chemin FROM documents d
                               JOIN sources s ON s.id = d.source_id
                              WHERE s.type = 'code' AND d.chemin = ANY(%s)""", (mots,)):
        out.append({"vers_id": _noeud_fichier(conn, d), "confiance": "EXTRAIT"})
    par_libelle: dict[str, list[int]] = {}
    for n in conn.execute("""SELECT n.id, n.libelle FROM noeuds n JOIN sources s ON s.id = n.source_id
                              WHERE s.type = 'code' AND n.type IN ('fonction', 'classe')
                                AND length(n.libelle) >= 6 AND n.libelle = ANY(%s)
                              ORDER BY n.id""", (mots,)):
        par_libelle.setdefault(n["libelle"], []).append(n["id"])
    for ids in par_libelle.values():
        conf = "EXTRAIT" if len(ids) == 1 else "AMBIGU"
        out += [{"vers_id": i, "confiance": conf} for i in ids[:3]]
    return [m | {"type": "mentionne"} for m in out]


def concepts_de_note(modeles, modele: str, titre: str, texte: str) -> list[dict]:
    rep = modeles.generer(modele, [{"role": "system", "content": CONSIGNE},
                                   {"role": "user", "content": f"# {titre}\n\n{texte[:12000]}"}],
                          temperature=0, json_mode=True)
    try:
        concepts = json.loads(rep)["concepts"]
        assert isinstance(concepts, list)
    except (ValueError, KeyError, TypeError, AssertionError):
        return []
    out = []
    for c in concepts[:8]:
        if (isinstance(c, dict) and isinstance(c.get("nom"), str)
                and 3 <= len(c["nom"].strip()) <= 60 and c.get("relation") in _RELATIONS):
            out.append({"nom": c["nom"].strip(), "relation": c["relation"]})
    return out


def passe_semantique(conn, config: Config, modeles, source_nom: str | None = None,
                     maximum: int | None = None) -> dict:
    v = version_courante(conn, config.indexation)
    docs = conn.execute(
        """SELECT d.id, d.source_id, d.chemin, d.titre, d.sha256 FROM documents d
             JOIN sources s ON s.id = d.source_id
            WHERE s.type = 'obsidian' AND (%(s)s::text IS NULL OR s.nom = %(s)s)
              AND d.semantique_sha IS DISTINCT FROM d.sha256
            ORDER BY d.id LIMIT %(max)s""", {"s": source_nom, "max": maximum}).fetchall()
    conn.commit()
    bilan = {"traites": 0, "liens": 0, "echecs": 0}
    for d in docs:
        texte = "\n\n".join(r["texte"] for r in conn.execute(
            """SELECT texte FROM morceaux WHERE document_id = %s AND version_id = %s
                 AND parent_id IS NULL ORDER BY ordre""", (d["id"], v)))
        conn.commit()
        try:
            concepts = concepts_de_note(modeles, config.reponse.modele, d["titre"], texte)
        except ErreurModele as e:
            log.warning("sémantique : %s en échec (%s)", d["chemin"], e)
            bilan["echecs"] += 1
            continue
        with conn.transaction():
            note = conn.execute(
                """INSERT INTO noeuds (source_id, cle, libelle, libelle_norm, type, document_id)
                   VALUES (%s, %s, %s, %s, 'note', %s) ON CONFLICT (source_id, cle)
                   DO UPDATE SET document_id = EXCLUDED.document_id RETURNING id""",
                (d["source_id"], d["chemin"], d["titre"], normaliser(d["titre"]), d["id"])
            ).fetchone()["id"]
            conn.execute("DELETE FROM liens WHERE de = %s AND origine = 'semantique'", (note,))
            liens = mentions_de_code(conn, d["id"], texte)
            for c in concepts:
                cid = conn.execute(
                    """INSERT INTO noeuds (source_id, cle, libelle, libelle_norm, type)
                       VALUES (%s, %s, %s, %s, 'concept') ON CONFLICT (source_id, cle)
                       DO UPDATE SET type = 'concept' RETURNING id""",
                    (d["source_id"], "concept:" + normaliser(c["nom"]), c["nom"],
                     normaliser(c["nom"]))).fetchone()["id"]
                liens.append({"vers_id": cid, "type": c["relation"], "confiance": "INFERE"})
            for lk in liens:
                bilan["liens"] += conn.execute(
                    """INSERT INTO liens (de, vers, type, confiance, origine)
                       VALUES (%s, %s, %s, %s, 'semantique') ON CONFLICT DO NOTHING""",
                    (note, lk["vers_id"], lk["type"], lk["confiance"])).rowcount
            conn.execute("UPDATE documents SET semantique_sha = sha256 WHERE id = %s", (d["id"],))
        bilan["traites"] += 1
    with conn.transaction():  # concepts et nœuds de dépôt qui ne sont plus cités
        conn.execute("""DELETE FROM noeuds n WHERE n.type IN ('concept', 'depot')
                          AND NOT EXISTS (SELECT 1 FROM liens l WHERE l.de = n.id OR l.vers = n.id)""")
    return bilan
