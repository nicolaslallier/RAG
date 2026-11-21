"""Entretien du coffre (agent-doc) : doublons, notes orphelines, liens entrants."""

from rag.recherche import lien_source


def _note(source: str, chemin: str, titre: str) -> dict:
    return {"cle": f"{source}:{chemin}", "titre": titre,
            "lien": lien_source("obsidian", source, chemin, None)}


_NOTES = """FROM documents d JOIN sources s ON s.id = d.source_id
           WHERE s.type = 'obsidian' AND s.visibilite = ANY(%(niveaux)s)"""


def doublons(conn, version_id: int, seuil: float = 0.92, limite: int = 50, *,
             niveaux: frozenset[str]) -> list[dict]:
    # ponytail: jointure n², correcte jusqu'à quelques milliers de notes ; passer par l'index HNSW
    # au-delà
    lignes = conn.execute(
        f"""WITH v AS (SELECT d.id, s.nom, d.chemin, d.titre, d.sha256,
                              avg(m.embedding::vector) AS e
                         {_NOTES.replace("WHERE", "JOIN morceaux m ON m.document_id = d.id WHERE")}
                          AND m.version_id = %(v)s AND m.embedding IS NOT NULL
                        GROUP BY d.id, s.nom)
            SELECT a.nom AS a_s, a.chemin AS a_c, a.titre AS a_t,
                   b.nom AS b_s, b.chemin AS b_c, b.titre AS b_t,
                   CASE WHEN a.sha256 = b.sha256 THEN 1.0 ELSE 1 - (a.e <=> b.e) END AS sim
              FROM v a JOIN v b ON a.id < b.id
             WHERE a.sha256 = b.sha256 OR 1 - (a.e <=> b.e) >= %(seuil)s
             ORDER BY sim DESC, a.id, b.id LIMIT %(lim)s""",
        {"niveaux": sorted(niveaux), "v": version_id, "seuil": seuil, "lim": limite}).fetchall()
    return [{"a": _note(r["a_s"], r["a_c"], r["a_t"]), "b": _note(r["b_s"], r["b_c"], r["b_t"]),
             "similarite": min(1.0, float(r["sim"]))} for r in lignes]


_A_DES_LIENS = """EXISTS (SELECT 1 FROM noeuds n JOIN liens l ON l.de = n.id OR l.vers = n.id
                          WHERE n.document_id = d.id AND l.origine = 'wikilien')"""


def orphelines(conn, *, niveaux: frozenset[str]) -> list[dict]:
    return [_note(r["nom"], r["chemin"], r["titre"]) for r in conn.execute(
        f"SELECT s.nom, d.chemin, d.titre {_NOTES} AND NOT {_A_DES_LIENS} ORDER BY s.nom, d.chemin",
        {"niveaux": sorted(niveaux)})]


def liens_entrants(conn, cle: str, *, niveaux: frozenset[str]) -> dict:
    source, _, chemin = cle.partition(":")
    p = {"niveaux": sorted(niveaux), "s": source, "c": chemin}
    note = conn.execute(f"SELECT d.id, s.nom, d.chemin, d.titre {_NOTES} "
                        "AND s.nom = %(s)s AND d.chemin = %(c)s", p).fetchone()
    if note is None:
        return {"etat": "introuvable", "note": None, "entrants": []}
    entrants = conn.execute(
        f"""SELECT DISTINCT s.nom, d.chemin, d.titre {_NOTES.replace("WHERE", '''
              JOIN noeuds n ON n.document_id = d.id JOIN liens l ON l.de = n.id
              JOIN noeuds cible ON cible.id = l.vers WHERE''')}
              AND l.origine = 'wikilien' AND cible.document_id = %(id)s
            ORDER BY s.nom, d.chemin""", p | {"id": note["id"]}).fetchall()
    return {"etat": "ok", "note": _note(note["nom"], note["chemin"], note["titre"]),
            "entrants": [_note(r["nom"], r["chemin"], r["titre"]) for r in entrants]}
