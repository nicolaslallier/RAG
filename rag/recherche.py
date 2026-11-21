"""Recherche hybride : vecteur + plein texte + flou, fusion RRF, filtres et niveaux."""

import unicodedata
from dataclasses import dataclass
from datetime import date
from urllib.parse import quote

from rag.config import RechercheConf


def normaliser(texte: str) -> str:
    t = unicodedata.normalize("NFKD", texte.lower())
    return unicodedata.normalize("NFC", "".join(c for c in t if not unicodedata.combining(c)))


def vecteur_sql(v: list[float]) -> str:
    return "[" + ",".join(f"{x:.6g}" for x in v) + "]"


@dataclass
class Filtres:
    sources: list[str] | None = None
    dossier: str | None = None
    tags: list[str] | None = None
    type: str | None = None
    depuis: date | None = None
    jusqua: date | None = None


@dataclass
class Resultat:
    morceau_id: int
    document_id: int
    cle: str
    source: str
    type_source: str
    chemin: str
    titre: str
    fil: str
    texte: str
    symbole: str | None
    ligne: int | None
    origine: str | None
    lien: str
    score: float
    cosinus: float | None
    rerank: float | None = None
    via: str = "hybride"


def lien_source(type_source: str, source: str, chemin: str, ligne: int | None) -> str:
    if type_source == "obsidian":
        sans_md = chemin.removesuffix(".md")
        return f"obsidian://open?vault=Maison&file={quote(sans_md)}"
    return f"{source}/{chemin}" + (f"#L{ligne}" if ligne is not None else "")


_FILTRES = {
    "sources": "s.nom = ANY(%(f_sources)s)",
    "dossier": "starts_with(d.chemin, %(f_dossier)s)",
    "tags": "d.tags @> %(f_tags)s",
    "type": "d.type = %(f_type)s",
    "depuis": "d.date_doc >= %(f_depuis)s",
    "jusqua": "d.date_doc <= %(f_jusqua)s",
}

# Pas de CTE commun aux trois listes : un CTE lu plusieurs fois est matérialisé et l'index HNSW
# ne sert plus. Chaque liste lit donc morceaux avec ses propres filtres ; la liste vecteur trie
# sur la seule distance (index HNSW, hnsw.iterative_scan) puis est renumérotée.
_DE = """FROM morceaux m JOIN documents d ON d.id = m.document_id
           JOIN sources s ON s.id = d.source_id
          WHERE m.version_id = %(version)s AND m.embedding IS NOT NULL
            AND s.visibilite = ANY(%(niveaux)s) {filtres}"""
_SQL = """
WITH q AS (SELECT NULLIF(replace(plainto_tsquery('fr', %(question)s)::text, '&', '|'), '')::tsquery
                    AS ts),
vecteur AS (SELECT id, row_number() OVER (ORDER BY dist, id) AS r FROM (
              SELECT m.id, m.embedding <=> %(vec)s::halfvec AS dist """ + _DE + """
               ORDER BY m.embedding <=> %(vec)s::halfvec LIMIT %(top_vecteur)s) x),
texte AS (SELECT id, row_number() OVER (ORDER BY rang DESC, id) AS r FROM (
            SELECT m.id, ts_rank_cd(m.tsv, (SELECT ts FROM q)) AS rang """ + _DE + """
               AND m.tsv @@ (SELECT ts FROM q)
             ORDER BY rang DESC, m.id LIMIT %(top_texte)s) x),
flou AS (SELECT id, row_number() OVER (ORDER BY sim DESC, id) AS r FROM (
           SELECT m.id, greatest(word_similarity(%(qnorm)s, d.titre_norm),
                                 word_similarity(%(qnorm)s, lower(coalesce(m.symbole, '')))) AS sim
           """ + _DE + """) x
          WHERE sim >= %(seuil_flou)s ORDER BY sim DESC, id LIMIT 20),
fusion AS (SELECT id, sum(poids / (%(rrf_k)s + r)) AS score FROM (
             SELECT id, r, %(p_vecteur)s::float AS poids FROM vecteur
   UNION ALL SELECT id, r, %(p_texte)s FROM texte
   {flou}) u WHERE poids > 0 GROUP BY id)
SELECT m.id AS morceau_id, d.id AS document_id, s.nom AS source, s.type AS type_source,
       d.chemin, d.titre, m.fil, m.symbole, m.ligne, d.origine, m.parent_id, f.score,
       1 - (m.embedding <=> %(vec)s::halfvec) AS cosinus, coalesce(p.texte, m.texte) AS texte
  FROM fusion f JOIN morceaux m ON m.id = f.id JOIN documents d ON d.id = m.document_id
  JOIN sources s ON s.id = d.source_id LEFT JOIN morceaux p ON p.id = m.parent_id
 ORDER BY f.score DESC, m.id LIMIT %(lim_sql)s
"""


def chercher(conn, question: str, conf: RechercheConf, modeles, version_id: int,
             modele_embedding: str, *, niveaux: frozenset[str], filtres: Filtres | None = None,
             limite: int = 10) -> list[Resultat]:
    if not niveaux or not question.strip():
        return []
    filtres = filtres or Filtres()
    vec = modeles.embed_requete(question, modele_embedding, conf.instruction_requete)
    params = {
        "vec": vecteur_sql(vec), "question": question,
        "qnorm": normaliser(question), "version": version_id, "niveaux": sorted(niveaux),
        "top_vecteur": conf.top_vecteur, "top_texte": conf.top_texte,
        "seuil_flou": conf.seuil_flou, "rrf_k": conf.rrf_k, "p_vecteur": conf.poids.vecteur,
        "p_texte": conf.poids.texte, "p_flou": conf.poids.flou, "lim_sql": limite * 4,
    }
    clauses = _clauses(filtres, params)
    sql = _SQL.format(filtres=" ".join(clauses),
                      flou="UNION ALL SELECT id, r, %(p_flou)s FROM flou" if conf.flou else "")
    with conn.transaction():
        conn.execute("SET LOCAL hnsw.iterative_scan = relaxed_order")
        conn.execute("SET LOCAL hnsw.ef_search = 200")  # 40 par défaut : rappel@10 -0,07 au banc
        lignes = conn.execute(sql, params).fetchall()
    out, parents_vus = [], set()
    for r in lignes:
        if r["parent_id"] is not None:  # parent_enfant : un seul résultat par parent
            if r["parent_id"] in parents_vus:
                continue
            parents_vus.add(r["parent_id"])
        out.append(_resultat(r))
        if len(out) == limite:
            break
    if conf.graphe.expansion:
        out = etendre_par_graphe(conn, out, vec, conf, version_id, niveaux=niveaux,
                                 filtres=filtres)[:limite]
    return out


def _clauses(filtres: Filtres, params: dict) -> list[str]:
    clauses = []
    for nom, sql in _FILTRES.items():
        valeur = getattr(filtres, nom)
        if valeur is not None:
            clauses.append(f"AND {sql}")
            params[f"f_{nom}"] = valeur
    return clauses


def _resultat(r, via: str = "hybride") -> Resultat:
    return Resultat(
        morceau_id=r["morceau_id"], document_id=r["document_id"],
        cle=f"{r['source']}:{r['chemin']}", source=r["source"],
        type_source=r["type_source"], chemin=r["chemin"], titre=r["titre"], fil=r["fil"],
        texte=r["texte"], symbole=r["symbole"], ligne=r["ligne"], origine=r["origine"],
        lien=lien_source(r["type_source"], r["source"], r["chemin"], r["ligne"]),
        score=float(r["score"]), cosinus=float(r["cosinus"]), via=via,
    )


_VOISINS = """
SELECT DISTINCT ON (d.id) m.id AS morceau_id, d.id AS document_id, s.nom AS source,
       s.type AS type_source, d.chemin, d.titre, m.fil, m.symbole, m.ligne, d.origine,
       0.0 AS score, 1 - (m.embedding <=> %(vec)s::halfvec) AS cosinus,
       coalesce(p.texte, m.texte) AS texte
  FROM morceaux m JOIN documents d ON d.id = m.document_id
  JOIN sources s ON s.id = d.source_id LEFT JOIN morceaux p ON p.id = m.parent_id
 WHERE m.version_id = %(version)s AND m.embedding IS NOT NULL
   AND s.visibilite = ANY(%(niveaux)s) {filtres}
   AND NOT (d.id = ANY(%(deja)s))
   AND d.id IN (SELECT n2.document_id FROM noeuds n1
                  JOIN liens l ON l.de = n1.id OR l.vers = n1.id
                  JOIN noeuds n2 ON n2.id = CASE WHEN l.de = n1.id THEN l.vers ELSE l.de END
                 WHERE n1.document_id = %(doc)s AND n2.document_id IS NOT NULL)
 ORDER BY d.id, m.embedding <=> %(vec)s::halfvec
"""


def etendre_par_graphe(conn, resultats: list[Resultat], vec: list[float], conf: RechercheConf,
                       version_id: int, *, niveaux: frozenset[str],
                       filtres: Filtres | None = None) -> list[Resultat]:
    """Voisins à un saut des `depuis` premiers résultats, score atténué, `plafond` ajouts."""
    g = conf.graphe
    params = {"vec": vecteur_sql(vec), "version": version_id, "niveaux": sorted(niveaux)}
    sql = _VOISINS.format(filtres=" ".join(_clauses(filtres or Filtres(), params)))
    deja = {r.document_id for r in resultats}
    ajouts: list[Resultat] = []
    for origine in resultats[:g.depuis]:
        voisins = conn.execute(sql, params | {"doc": origine.document_id,
                                              "deja": list(deja)}).fetchall()
        for r in sorted(voisins, key=lambda r: -r["cosinus"]):
            if len(ajouts) == g.plafond:
                break
            x = _resultat(r, via="graphe")
            x.score = origine.score * g.attenuation
            ajouts.append(x)
            deja.add(x.document_id)
    return sorted(resultats + ajouts, key=lambda r: -r.score)
