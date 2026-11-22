"""Lister : exhaustif par métadonnées (type, tags), complété par la recherche ; sinon regroupement
des résultats de recherche par document."""

from dataclasses import asdict, replace

from rag.config import Config
from rag.recherche import Filtres, _clauses, chercher, lien_source
from rag.refus import doit_refuser

PLAFOND = 500


def taxonomie(conn, *, niveaux: frozenset[str]) -> dict:
    vis = sorted(niveaux)
    base = "FROM documents d JOIN sources s ON s.id = d.source_id WHERE s.visibilite = ANY(%s)"
    types = conn.execute(f"SELECT d.type, count(*) n {base} AND d.type IS NOT NULL "
                         "GROUP BY 1 ORDER BY 1", (vis,)).fetchall()
    tags = conn.execute(f"SELECT t, count(*) n {base.replace('WHERE', ', unnest(d.tags) t WHERE')}"
                        " GROUP BY 1 ORDER BY 1", (vis,)).fetchall()
    sources = conn.execute("SELECT nom FROM sources WHERE visibilite = ANY(%s) ORDER BY nom",
                           (vis,)).fetchall()
    return {"types": {r["type"]: r["n"] for r in types}, "tags": {r["t"]: r["n"] for r in tags},
            "sources": [r["nom"] for r in sources]}


_DOCS = """SELECT d.id, s.nom AS source, s.type AS type_source, d.chemin, d.titre, d.origine,
       d.type, d.tags, d.date_doc
  FROM documents d JOIN sources s ON s.id = d.source_id
 WHERE s.visibilite = ANY(%(niveaux)s) {filtres}"""


def _element(d, certitude: str, extrait: str | None = None) -> dict:
    return {"cle": f"{d['source']}:{d['chemin']}", "titre": d["titre"],
            "lien": lien_source(d["type_source"], d["source"], d["chemin"], None),
            "origine": d["origine"], "type": d["type"], "tags": list(d["tags"]),
            "date": d["date_doc"].isoformat() if d["date_doc"] else None,
            "certitude": certitude, "extrait": extrait}


def _par_document(conn, resultats, niveaux, exclus: set[int]) -> list[dict]:
    meilleurs = {}
    for r in resultats:  # déjà triés : le premier morceau d'un document est son meilleur
        if r.document_id not in exclus:
            meilleurs.setdefault(r.document_id, r)
    if not meilleurs:
        return []
    docs = {d["id"]: d for d in conn.execute(
        _DOCS.format(filtres="AND d.id = ANY(%(ids)s)"),
        {"niveaux": sorted(niveaux), "ids": list(meilleurs)}).fetchall()}
    return [_element(docs[i], "probable", r.fil) for i, r in meilleurs.items() if i in docs]


def lister(conn, question: str, filtres: Filtres, config: Config, modeles, version_id: int, *,
           niveaux: frozenset[str], reranker=None) -> dict:
    def cherche(f: Filtres, limite: int):
        return chercher(conn, question, config.recherche, modeles, version_id,
                        config.indexation.modele_embedding, niveaux=niveaux, filtres=f,
                        limite=limite, reranker=reranker)

    if filtres.type is not None or filtres.tags:
        params = {"niveaux": sorted(niveaux)}
        sql = _DOCS.format(filtres=" ".join(_clauses(filtres, params)))
        docs = conn.execute(sql + f" ORDER BY d.titre, d.id LIMIT {PLAFOND + 1}",
                            params).fetchall()
        tronque = len(docs) > PLAFOND
        docs = docs[:PLAFOND]
        elements = [_element(d, "tague") for d in docs]
        actif = config.recherche.rerank.actif
        pertinents = [r for r in cherche(replace(filtres, type=None, tags=None), 30)
                      if not doit_refuser([r], config.reponse, actif)]
        elements += _par_document(conn, pertinents, niveaux, {d["id"] for d in docs})
        mode = "metadonnees"
    else:
        elements = _par_document(conn, cherche(filtres, 50), niveaux, set())
        mode, tronque = "regroupement", False
    f = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in asdict(filtres).items()}
    return {"mode": mode, "filtres": f, "elements": elements, "total": len(elements),
            "tronque": tronque}
