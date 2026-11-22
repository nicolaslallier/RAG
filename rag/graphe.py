"""Graphe de connaissances dans Postgres : structure du code (graphify), [[liens]] du coffre,
chemin et expliquer. Toute lecture prend `niveaux` (visibilité des sources)."""

import json
import logging
import os
import re
import shlex
import shutil
import subprocess
from pathlib import Path

import psycopg

from rag.exclusions import motif_exclusion
from rag.gestion import motifs_manuels
from rag.recherche import lien_source, normaliser

log = logging.getLogger(__name__)

_CONFIANCE = {"EXTRACTED": "EXTRAIT", "INFERRED": "INFERE", "AMBIGUOUS": "AMBIGU"}
_LIGNE = re.compile(r"L(\d+)")


class ErreurGraphe(Exception):
    pass


# --- Structure du code ----------------------------------------------------------------------

def _type_graphify(n: dict) -> str:
    if n.get("_callable_class"):
        return "classe"
    if n.get("_callable"):
        return "fonction"
    if n.get("file_type") == "rationale":
        return "rationale"
    if n.get("type"):
        return n["type"]
    if n.get("source_file") and Path(n["source_file"]).name == n.get("label"):
        return "fichier"
    return "symbole"


def depuis_graphify(graphe: dict) -> dict:
    noeuds = []
    for n in graphe.get("nodes", []):
        m = _LIGNE.fullmatch(n.get("source_location") or "")
        libelle = n.get("label") or n["id"]
        if n.get("_callable") and libelle.endswith("()"):  # « .lancer() » -> « lancer »
            libelle = libelle.removesuffix("()").lstrip(".") or libelle
        noeuds.append({"cle": n["id"], "libelle": libelle,
                       "type": _type_graphify(n), "fichier": n.get("source_file") or None,
                       "ligne": int(m.group(1)) if m else None, "communaute": n.get("community")})
    cles = {n["cle"] for n in noeuds}
    liens = [{"de": lk["source"], "vers": lk["target"], "type": lk.get("relation") or "lie",
              "confiance": _CONFIANCE.get(lk.get("confidence"), "EXTRAIT")}
             for lk in graphe.get("links", []) if lk["source"] in cles and lk["target"] in cles]
    return {"noeuds": noeuds, "liens": liens}


def _copier_sans_exclus(clone: Path, dest: Path, exclure: list[str]) -> None:
    """Copie de travail pour graphify : sans .git, sans secrets ni fichiers générés."""
    for p in sorted(clone.rglob("*")):
        rel = p.relative_to(clone)
        if rel.parts[0] == ".git" or p.is_symlink() or not p.is_file():
            continue
        contenu = p.read_bytes()
        if motif_exclusion(rel.as_posix(), contenu, exclure) is None:
            (dest / rel).parent.mkdir(parents=True, exist_ok=True)
            (dest / rel).write_bytes(contenu)


def extraire_structure(clone: Path, travail: Path, delai_s: int = 600, *,
                       commande: str = "graphify", exclure: list[str] = ()) -> dict:
    copie = Path(travail) / "src"
    shutil.rmtree(copie, ignore_errors=True)
    copie.mkdir(parents=True)
    _copier_sans_exclus(Path(clone), copie, list(exclure))
    # Liste blanche : aucune clé (infonuagique ou RAG_*) ne peut atteindre graphify.
    env = {k: v for k, v in os.environ.items() if k in ("PATH", "HOME", "LANG")}
    try:
        r = subprocess.run([*shlex.split(commande), "update", str(copie.resolve())], check=False,
                           env=env, capture_output=True, timeout=delai_s, cwd=travail)
    except subprocess.TimeoutExpired:
        raise ErreurGraphe(f"graphify : délai dépassé ({delai_s} s)") from None
    except OSError as e:
        raise ErreurGraphe(f"graphify : {e}") from None
    if r.returncode:
        raise ErreurGraphe(f"graphify : code {r.returncode} : "
                           + r.stderr.decode(errors="replace").strip()[-300:])
    sortie = copie / "graphify-out" / "graph.json"
    if not sortie.exists():
        raise ErreurGraphe("graphify : graph.json absent")
    try:
        return depuis_graphify(json.loads(sortie.read_text(encoding="utf-8")))
    except (ValueError, KeyError) as e:
        raise ErreurGraphe(f"graphify : graph.json illisible ({type(e).__name__})") from None


def charger_graphe(conn, source_nom: str, graphe: dict, origine: str = "structure"
                   ) -> tuple[int, int]:
    """Remplace les liens de `origine` partant de la source. Un lien peut viser un nœud d'une
    autre source par `vers_id`."""
    with conn.transaction():
        sid = conn.execute("SELECT id FROM sources WHERE nom = %s", (source_nom,)
                           ).fetchone()["id"]
        conn.execute("""DELETE FROM liens l USING noeuds n
                         WHERE l.de = n.id AND n.source_id = %s AND l.origine = %s""",
                     (sid, origine))
        ids = {}
        for n in graphe["noeuds"]:
            ids[n["cle"]] = conn.execute(
                """INSERT INTO noeuds (source_id, cle, libelle, libelle_norm, type, communaute,
                                       document_id, ligne)
                   VALUES (%(s)s, %(cle)s, %(lib)s, %(norm)s, %(type)s, %(com)s,
                           (SELECT id FROM documents WHERE source_id = %(s)s AND chemin = %(f)s),
                           %(ligne)s)
                   ON CONFLICT (source_id, cle) DO UPDATE SET libelle = EXCLUDED.libelle,
                     libelle_norm = EXCLUDED.libelle_norm, type = EXCLUDED.type,
                     communaute = coalesce(EXCLUDED.communaute, noeuds.communaute),
                     document_id = coalesce(EXCLUDED.document_id, noeuds.document_id),
                     ligne = coalesce(EXCLUDED.ligne, noeuds.ligne)
                   RETURNING id""",
                {"s": sid, "cle": n["cle"], "lib": n["libelle"], "norm": normaliser(n["libelle"]),
                 "type": n.get("type"), "com": n.get("communaute"), "f": n.get("fichier"),
                 "ligne": n.get("ligne")}).fetchone()["id"]
        n_liens = 0
        for lk in graphe["liens"]:
            vers = lk.get("vers_id") or ids.get(lk["vers"])
            if ids.get(lk["de"]) is None or vers is None:
                continue
            n_liens += conn.execute(
                """INSERT INTO liens (de, vers, type, confiance, origine)
                   VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                (ids[lk["de"]], vers, lk["type"], lk["confiance"], origine)).rowcount
        conn.execute("""DELETE FROM noeuds n WHERE n.source_id = %s AND NOT (n.cle = ANY(%s))
                          AND NOT EXISTS (SELECT 1 FROM liens l WHERE l.de = n.id OR l.vers = n.id)
                     """, (sid, list(ids)))
    return len(ids), n_liens


# --- Liens [[…]] du coffre ------------------------------------------------------------------

def _resoudre(cible: str, origine: str, par_chemin: dict, par_titre: dict, par_alias: dict):
    """Chemin exact, puis titre, puis alias ; homonymes : celui du même dossier, sinon rien."""
    c = cible.strip()
    for k in (c, c + ".md"):
        if k in par_chemin:
            return k
    for index in (par_titre, par_alias):
        cand = index.get(normaliser(c), [])
        if len(cand) == 1:
            return cand[0]
        if cand:
            dossier = origine.rpartition("/")[0]
            meme = [x for x in cand if x.rpartition("/")[0] == dossier]
            return meme[0] if len(meme) == 1 else None
    return None


def charger_wikiliens(conn, source_nom: str) -> tuple[int, int, int]:
    docs = conn.execute(
        """SELECT d.chemin, d.titre, d.alias, d.liens_sortants FROM documents d
             JOIN sources s ON s.id = d.source_id WHERE s.nom = %s ORDER BY d.chemin""",
        (source_nom,)).fetchall()
    par_chemin = {d["chemin"]: d for d in docs}
    par_titre, par_alias = {}, {}
    for d in docs:
        nom_fichier = d["chemin"].rsplit("/", 1)[-1].removesuffix(".md")
        for t in {normaliser(nom_fichier), normaliser(d["titre"])}:
            par_titre.setdefault(t, []).append(d["chemin"])
        for a in d["alias"]:
            par_alias.setdefault(normaliser(a), []).append(d["chemin"])
    noeuds = [{"cle": d["chemin"], "libelle": d["titre"], "type": "note", "fichier": d["chemin"]}
              for d in docs]
    liens, non_resolues = [], []
    for d in docs:
        for cible in d["liens_sortants"]:
            if not cible.strip():
                continue
            c = _resoudre(cible, d["chemin"], par_chemin, par_titre, par_alias)
            if c is None:
                non_resolues.append((d["chemin"], cible))
            elif c != d["chemin"]:
                liens.append({"de": d["chemin"], "vers": c, "type": "lien",
                              "confiance": "EXTRAIT"})
    n, l = charger_graphe(conn, source_nom, {"noeuds": noeuds, "liens": liens}, "wikilien")
    for chemin, cible in non_resolues:
        log.info("%s : [[%s]] non résolu dans %s", source_nom, cible, chemin)
    charger_wikiliens.non_resolues = non_resolues
    return n, l, len(non_resolues)


# --- Mise à jour après indexation -----------------------------------------------------------

def maj_graphe(conn, source, lecteur, reglages, *, forcer: bool = False) -> str:
    try:
        if source.type == "obsidian":
            n, l, non = charger_wikiliens(conn, source.nom)
            return f"graphe : {n} nœuds, {l} liens, {non} cibles non résolues"
        r = conn.execute("SELECT revision, graphe_revision FROM sources WHERE nom = %s",
                         (source.nom,)).fetchone()
        motifs = motifs_manuels(conn, source.nom)
        conn.commit()
        if r is None or not r["revision"]:
            return "graphe : source pas encore indexée"
        if r["revision"] == r["graphe_revision"] and not forcer:
            return "graphe : à jour"
        travail = Path(reglages.donnees_dir) / "graphe" / source.nom
        g = extraire_structure(lecteur.dest, travail, commande=reglages.graphify_commande,
                               exclure=[*source.exclure, *motifs])
        n, l = charger_graphe(conn, source.nom, g)
        conn.execute("UPDATE sources SET graphe_revision = revision WHERE nom = %s",
                     (source.nom,))
        conn.commit()
        return f"graphe : {n} nœuds, {l} liens"
    except (ErreurGraphe, psycopg.Error) as e:
        conn.rollback()
        log.error("%s : graphe en échec : %s", source.nom, e)
        return f"graphe : ÉCHEC {e}"


# --- Lecture : trouver, chemin, expliquer ---------------------------------------------------

_COLS = """n.id, n.libelle, n.type, n.cle, n.ligne, n.communaute, s.nom AS source,
           s.type AS type_source, d.chemin"""
_VISIBLE = """FROM noeuds n JOIN sources s ON s.id = n.source_id
              LEFT JOIN documents d ON d.id = n.document_id
             WHERE s.visibilite = ANY(%(niveaux)s)"""


def _public(r: dict) -> dict:
    lien = lien_source(r["type_source"], r["source"], r["chemin"], r["ligne"]) \
        if r["chemin"] else None
    return {"id": r["id"], "libelle": r["libelle"], "type": r["type"], "source": r["source"],
            "chemin": r["chemin"], "lien": lien}


def trouver_noeuds(conn, nom: str, limite: int = 8, *, niveaux: frozenset[str]) -> list[dict]:
    """Égalité sur libellé normalisé ou clé d'abord, sinon similarité ≥ 0,5 ; « id:<n> »."""
    p = {"niveaux": sorted(niveaux), "norm": normaliser(nom.strip()), "nom": nom.strip(),
         "lim": limite}
    if not niveaux or not p["norm"]:
        return []
    if nom.startswith("id:") and nom[3:].isdigit():
        r = conn.execute(f"SELECT {_COLS} {_VISIBLE} AND n.id = %(id)s",
                         p | {"id": int(nom[3:])}).fetchall()
        return [_public(x) for x in r]
    r = conn.execute(f"""SELECT {_COLS} {_VISIBLE}
                          AND (n.libelle_norm = %(norm)s OR n.cle = %(nom)s)
                        ORDER BY n.id LIMIT %(lim)s""", p).fetchall()
    if r:
        return [_public(x) for x in r]
    r = conn.execute(f"""SELECT {_COLS} {_VISIBLE}
                          AND n.libelle_norm %% %(norm)s
                          AND similarity(n.libelle_norm, %(norm)s) >= 0.5
                        ORDER BY similarity(n.libelle_norm, %(norm)s) DESC, n.id
                        LIMIT %(lim)s""", p).fetchall()
    return [_public(x) for x in r]


def _un(conn, nom: str, niveaux) -> tuple[list[dict], dict | None]:
    cands = trouver_noeuds(conn, nom, niveaux=niveaux)
    return cands, (cands[0] if len(cands) == 1 else None)


def chemin(conn, a: str, b: str, max_sauts: int = 6, *, niveaux: frozenset[str]) -> dict:
    ca, na = _un(conn, a, niveaux)
    cb, nb = _un(conn, b, niveaux)
    out = {"etat": "ok", "candidats": {"a": ca, "b": cb}, "etapes": []}
    if not ca or not cb:
        return out | {"etat": "introuvable"}
    if na is None or nb is None:
        return out | {"etat": "ambigu"}
    p = {"niveaux": sorted(niveaux)}
    precedent = {na["id"]: None}  # nœud -> (nœud d'avant, lien)
    frontiere = [na["id"]]
    for _ in range(max_sauts):
        if nb["id"] in precedent or not frontiere:
            break
        lignes = conn.execute(
            """SELECT l.de, l.vers, l.type, l.confiance FROM liens l
                 JOIN noeuds n1 ON n1.id = l.de JOIN sources s1 ON s1.id = n1.source_id
                 JOIN noeuds n2 ON n2.id = l.vers JOIN sources s2 ON s2.id = n2.source_id
                WHERE (l.de = ANY(%(f)s) OR l.vers = ANY(%(f)s))
                  AND s1.visibilite = ANY(%(niveaux)s) AND s2.visibilite = ANY(%(niveaux)s)
                ORDER BY CASE l.confiance WHEN 'EXTRAIT' THEN 0 WHEN 'INFERE' THEN 1 ELSE 2 END,
                         l.id""", p | {"f": frontiere}).fetchall()
        fset, suivante = set(frontiere), []
        for lk in lignes:
            for depuis, vers, sens in ((lk["de"], lk["vers"], "sortant"),
                                       (lk["vers"], lk["de"], "entrant")):
                if depuis in fset and vers not in precedent:
                    precedent[vers] = (depuis, {"type": lk["type"], "confiance": lk["confiance"],
                                                "sens": sens})
                    suivante.append(vers)
        frontiere = suivante
    if nb["id"] not in precedent:
        return out | {"etat": "aucun_chemin"}
    suite, x = [], nb["id"]
    while precedent[x] is not None:
        avant, lien = precedent[x]
        suite.append((x, lien))
        x = avant
    suite.append((x, None))
    suite.reverse()
    infos = {r["id"]: _public(r) for r in conn.execute(
        f"SELECT {_COLS} {_VISIBLE} AND n.id = ANY(%(ids)s)",
        p | {"ids": [i for i, _ in suite]})}
    return out | {"etapes": [{"noeud": infos[i], "lien": lien} for i, lien in suite]}


def expliquer(conn, x: str, limite: int = 20, *, niveaux: frozenset[str]) -> dict:
    cands, n = _un(conn, x, niveaux)
    out = {"etat": "ok", "candidats": cands, "noeud": None, "communaute": None, "voisins": [],
           "n_voisins": 0}
    if not cands:
        return out | {"etat": "introuvable"}
    if n is None:
        return out | {"etat": "ambigu"}
    p = {"niveaux": sorted(niveaux), "id": n["id"], "lim": limite}
    voisins_sql = f"""
        SELECT {_COLS}, v.lien_type, v.confiance, v.sens FROM (
            SELECT vers AS autre, type AS lien_type, confiance, 'sortant' AS sens, id AS lid
              FROM liens WHERE de = %(id)s
            UNION ALL
            SELECT de, type, confiance, 'entrant', id FROM liens WHERE vers = %(id)s) v
        JOIN noeuds n ON n.id = v.autre JOIN sources s ON s.id = n.source_id
        LEFT JOIN documents d ON d.id = n.document_id
       WHERE s.visibilite = ANY(%(niveaux)s)"""
    total = conn.execute(f"SELECT count(*) AS c FROM ({voisins_sql}) t", p).fetchone()["c"]
    voisins = conn.execute(
        voisins_sql + """ ORDER BY CASE v.confiance WHEN 'EXTRAIT' THEN 0 WHEN 'INFERE' THEN 1
                          ELSE 2 END, v.lid LIMIT %(lim)s""", p).fetchall()
    com = conn.execute(f"SELECT {_COLS} {_VISIBLE} AND n.id = %(id)s", p).fetchone()["communaute"]
    communaute = None
    if com is not None:
        membres = conn.execute(
            f"""SELECT n.libelle, (SELECT count(*) FROM liens l WHERE l.de = n.id OR l.vers = n.id)
                       AS degre {_VISIBLE} AND n.communaute = %(c)s
                   AND n.source_id = (SELECT source_id FROM noeuds WHERE id = %(id)s)
                 ORDER BY degre DESC, n.id""", p | {"c": com}).fetchall()
        communaute = {"id": com, "taille": len(membres),
                      "membres": [m["libelle"] for m in membres[:10]]}
    return out | {"noeud": n, "communaute": communaute, "n_voisins": total,
                  "voisins": [_public(v) | {"lien_type": v["lien_type"],
                                            "confiance": v["confiance"], "sens": v["sens"]}
                              for v in voisins]}
