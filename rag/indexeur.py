"""Indexeur incrémental : signature, puis sha256 ; une transaction par document."""

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from rag.config import IndexationConf, SourceConf, empreinte_indexation
from rag.decoupage import Morceau, decouper_note
from rag.decoupage_code import decouper_code, langage
from rag.exclusions import motif_exclusion
from rag.notes import Note, analyser_note
from rag.recherche import normaliser, vecteur_sql
from rag.sources import nfc

log = logging.getLogger(__name__)


class CoffrePartielError(Exception):
    pass


@dataclass
class Bilan:
    nouveaux: int = 0
    modifies: int = 0
    supprimes: int = 0
    inchanges: int = 0
    reportes: int = 0
    exclus: int = 0
    morceaux: int = 0


def version_courante(conn, conf: IndexationConf) -> int:
    emp = empreinte_indexation(conf)
    conn.execute("INSERT INTO index_versions (empreinte, config) VALUES (%s, %s) "
                 "ON CONFLICT (empreinte) DO NOTHING", (emp, json.dumps(conf.model_dump())))
    v = conn.execute("SELECT id FROM index_versions WHERE empreinte = %s", (emp,)).fetchone()["id"]
    conn.commit()
    return v


def _morceaux(source: SourceConf, chemin: str, contenu: bytes, conf: IndexationConf):
    texte = contenu.decode("utf-8", errors="replace")
    if source.type == "code":
        note = Note(chemin, [], [], langage(chemin), None, None, {}, texte, [])
        if len(contenu) > conf.taille_max_octets:
            return note, [Morceau(0, f"{source.nom} > {chemin}", texte[:2000])]  # gros fichier
        fil = f"{source.nom} > {chemin}"
        return note, decouper_code(source.nom, chemin, texte, conf) or [Morceau(0, fil, chemin)]
    note = analyser_note(chemin, texte)
    if len(contenu) > conf.taille_max_octets:
        ms = [Morceau(0, note.titre, note.corps[:2000])]
    else:
        ms = decouper_note(note.titre, note.corps, conf) or [Morceau(0, note.titre, note.titre)]
    return note, ms


def _ecrire(conn, source_id, chemin, e, sha, note, ms, conf, v, modeles, existant) -> int:
    titre_norm = normaliser(" ".join([note.titre, *note.alias]))
    doc_id = conn.execute(
        """INSERT INTO documents (source_id, chemin, titre, titre_norm, alias, tags, type, origine,
             date_doc, frontmatter, liens_sortants, signature, sha256, modifie_le)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (source_id, chemin) DO UPDATE SET titre=EXCLUDED.titre,
             titre_norm=EXCLUDED.titre_norm, alias=EXCLUDED.alias, tags=EXCLUDED.tags,
             type=EXCLUDED.type, origine=EXCLUDED.origine, date_doc=EXCLUDED.date_doc,
             frontmatter=EXCLUDED.frontmatter, liens_sortants=EXCLUDED.liens_sortants,
             signature=EXCLUDED.signature, sha256=EXCLUDED.sha256, modifie_le=EXCLUDED.modifie_le
           RETURNING id""",
        (source_id, chemin, note.titre, titre_norm, note.alias, note.tags, note.type,
         note.origine, note.date, json.dumps(note.frontmatter), note.liens, e.signature, sha,
         e.modifie_le),
    ).fetchone()["id"]
    if existant and existant["sha256"] != sha:
        conn.execute("DELETE FROM morceaux WHERE document_id = %s", (doc_id,))
    else:
        conn.execute("DELETE FROM morceaux WHERE document_id = %s AND version_id = %s", (doc_id, v))
    cherchables = [m for m in ms if m.cherchable]
    vecs = modeles.embed_documents([f"{m.fil}\n\n{m.texte}" for m in cherchables],
                                   conf.modele_embedding) if cherchables else []
    vec_de = {m.ordre: vecteur_sql(x) for m, x in zip(cherchables, vecs, strict=True)}
    tags = " ".join(note.tags)
    ids: dict[int, int] = {}
    for m in ms:
        ids[m.ordre] = conn.execute(
            """INSERT INTO morceaux (document_id, version_id, ordre, fil, texte, parent_id, symbole,
                 ligne, embedding, tsv)
               VALUES (%(d)s, %(v)s, %(o)s, %(fil)s, %(t)s, %(p)s, %(s)s, %(l)s, %(e)s::halfvec,
                 CASE WHEN %(c)s THEN setweight(to_tsvector('fr', %(fil)s), 'A')
                   || setweight(to_tsvector('fr', %(tags)s), 'B')
                   || setweight(to_tsvector('fr', %(t)s), 'C') END)
               RETURNING id""",
            {"d": doc_id, "v": v, "o": m.ordre, "fil": m.fil, "t": m.texte,
             "p": ids.get(m.parent) if m.parent is not None else None, "s": m.symbole,
             "l": m.ligne, "e": vec_de.get(m.ordre), "c": m.cherchable, "tags": tags},
        ).fetchone()["id"]
    return len(ms)


def indexer_source(conn, source: SourceConf, lecteur, conf: IndexationConf, modeles, *,
                   forcer: bool = False, sans_repos: bool = False,
                   maintenant: datetime | None = None) -> Bilan:
    maintenant = maintenant or datetime.now(UTC)
    bilan = Bilan()
    source_id = conn.execute(
        """INSERT INTO sources (nom, type, emplacement, branche, visibilite)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (nom) DO UPDATE SET type=EXCLUDED.type, emplacement=EXCLUDED.emplacement,
             branche=EXCLUDED.branche, visibilite=EXCLUDED.visibilite RETURNING id""",
        (source.nom, source.type, source.emplacement, source.branche, source.visibilite),
    ).fetchone()["id"]
    conn.commit()
    v = version_courante(conn, conf)

    entrees = {}
    for e in lecteur.lister():
        chemin = nfc(e.chemin)
        if source.type == "obsidian" and not chemin.endswith(".md"):
            continue  # pièces jointes, .gitignore, .DS_Store : hors coffre de notes
        raison = motif_exclusion(chemin, b"", source.exclure)
        if raison is None and e.taille > 10 * conf.taille_max_octets:
            raison = "trop_gros"  # jamais lu : un blob géant ne passe pas en mémoire
            log.warning("%s : %s exclu (trop_gros, %d octets)", source.nom, chemin, e.taille)
        if raison is None:
            entrees[chemin] = e
        elif raison != "config":
            bilan.exclus += 1  # secret, verrou, généré : exclus sur le nom, jamais lus
    existants = {
        r["chemin"]: r for r in conn.execute(
            """SELECT d.id, d.chemin, d.signature, d.sha256,
                      EXISTS (SELECT 1 FROM morceaux m WHERE m.document_id = d.id
                              AND m.version_id = %s) AS a_v
                 FROM documents d WHERE d.source_id = %s""", (v, source_id))
    }
    emp = empreinte_indexation(conf)
    deja_exclus = {r["chemin"]: r["signature"] for r in conn.execute(
        "SELECT chemin, signature FROM exclus WHERE source_id = %s AND empreinte = %s",
        (source_id, emp))}
    absents = existants.keys() - entrees.keys()
    if not forcer and (not entrees or len(absents) > len(existants) / 2):
        conn.rollback()
        raise CoffrePartielError(
            f"{source.nom} : listage de {len(entrees)} entrées, {len(absents)} documents "
            f"sur {len(existants)} absents ; relancer avec forcer pour confirmer")

    repos = timedelta(minutes=source.repos_minutes)
    for chemin, e in sorted(entrees.items()):
        ex = existants.get(chemin)
        try:
            if deja_exclus.get(chemin) == e.signature:
                bilan.exclus += 1
                continue
            if ex and ex["signature"] == e.signature and ex["a_v"]:
                bilan.inchanges += 1
                continue
            if not sans_repos and source.repos_minutes and e.modifie_le > maintenant - repos:
                bilan.reportes += 1
                continue
            contenu = lecteur.lire(chemin)
            if raison := motif_exclusion(chemin, contenu, source.exclure):
                log.warning("%s : %s exclu (%s)", source.nom, chemin, raison)  # jamais le contenu
                if ex:
                    conn.execute("DELETE FROM documents WHERE id = %s", (ex["id"],))
                conn.execute(
                    """INSERT INTO exclus (source_id, chemin, signature, empreinte, raison)
                       VALUES (%s, %s, %s, %s, %s) ON CONFLICT (source_id, chemin) DO UPDATE
                       SET signature = EXCLUDED.signature, empreinte = EXCLUDED.empreinte,
                           raison = EXCLUDED.raison""",
                    (source_id, chemin, e.signature, emp, raison))
                conn.commit()
                bilan.exclus += 1
                continue
            sha = hashlib.sha256(contenu).hexdigest()
            if ex and ex["sha256"] == sha and ex["a_v"]:
                conn.execute("UPDATE documents SET signature = %s WHERE id = %s",
                             (e.signature, ex["id"]))
                conn.commit()
                bilan.inchanges += 1
                continue
            conn.execute("DELETE FROM exclus WHERE source_id = %s AND chemin = %s",
                         (source_id, chemin))
            note, ms = _morceaux(source, chemin, contenu, conf)
            bilan.morceaux += _ecrire(conn, source_id, chemin, e, sha, note, ms, conf, v,
                                      modeles, ex)
            conn.commit()
            if ex:
                bilan.modifies += 1
            else:
                bilan.nouveaux += 1
        except BaseException:
            conn.rollback()
            raise

    conn.execute("DELETE FROM exclus WHERE source_id = %s AND NOT (chemin = ANY(%s))",
                 (source_id, list(entrees)))
    for chemin in absents:
        conn.execute("DELETE FROM documents WHERE id = %s", (existants[chemin]["id"],))
        bilan.supprimes += 1
    conn.execute("UPDATE sources SET revision = %s, indexe_le = now() WHERE id = %s",
                 (lecteur.revision(), source_id))
    conn.commit()
    return bilan
