"""Indexeur incrémental : signature, puis sha256 ; une transaction par document."""

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import psycopg
from psycopg.types.json import Jsonb

from rag import quarantaine
from rag.config import IndexationConf, SourceConf, empreinte_indexation, plafond_lecture
from rag.decoupage import Morceau, decouper_note
from rag.decoupage_code import decouper_code, langage
from rag.exclusions import affiner, motif_exclusion
from rag.gestion import SourceSupprimee, correspond, motifs_manuels, verrouiller_source
from rag.modeles import ErreurModele
from rag.notes import Note, analyser_note
from rag.recherche import normaliser, vecteur_sql
from rag.sources import ErreurSource, nfc

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
    echecs: int = 0  # documents en échec, retentés à la passe suivante


ECHECS_MAX = 20  # au 20e échec, la panne est celle de la source, pas d'un document
_REFUS_DU_DOCUMENT = re.compile(r"HTTP (400|413|422)$")


def _arret_global(exc: Exception) -> bool:
    """Pannes qui ne tiennent pas au document : base, modèle injoignable, source disparue.

    Postgres en liste blanche : seule une `DataError` (valeur refusée, caractère intraduisible…)
    tient au document ; toute autre erreur de la base arrête la passe."""
    if isinstance(exc, SourceSupprimee):
        return True
    if isinstance(exc, psycopg.Error):
        return not isinstance(exc, psycopg.DataError)
    if isinstance(exc, ErreurModele):  # un 400/413/422 vise l'entrée ; le reste est une panne
        return not _REFUS_DU_DOCUMENT.search(str(exc))
    return False


def _resume(exc: Exception) -> str:
    """Pour le journal : jamais le message d'une erreur Postgres (il peut citer le contenu)."""
    if isinstance(exc, psycopg.Error):
        return f"{type(exc).__name__} {exc.sqlstate or ''}".strip()
    if isinstance(exc, (ErreurModele, ErreurSource)):
        return f"{type(exc).__name__} : {exc}"
    return type(exc).__name__


def version_courante(conn, conf: IndexationConf) -> int:
    emp = empreinte_indexation(conf)
    conn.execute("INSERT INTO index_versions (empreinte, config) VALUES (%s, %s) "
                 "ON CONFLICT (empreinte) DO NOTHING", (emp, json.dumps(conf.model_dump())))
    v = conn.execute("SELECT id FROM index_versions WHERE empreinte = %s", (emp,)).fetchone()["id"]
    conn.commit()
    return v


def _morceaux(source: SourceConf, chemin: str, contenu: bytes, conf: IndexationConf):
    texte = contenu.decode("utf-8", errors="replace").replace("\x00", "")
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


def _vecteurs(ms, conf: IndexationConf, modeles) -> dict[int, str]:
    """Embeddings des morceaux cherchables, par ordre ; calculés avant toute écriture."""
    cherchables = [m for m in ms if m.cherchable]
    vecs = modeles.embed_documents([f"{m.fil}\n\n{m.texte}" for m in cherchables],
                                   conf.modele_embedding) if cherchables else []
    return {m.ordre: vecteur_sql(x) for m, x in zip(cherchables, vecs, strict=True)}


def _ecrire(conn, source_id, chemin, e, sha, note, ms, v, vec_de, existant) -> int:
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


def _verrouiller_identite(conn, source: SourceConf, source_id: int) -> None:
    """Verrou de la source, puis identité relue : une source code modifiée (URL, branche)
    ou supprimée depuis le début de la passe n'est plus écrite."""
    verrouiller_source(conn, source.nom)
    if source.type != "code":
        return
    if conn.execute(
            """SELECT 1 FROM sources WHERE id = %s AND nom = %s AND type = 'code'
                  AND emplacement = %s AND COALESCE(branche, 'main') = %s""",
            (source_id, source.nom, source.emplacement, source.branche or "main")
            ).fetchone() is None:
        conn.rollback()
        raise SourceSupprimee(f"{source.nom} : source changée ou supprimée")


def indexer_source(conn, source: SourceConf, lecteur, conf: IndexationConf, modeles, *,
                   forcer: bool = False, sans_repos: bool = False,
                   maintenant: datetime | None = None) -> Bilan:
    maintenant = maintenant or datetime.now(UTC)
    bilan = Bilan()
    if source.type == "code":  # gérée en base : une source supprimée ne se recrée jamais
        r = conn.execute(
            """SELECT id FROM sources WHERE nom = %s AND type = 'code' AND emplacement = %s
                  AND COALESCE(branche, 'main') = %s""",
            (source.nom, source.emplacement, source.branche or "main")).fetchone()
        if r is None:
            conn.rollback()
            raise SourceSupprimee(f"{source.nom} : source changée ou supprimée")
        source_id = r["id"]
    else:
        source_id = conn.execute(
            """INSERT INTO sources (nom, type, emplacement, branche, visibilite)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (nom) DO UPDATE SET type=EXCLUDED.type, emplacement=EXCLUDED.emplacement,
                 branche=EXCLUDED.branche, visibilite=EXCLUDED.visibilite RETURNING id""",
            (source.nom, source.type, source.emplacement, source.branche, source.visibilite),
        ).fetchone()["id"]
    conn.commit()
    v = version_courante(conn, conf)
    motifs = motifs_manuels(conn, source.nom)
    acceptes = quarantaine.levees(conn, source.nom)
    conn.commit()

    def levees_de(chemin, e, sha256: str | None = None) -> frozenset[str]:
        """Règles levées pour ce fichier, si sa signature est celle qui a été acceptée ;
        `contenu_secret` seulement si le contenu lu (`sha256`) est celui qui a été accepté."""
        lv = acceptes.get(chemin)
        return lv.pour(e.signature, sha256) if lv else frozenset()

    entrees = {}
    exclus_listage = {}  # chemin -> (entrée, raison, détail) : écartés sur le nom, jamais lus
    for e in lecteur.lister():
        chemin = nfc(e.chemin)
        if source.type == "obsidian" and not chemin.endswith(".md"):
            continue  # pièces jointes, .gitignore, .DS_Store : hors coffre de notes
        raison, detail = affiner(motif_exclusion(chemin, b"", source.exclure), chemin, b"",
                                 source.exclure, levees_de(chemin, e))
        if raison is None and correspond(chemin, motifs):
            raison = "manuelle"
        if raison is None and e.taille > plafond_lecture(conf):
            raison = "trop_gros"  # jamais lu : un blob géant ne passe pas en mémoire
            log.warning("%s : %r exclu (trop_gros, %d octets)", source.nom, chemin, e.taille)
        if raison is None:
            entrees[chemin] = e
            continue
        exclus_listage[chemin] = (e, raison, detail)
        if raison not in ("config", "manuelle"):
            bilan.exclus += 1  # secret, verrou, généré : exclus sur le nom, jamais lus
    existants = {
        r["chemin"]: r for r in conn.execute(
            """SELECT d.id, d.chemin, d.signature, d.sha256,
                      EXISTS (SELECT 1 FROM morceaux m WHERE m.document_id = d.id
                              AND m.version_id = %s) AS a_v
                 FROM documents d WHERE d.source_id = %s""", (v, source_id))
    }
    emp = empreinte_indexation(conf)
    deja_exclus = {r["chemin"]: (r["signature"], r["regle"]) for r in conn.execute(
        "SELECT chemin, signature, detail->>'regle' AS regle FROM exclus "
        "WHERE source_id = %s AND empreinte = %s AND lu", (source_id, emp))}
    absents = existants.keys() - entrees.keys()
    # exclus à la main : voulus, ils ne trahissent pas un listage partiel
    manuels = {c for c, (_, raison, _) in exclus_listage.items() if raison == "manuelle"}
    manquants = absents - manuels
    # rien d'indexé encore : un listage vide n'efface rien, ce n'est pas un échec
    if not forcer and existants and ((not entrees and not manuels)
                                     or len(manquants) > len(existants) / 2):
        conn.rollback()
        raise CoffrePartielError(
            f"{source.nom} : listage de {len(entrees)} entrées, {len(manquants)} documents "
            f"sur {len(existants)} absents ; relancer avec forcer pour confirmer")
    if not entrees and not existants:
        log.warning("%s : aucun fichier indexable", source.nom)

    # le listage (clone ou fetch) est la fenêtre la plus longue : identité relue avant d'écrire
    _verrouiller_identite(conn, source, source_id)
    for chemin, (e, raison, detail) in exclus_listage.items():
        conn.execute(
            """INSERT INTO exclus (source_id, chemin, signature, empreinte, raison, lu, detail,
                                  contenu_sha256)
               VALUES (%s, %s, %s, %s, %s, false, %s, NULL) ON CONFLICT (source_id, chemin)
               DO UPDATE SET signature = EXCLUDED.signature, empreinte = EXCLUDED.empreinte,
                   raison = EXCLUDED.raison, lu = false, detail = EXCLUDED.detail,
                   contenu_sha256 = NULL""",
            (source_id, chemin, e.signature, emp, raison, Jsonb(detail) if detail else None))
    conn.commit()  # pas de verrou de ligne sur `exclus` pendant le travail lent qui suit

    repos = timedelta(minutes=source.repos_minutes)
    exclus_en_passe = set()  # exclus à la main pendant la passe

    def exclu_en_passe(chemin, e, ex) -> bool:
        """Relit les motifs manuels ; si le chemin vient d'être exclu, le retire et valide."""
        if not correspond(chemin, motifs_manuels(conn, source.nom)):
            return False
        _verrouiller_identite(conn, source, source_id)
        if ex:
            conn.execute("DELETE FROM documents WHERE id = %s", (ex["id"],))
        conn.execute(
            """INSERT INTO exclus (source_id, chemin, signature, empreinte, raison, lu, detail,
                                  contenu_sha256)
               VALUES (%s, %s, %s, %s, 'manuelle', false, NULL, NULL)
               ON CONFLICT (source_id, chemin) DO UPDATE
               SET signature = EXCLUDED.signature, empreinte = EXCLUDED.empreinte,
                   raison = EXCLUDED.raison, lu = false, detail = NULL, contenu_sha256 = NULL""",
            (source_id, chemin, e.signature, emp))
        conn.commit()
        exclus_en_passe.add(chemin)
        return True

    for chemin, e in sorted(entrees.items()):
        ex = existants.get(chemin)
        try:
            deja = deja_exclus.get(chemin)
            if deja and deja[0] == e.signature and deja[1] not in levees_de(chemin, e):
                bilan.exclus += 1
                continue
            if ex and ex["signature"] == e.signature and ex["a_v"]:
                bilan.inchanges += 1
                continue
            if not sans_repos and source.repos_minutes and e.modifie_le > maintenant - repos:
                bilan.reportes += 1
                continue
            if exclu_en_passe(chemin, e, ex):  # exclu depuis le listage : jamais lu
                continue
            contenu = lecteur.lire(chemin)
            sha = hashlib.sha256(contenu).hexdigest()
            brut = motif_exclusion(chemin, contenu, source.exclure)
            raison, detail = affiner(brut, chemin, contenu, source.exclure,
                                     levees_de(chemin, e, sha))
            if raison:
                log.warning("%s : %r exclu (%s)", source.nom, chemin, raison)  # jamais le contenu
                _verrouiller_identite(conn, source, source_id)
                if ex:
                    conn.execute("DELETE FROM documents WHERE id = %s", (ex["id"],))
                conn.execute(
                    """INSERT INTO exclus (source_id, chemin, signature, empreinte, raison, lu,
                                          detail, contenu_sha256)
                       VALUES (%s, %s, %s, %s, %s, true, %s, %s) ON CONFLICT (source_id, chemin)
                       DO UPDATE SET signature = EXCLUDED.signature, empreinte = EXCLUDED.empreinte,
                           raison = EXCLUDED.raison, lu = true, detail = EXCLUDED.detail,
                           contenu_sha256 = EXCLUDED.contenu_sha256""",
                    (source_id, chemin, e.signature, emp, raison,
                     Jsonb(detail) if detail else None, sha))
                conn.commit()
                bilan.exclus += 1
                continue
            par_levee = brut is not None  # indexé seulement grâce à une acceptation
            if ex and ex["sha256"] == sha and ex["a_v"]:
                conn.execute("UPDATE documents SET signature = %s WHERE id = %s",
                             (e.signature, ex["id"]))
                conn.commit()
                bilan.inchanges += 1
                continue
            note, ms = _morceaux(source, chemin, contenu, conf)
            vec_de = _vecteurs(ms, conf, modeles)
            # relu après les embeddings (longs), dans la transaction de l'écriture, sous le
            # verrou de la source : une exclusion validée entre-temps est vue, une exclusion
            # qui arrive attend le commit puis retire le document écrit
            _verrouiller_identite(conn, source, source_id)
            if exclu_en_passe(chemin, e, ex):
                continue
            # acceptation retirée ou changée pendant la passe : rien n'est écrit, reprise à la
            # suivante
            if par_levee and quarantaine.levees(conn, source.nom).get(chemin) != acceptes.get(chemin):
                conn.rollback()
                bilan.reportes += 1
                continue
            conn.execute("DELETE FROM exclus WHERE source_id = %s AND chemin = %s",
                         (source_id, chemin))
            bilan.morceaux += _ecrire(conn, source_id, chemin, e, sha, note, ms, v, vec_de, ex)
            conn.commit()
            if ex:
                bilan.modifies += 1
            else:
                bilan.nouveaux += 1
        except Exception as exc:
            conn.rollback()
            if _arret_global(exc):
                raise
            bilan.echecs += 1
            log.error("%s : %r en échec (%s)", source.nom, chemin, _resume(exc))
            if bilan.echecs >= ECHECS_MAX:  # sans l'erreur d'origine : elle peut citer le contenu
                raise RuntimeError(f"{source.nom} : trop de documents en échec "
                                   f"({bilan.echecs})") from None
        except BaseException:  # KeyboardInterrupt, SystemExit : arrêt net
            conn.rollback()
            raise

    # oublie les fichiers disparus du listage, et les exclusions sur le nom levées depuis ; garde
    # les exclusions manuelles posées pendant la passe (motif encore en base) ; jamais la
    # révision d'un ancien dépôt sur une source modifiée entre-temps
    _verrouiller_identite(conn, source, source_id)
    motifs = motifs_manuels(conn, source.nom)
    manuelles = [r["chemin"] for r in conn.execute(
        "SELECT chemin FROM exclus WHERE source_id = %s AND raison = 'manuelle'", (source_id,))
        if correspond(r["chemin"], motifs)]
    conn.execute(
        """DELETE FROM exclus WHERE source_id = %s
             AND (NOT (chemin = ANY(%s)) OR (NOT lu AND NOT (chemin = ANY(%s))))""",
        (source_id, [*entrees, *exclus_listage],
         [*exclus_listage, *exclus_en_passe, *manuelles]))
    for chemin in absents:
        conn.execute("DELETE FROM documents WHERE id = %s", (existants[chemin]["id"],))
        bilan.supprimes += 1
    conn.execute("UPDATE sources SET revision = %s, indexe_le = now() WHERE id = %s",
                 (lecteur.revision(), source_id))
    conn.commit()
    return bilan
