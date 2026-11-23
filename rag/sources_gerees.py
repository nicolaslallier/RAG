"""Sources git gérées en base : chiffrement des jetons (Coffret) et liste des sources actives."""

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

import yaml
from cryptography.fernet import Fernet, InvalidToken
from pydantic import ValidationError

from rag.config import Config, ErreurConfig, Reglages, SourceConf
from rag.gestion import (
    MotifInvalide,
    SourceInconnue,
    demander_reindexation,
    valider_motif,
    verrouiller_source,
)

log = logging.getLogger(__name__)


class JetonIllisible(Exception):
    """Le jeton stocké n'est pas déchiffrable avec la clé courante."""


class Coffret:
    """Chiffre les jetons d'accès avec une clé Fernet (RAG_CLE_SECRETS)."""

    def __init__(self, cle: str):
        self._fernet = Fernet(cle.encode())

    def chiffrer(self, jeton: str) -> bytes:
        return self._fernet.encrypt(jeton.encode())

    def dechiffrer(self, brut: bytes) -> str:
        try:
            return self._fernet.decrypt(bytes(brut)).decode()
        except InvalidToken as e:
            raise JetonIllisible("jeton illisible avec la clé courante") from e


def coffret_depuis(reglages: Reglages) -> Coffret | None:
    if not reglages.cle_secrets:
        return None
    try:
        return Coffret(reglages.cle_secrets)
    except ValueError as e:
        raise ErreurConfig("RAG_CLE_SECRETS invalide") from e


@dataclass(frozen=True)
class SourceActive:
    conf: SourceConf
    jeton: str | None = None
    utilisateur: str = "x-access-token"


def sources_actives(conn, config: Config, coffret: Coffret | None) -> list[SourceActive]:
    """Sources obsidian du YAML, puis sources git gérées en base (par nom).

    Les sources `code` du YAML sont ignorées : la base les remplace.
    """
    actives = [SourceActive(s) for s in config.sources if s.type == "obsidian"]
    for r in conn.execute("SELECT * FROM sources WHERE geree ORDER BY nom"):
        jeton = None
        if r["jeton_chiffre"] is not None:
            if coffret is None:
                log.warning("jeton ignoré pour la source %s : RAG_CLE_SECRETS absente", r["nom"])
            else:
                try:
                    jeton = coffret.dechiffrer(r["jeton_chiffre"])
                except JetonIllisible:
                    log.warning("jeton illisible pour la source %s : ignoré", r["nom"])
        conf = SourceConf(nom=r["nom"], type="code", emplacement=r["emplacement"],
                          branche=r["branche"] or "main", exclure=list(r["exclure"]),
                          visibilite=r["visibilite"])
        actives.append(SourceActive(conf, jeton, r["utilisateur"]))
    return actives


class SourceInvalide(ValueError):
    """Source refusée ; rien n'est écrit."""


class JetonImpossible(SourceInvalide):
    """Un jeton est fourni mais le coffret (RAG_CLE_SECRETS) est absent."""


_UTILISATEUR = re.compile(r"^[\w.@+-]+$")


def valider(nom: str, emplacement: str, branche: str, visibilite: str, exclure: list[str],
            utilisateur: str, *, noms_reserves: set[str]) -> SourceConf:
    """`SourceConf` de type code, ou `SourceInvalide`. `noms_reserves` : sources du YAML."""
    if nom in noms_reserves:
        raise SourceInvalide("nom réservé")
    if not isinstance(emplacement, str) or not emplacement.strip():
        raise SourceInvalide("l'URL est obligatoire")
    emplacement = emplacement.strip()
    try:
        u = urlsplit(emplacement)
        hote = u.hostname
        u.port  # noqa: B018 : lève ValueError si le port est invalide
    except ValueError as e:
        raise SourceInvalide("URL invalide") from e
    if u.scheme != "https" or not hote or not u.netloc or u.username or u.password \
            or re.search(r"\s", emplacement):
        raise SourceInvalide("l'URL doit être https://hôte/… sans identifiants")
    if not isinstance(branche, str) or not branche.strip() or branche.startswith("-") \
            or re.search(r"\s", branche):
        raise SourceInvalide("branche invalide")
    if not isinstance(utilisateur, str) or not _UTILISATEUR.match(utilisateur):
        raise SourceInvalide("nom d'utilisateur invalide")
    try:
        motifs = [valider_motif(m) for m in exclure]
    except MotifInvalide as e:
        raise SourceInvalide(str(e)) from e
    try:
        return SourceConf(nom=nom, type="code", emplacement=emplacement, branche=branche,
                          exclure=motifs, visibilite=visibilite)
    except ValidationError as e:
        raise SourceInvalide("nom ou visibilité invalide") from e


def _inserer_source(conn, conf: SourceConf, utilisateur: str,
                    chiffre: bytes | None) -> None:
    """INSERT d'une source gérée, sans commit (l'appelant tient la transaction)."""
    verrouiller_source(conn, conf.nom)
    if conn.execute("SELECT 1 FROM sources WHERE nom = %s", (conf.nom,)).fetchone():
        raise SourceInvalide("nom déjà pris")
    conn.execute(
        """INSERT INTO sources (nom, type, emplacement, branche, visibilite, exclure,
                                jeton_chiffre, utilisateur, geree, demande_le)
           VALUES (%s, 'code', %s, %s, %s, %s, %s, %s, true, now())""",
        (conf.nom, conf.emplacement, conf.branche, conf.visibilite, list(conf.exclure),
         chiffre, utilisateur))
    conn.execute("NOTIFY rag_reindexer")


def ajouter(conn, conf: SourceConf, *, utilisateur: str, jeton: str | None,
            coffret: Coffret | None) -> None:
    """Enregistre une source git gérée et demande son indexation."""
    if jeton and coffret is None:
        raise JetonImpossible("RAG_CLE_SECRETS absente : impossible d'enregistrer un jeton")
    chiffre = coffret.chiffrer(jeton) if jeton and coffret else None
    try:
        _inserer_source(conn, conf, utilisateur, chiffre)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def _origine(url: str) -> tuple[str, str, int | None]:
    try:
        u = urlsplit(url)
        return (u.scheme.lower(), (u.hostname or "").lower(), u.port)
    except ValueError:
        return (url, "", None)


def _geree(conn, nom: str) -> dict:
    r = conn.execute("SELECT * FROM sources WHERE nom = %s AND geree", (nom,)).fetchone()
    if r is None:
        raise SourceInconnue(nom)
    return r


def _appliquer_modification(conn, nom: str, conf: SourceConf, utilisateur: str,
                            nouveau_jeton: bytes | None, retirer_jeton: bool) -> set[str]:
    """Écrit les changements d'une source gérée, sans commit ; rend les champs modifiés."""
    verrouiller_source(conn, nom)
    r = _geree(conn, nom)
    changes: set[str] = set()
    if conf.emplacement != r["emplacement"]:
        changes.add("emplacement")
    if conf.branche != (r["branche"] or "main"):
        changes.add("branche")
    if conf.visibilite != r["visibilite"]:
        changes.add("visibilite")
    if list(conf.exclure) != list(r["exclure"]):
        changes.add("exclure")
    if utilisateur != r["utilisateur"]:
        changes.add("utilisateur")
    if nouveau_jeton is not None:
        changes.add("jeton")
    elif (retirer_jeton or _origine(conf.emplacement) != _origine(r["emplacement"])) \
            and r["jeton_chiffre"] is not None:
        # Un changement d'hôte ne doit jamais envoyer l'ancien jeton au nouveau.
        changes.add("jeton")
    if not changes:
        return changes
    if changes & {"emplacement", "branche"}:
        sid = r["id"]
        conn.execute("DELETE FROM documents WHERE source_id = %s", (sid,))
        conn.execute("DELETE FROM noeuds WHERE source_id = %s", (sid,))
        conn.execute("DELETE FROM exclus WHERE source_id = %s", (sid,))
        conn.execute("UPDATE sources SET revision = NULL, graphe_revision = NULL"
                     " WHERE id = %s", (sid,))
    conn.execute(
        """UPDATE sources SET emplacement = %s, branche = %s, visibilite = %s, exclure = %s,
               utilisateur = %s,
               sans_garde = sans_garde OR %s
           WHERE id = %s""",
        (conf.emplacement, conf.branche, conf.visibilite, list(conf.exclure), utilisateur,
         bool(changes & {"emplacement", "branche", "exclure"}), r["id"]))
    if "jeton" in changes:
        conn.execute("UPDATE sources SET jeton_chiffre = %s WHERE id = %s",
                     (nouveau_jeton, r["id"]))
    return changes


def modifier(conn, nom: str, conf: SourceConf, *, utilisateur: str, jeton: str | None,
             retirer_jeton: bool, coffret: Coffret | None) -> set[str]:
    """Applique les changements ; rend les champs modifiés (voir la spec, « Effets »)."""
    if jeton and coffret is None:
        raise JetonImpossible("RAG_CLE_SECRETS absente : impossible d'enregistrer un jeton")
    try:
        changes = _appliquer_modification(
            conn, nom, conf, utilisateur,
            coffret.chiffrer(jeton) if jeton and coffret else None, retirer_jeton)
        if not changes:
            conn.rollback()
            return changes
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    if changes != {"visibilite"}:
        demander_reindexation(conn, nom)
    return changes


def bilan_suppression(conn, nom: str) -> dict:
    """Ce qu'effacerait `supprimer` ; `SourceInconnue` si absente ou non gérée."""
    r = _geree(conn, nom)
    n = lambda sql, *a: conn.execute(sql, a).fetchone()["n"]
    return {
        "nom": nom,
        "emplacement": r["emplacement"],
        "documents": n("SELECT count(*) AS n FROM documents WHERE source_id = %s", r["id"]),
        "exclus": n("SELECT count(*) AS n FROM exclus WHERE source_id = %s", r["id"]),
        "exclusions_manuelles": n(
            "SELECT count(*) AS n FROM exclusions_manuelles WHERE source = %s", nom),
    }


def supprimer(conn, nom: str) -> None:
    """Efface la source gérée et tout ce qui en vient (cascade) ; `SourceInconnue` sinon."""
    try:
        verrouiller_source(conn, nom)
        _geree(conn, nom)
        conn.execute("DELETE FROM exclusions_manuelles WHERE source = %s", (nom,))
        conn.execute("DELETE FROM sources WHERE nom = %s AND geree", (nom,))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


# --- export / import YAML ---

_CLES_EXPORT = ("nom", "emplacement", "branche", "visibilite", "exclure", "utilisateur")
_DEFAUTS = {"branche": "main", "visibilite": "public-maison", "exclure": [],
            "utilisateur": "x-access-token"}


def exporter(conn) -> str:
    """YAML des sources gérées, triées par nom. Jamais de jeton."""
    lignes = [{"nom": r["nom"], "emplacement": r["emplacement"],
               "branche": r["branche"] or "main", "visibilite": r["visibilite"],
               "exclure": list(r["exclure"]), "utilisateur": r["utilisateur"]}
              for r in conn.execute("SELECT * FROM sources WHERE geree ORDER BY nom")]
    return yaml.safe_dump(lignes, allow_unicode=True, sort_keys=False)


def lire_export(texte: str, *, noms_reserves: set[str] = frozenset()) -> list[dict]:
    """Entrées validées (défauts appliqués) ou `SourceInvalide("entrée i : …")`."""
    try:
        donnees = yaml.safe_load(texte)
    except yaml.YAMLError as e:
        raise SourceInvalide("YAML illisible") from e
    if donnees is None:
        return []
    if not isinstance(donnees, list):
        raise SourceInvalide("une liste de sources est attendue")
    sorties: list[dict] = []
    vus: set[str] = set()
    for i, e in enumerate(donnees, 1):
        try:
            if not isinstance(e, dict):
                raise SourceInvalide("un dictionnaire est attendu")
            inconnues = set(e) - set(_CLES_EXPORT)
            if inconnues:
                raise SourceInvalide("clé(s) inconnue(s) : "
                                     + ", ".join(sorted(map(str, inconnues))))
            for k in ("nom", "emplacement"):
                if k not in e:
                    raise SourceInvalide(f"« {k} » est obligatoire")
            d = {**_DEFAUTS, **e}
            d["exclure"] = list(d["exclure"]) if isinstance(d["exclure"], list) else None
            if d["exclure"] is None:
                raise SourceInvalide("« exclure » doit être une liste")
            if not isinstance(d["nom"], str):
                raise SourceInvalide("nom invalide")
            if d["nom"] in vus:
                raise SourceInvalide("nom en double")
            conf = valider(d["nom"], d["emplacement"], d["branche"], d["visibilite"],
                           d["exclure"], d["utilisateur"], noms_reserves=noms_reserves)
        except SourceInvalide as err:
            raise SourceInvalide(f"entrée {i} : {err}") from err
        vus.add(conf.nom)
        sorties.append({"nom": conf.nom, "emplacement": conf.emplacement,
                        "branche": conf.branche, "visibilite": conf.visibilite,
                        "exclure": list(conf.exclure), "utilisateur": d["utilisateur"]})
    return sorties


def importer(conn, texte: str, *, noms_reserves: set[str]) -> dict:
    """Ajoute ou met à jour les sources du YAML en une transaction ; rien n'est supprimé.

    Jamais de jeton : celui d'une source connue est gardé (sauf changement d'hôte).
    """
    entrees = lire_export(texte, noms_reserves=noms_reserves)
    bilan = {"ajoutees": 0, "modifiees": 0, "inchangees": 0}
    try:
        a_reindexer: list[str] = []
        for e in entrees:
            conf = SourceConf(nom=e["nom"], type="code", emplacement=e["emplacement"],
                              branche=e["branche"], exclure=e["exclure"],
                              visibilite=e["visibilite"])
            verrouiller_source(conn, conf.nom)
            if conn.execute("SELECT 1 FROM sources WHERE nom = %s AND geree",
                            (conf.nom,)).fetchone():
                changes = _appliquer_modification(conn, conf.nom, conf, e["utilisateur"],
                                                  None, False)
                if not changes:
                    bilan["inchangees"] += 1
                    continue
                bilan["modifiees"] += 1
                if changes != {"visibilite"}:
                    a_reindexer.append(conf.nom)
            else:
                _inserer_source(conn, conf, e["utilisateur"], None)
                bilan["ajoutees"] += 1
        if a_reindexer:
            conn.execute("UPDATE sources SET demande_le = now() WHERE nom = ANY(%s)",
                         (a_reindexer,))
            conn.execute("NOTIFY rag_reindexer")
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return bilan


_VERROU_AMORCE = 7242   # à côté de VERROU_MIGRATION (7240) et du travailleur (7241)


def amorcer(conn, chemin: Path) -> int:
    """Amorçage unique : verse les sources du fichier en base, sans réindexer.

    Upsert par nom (une ligne déjà indexée garde id, révision et documents), `geree = true`,
    ni `demande_le` ni `sans_garde` touchés. Le marqueur `instance.sources_amorcees` est posé
    dans la même transaction : un second appel rend 0. Rend le nombre de sources versées.
    """
    entrees = lire_export(Path(chemin).read_text(encoding="utf-8"))
    try:
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (_VERROU_AMORCE,))
        if conn.execute("SELECT 1 FROM instance WHERE cle = 'sources_amorcees'").fetchone():
            conn.rollback()
            return 0
        for e in entrees:
            verrouiller_source(conn, e["nom"])
            conn.execute(
                """INSERT INTO sources (nom, type, emplacement, branche, visibilite, exclure,
                                        utilisateur, geree)
                   VALUES (%s, 'code', %s, %s, %s, %s, %s, true)
                   ON CONFLICT (nom) DO UPDATE SET
                     type = 'code', geree = true, emplacement = EXCLUDED.emplacement,
                     branche = EXCLUDED.branche, visibilite = EXCLUDED.visibilite,
                     exclure = EXCLUDED.exclure, utilisateur = EXCLUDED.utilisateur""",
                (e["nom"], e["emplacement"], e["branche"], e["visibilite"], e["exclure"],
                 e["utilisateur"]))
        noms = [e["nom"] for e in entrees]
        for r in conn.execute("SELECT nom FROM sources WHERE type = 'code' AND NOT geree"
                              " AND NOT (nom = ANY(%s)) ORDER BY nom", (noms,)).fetchall():
            log.warning("source de code %s absente du fichier d'amorçage : gardée telle quelle",
                        r["nom"])
        conn.execute("INSERT INTO instance (cle, valeur) VALUES ('sources_amorcees', 'oui')")
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return len(entrees)
