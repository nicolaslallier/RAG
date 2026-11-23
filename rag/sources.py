"""Lecteurs de source : même interface lister() / lire() / revision()."""

import base64
import logging
import os
import shutil
import subprocess
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

log = logging.getLogger(__name__)


class ListagePartielError(Exception):
    pass


class ErreurSource(Exception):
    pass


@dataclass(frozen=True)
class Entree:
    chemin: str
    signature: str
    modifie_le: datetime
    taille: int


def nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s)


class LecteurDossier:
    def __init__(self, racine: Path):
        self.racine = Path(racine)

    def lister(self) -> list[Entree]:
        entrees = []
        for p in sorted(self.racine.rglob("*")):
            rel = p.relative_to(self.racine)
            if rel.parts[0] == ".git" or not p.is_file():
                continue
            st = p.stat()
            entrees.append(Entree(nfc(rel.as_posix()), f"{st.st_mtime_ns}-{st.st_size}",
                                  datetime.fromtimestamp(st.st_mtime, UTC), st.st_size))
        return entrees

    def lire(self, chemin: str) -> bytes:
        p = self.racine / chemin
        if not p.exists():  # le disque peut garder la forme NFD (macOS)
            p = self.racine / unicodedata.normalize("NFD", chemin)
        return p.read_bytes()

    def revision(self) -> str:
        return datetime.now(UTC).isoformat()


class LecteurS3:
    """Lecture seule : list_objects_v2 et get_object, rien d'autre."""

    def __init__(self, client, bucket: str, prefixe: str = ""):
        self.client, self.bucket, self.prefixe = client, bucket, prefixe
        self._cles: dict[str, str] = {}
        self._quand = ""

    def lister(self) -> list[Entree]:
        entrees, cles, jeton = [], {}, None
        while True:
            args = {"Bucket": self.bucket, "Prefix": self.prefixe}
            if jeton:
                args["ContinuationToken"] = jeton
            page = self.client.list_objects_v2(**args)
            for o in page.get("Contents", []):
                cle = o["Key"]
                if cle.endswith("/"):
                    continue
                chemin = nfc(cle[len(self.prefixe):])
                cles[chemin] = cle
                entrees.append(Entree(chemin, o["ETag"].strip('"'), o["LastModified"], o["Size"]))
            if not page.get("IsTruncated"):
                break
            jeton = page.get("NextContinuationToken")
            if not jeton:
                raise ListagePartielError("listage S3 tronqué sans jeton de suite")
        self._cles, self._quand = cles, datetime.now(UTC).isoformat()
        return entrees

    def lire(self, chemin: str) -> bytes:
        cle = self._cles.get(chemin, self.prefixe + chemin)
        return self.client.get_object(Bucket=self.bucket, Key=cle)["Body"].read()

    def revision(self) -> str:
        return self._quand


def _env_git(url: str, jeton: str, utilisateur: str,
             protocoles: str | None) -> tuple[dict, list[str]]:
    """Environnement de git et secrets à masquer. Le jeton ne passe jamais par l'URL ni un argument."""
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    secrets: list[str] = []
    if protocoles is not None:
        env["GIT_ALLOW_PROTOCOL"] = protocoles
    parts = urlsplit(url)
    if jeton and parts.scheme in ("http", "https") and parts.hostname:
        b64 = base64.b64encode(f"{utilisateur}:{jeton}".encode()).decode()
        secrets = [jeton, b64]
        hote = parts.hostname + (f":{parts.port}" if parts.port else "")
        env |= {"GIT_CONFIG_COUNT": "1",
                "GIT_CONFIG_KEY_0": f"http.{parts.scheme}://{hote}/.extraheader",
                "GIT_CONFIG_VALUE_0": f"AUTHORIZATION: basic {b64}"}
    return env, secrets


def _masquer(msg: str, secrets: list[str]) -> str:
    for s in secrets:
        msg = msg.replace(s, "***")
    return msg


def verifier_depot(url: str, branche: str, jeton: str = "", utilisateur: str = "x-access-token",
                   *, protocoles: str | None = "https", delai_s: int = 30) -> None:
    """Lève ErreurSource si le dépôt est injoignable ou si la branche n'existe pas."""
    env, secrets = _env_git(url, jeton, utilisateur, protocoles)
    try:
        r = subprocess.run(["git", "ls-remote", "--heads", "--", url, branche], check=False,
                           env=env, capture_output=True, timeout=delai_s)
    except subprocess.TimeoutExpired:
        raise ErreurSource("git ls-remote : délai dépassé") from None
    except OSError as e:
        raise ErreurSource(f"git ls-remote : {type(e).__name__}") from None
    if r.returncode:
        msg = _masquer(r.stderr.decode(errors="replace").strip(), secrets)
        raise ErreurSource(f"git ls-remote : {msg}")
    if not r.stdout.strip():
        raise ErreurSource(f"branche introuvable : {branche}")


class LecteurGit:
    """Clone propre au service, qui suit `origin/<branche>` (réécritures d'historique comprises)."""

    def __init__(self, url: str, dest: Path, branche: str = "main", jeton: str = "",
                 utilisateur: str = "x-access-token", protocoles: str | None = None):
        self.url, self.dest, self.branche = url, Path(dest).absolute(), branche
        self._env, self._secrets = _env_git(url, jeton, utilisateur, protocoles)
        self._sha = ""

    def _git(self, *args: str, cwd: Path | None = None) -> str:
        try:
            r = subprocess.run(["git", *args], check=False, cwd=cwd or self.dest,
                               env=self._env, capture_output=True, timeout=600)
        except subprocess.TimeoutExpired:
            raise ErreurSource(f"git {args[0]} : délai dépassé") from None
        except OSError as e:
            raise ErreurSource(f"git {args[0]} : {type(e).__name__}") from None
        if r.returncode:
            msg = _masquer(r.stderr.decode(errors="replace").strip(), self._secrets)
            raise ErreurSource(f"git {args[0]} : {msg}")
        return r.stdout.decode(errors="replace")

    def _cloner(self) -> None:
        if self.dest.parent.name != "clones":  # jamais de rmtree hors du dossier des clones
            raise ErreurSource(f"destination refusée : {self.dest}")
        shutil.rmtree(self.dest, ignore_errors=True)
        self.dest.parent.mkdir(parents=True, exist_ok=True)
        self._git("clone", "--single-branch", "--branch", self.branche, self.url,
                  str(self.dest), cwd=self.dest.parent)

    def synchroniser(self) -> str:
        if not (self.dest / ".git").exists():
            self._cloner()
        else:
            b = self.branche
            try:
                if self._git("remote", "get-url", "origin").strip() != self.url:
                    raise ErreurSource("URL d'origine changée")
                self._git("fetch", "origin", f"+{b}:refs/remotes/origin/{b}")
                self._git("reset", "--hard", f"origin/{b}")
            except ErreurSource as e:  # clone abîmé : on repart de zéro (il appartient au service)
                log.warning("%s : clone recréé (%s)", self.dest.name, e)
                self._cloner()
        self._sha = self._git("rev-parse", "HEAD").strip()
        return self._sha

    def lister(self) -> list[Entree]:
        if not self._sha:
            self.synchroniser()
        out = []
        for ligne in self._git("ls-tree", "-r", "-z", "--long", "HEAD").split("\0"):
            if not ligne:
                continue
            meta, chemin = ligne.split("\t", 1)
            mode, _type, sha, taille = meta.split()
            if mode in ("160000", "120000"):  # sous-module, lien symbolique : jamais suivis
                continue
            if nfc(chemin) != chemin or "\ufffd" in chemin:  # NFD, nom non UTF-8 : ignoré seul
                log.warning("%s : chemin ignoré (nom non normalisé) : %r", self.dest.name, chemin)
                continue
            out.append(Entree(chemin, sha, datetime.min.replace(tzinfo=UTC), int(taille)))
        return out

    def lire(self, chemin: str) -> bytes:
        """Le blob du commit indexé, jamais le système de fichiers (liens symboliques)."""
        if not self._sha:
            self.synchroniser()
        if chemin.startswith("/") or ".." in chemin.split("/"):
            raise ErreurSource(f"chemin refusé : {chemin}")
        try:
            r = subprocess.run(["git", "cat-file", "blob", f"{self._sha}:{chemin}"], check=False,
                               cwd=self.dest, env=self._env, capture_output=True, timeout=120)
        except subprocess.TimeoutExpired:
            raise ErreurSource("git cat-file : délai dépassé") from None
        if r.returncode:
            raise ErreurSource(f"git cat-file : {chemin} illisible")
        return r.stdout

    def revision(self) -> str:
        return self._sha


def client_s3(reglages):
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3", endpoint_url=reglages.s3_endpoint, aws_access_key_id=reglages.s3_access_key,
        aws_secret_access_key=reglages.s3_secret_key, region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def lecteur_pour(source, reglages, jeton: str | None = None, utilisateur: str = "x-access-token"):
    if source.emplacement == "s3":
        p = reglages.coffre_prefixe.strip()
        prefixe = p.rstrip("/") + "/" if p.strip("/") else ""
        return LecteurS3(client_s3(reglages), reglages.coffre_bucket, prefixe)
    if source.type == "obsidian":
        return LecteurDossier(Path(source.emplacement).expanduser())
    if jeton is None:  # le jeton général ne sert qu'à github.com
        hote = urlsplit(source.emplacement).hostname
        jeton = reglages.github_jeton if hote == "github.com" else ""
    return LecteurGit(source.emplacement, Path(reglages.donnees_dir) / "clones" / source.nom,
                      source.branche, jeton, utilisateur, protocoles="https")
