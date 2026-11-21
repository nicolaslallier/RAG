"""Lecteurs de source : même interface lister() / lire() / revision()."""

import base64
import os
import subprocess
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


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


class LecteurGit:
    """Clone propre au service, qui suit `origin/<branche>` (réécritures d'historique comprises)."""

    def __init__(self, url: str, dest: Path, branche: str = "main", jeton: str = ""):
        self.url, self.dest, self.branche = url, Path(dest).absolute(), branche
        self._secrets = []
        self._env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
        if jeton:  # jamais dans une URL ni un argument : par l'environnement de git seulement
            b64 = base64.b64encode(f"x-access-token:{jeton}".encode()).decode()
            self._secrets = [jeton, b64]
            self._env |= {"GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "http.extraheader",
                          "GIT_CONFIG_VALUE_0": f"AUTHORIZATION: basic {b64}"}
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
            msg = r.stderr.decode(errors="replace").strip()
            for s in self._secrets:
                msg = msg.replace(s, "***")
            raise ErreurSource(f"git {args[0]} : {msg}")
        return r.stdout.decode(errors="replace")

    def synchroniser(self) -> str:
        if not (self.dest / ".git").exists():
            self.dest.parent.mkdir(parents=True, exist_ok=True)
            self._git("clone", "--single-branch", "--branch", self.branche, self.url,
                      str(self.dest), cwd=self.dest.parent)
        else:
            self._git("fetch", "origin", self.branche)
            self._git("reset", "--hard", f"origin/{self.branche}")
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
            out.append(Entree(nfc(chemin), sha, datetime.min.replace(tzinfo=UTC), int(taille)))
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


def lecteur_pour(source, reglages):
    if source.emplacement == "s3":
        p = reglages.coffre_prefixe.strip()
        prefixe = p.rstrip("/") + "/" if p.strip("/") else ""
        return LecteurS3(client_s3(reglages), reglages.coffre_bucket, prefixe)
    if source.type == "obsidian":
        return LecteurDossier(Path(source.emplacement).expanduser())
    return LecteurGit(source.emplacement, Path(reglages.donnees_dir) / "clones" / source.nom,
                      source.branche, reglages.github_jeton)
