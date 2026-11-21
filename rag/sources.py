"""Lecteurs de source : même interface lister() / lire() / revision()."""

import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


class ListagePartielError(Exception):
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
        return LecteurS3(client_s3(reglages), reglages.coffre_bucket, reglages.coffre_prefixe)
    if source.type == "obsidian":
        return LecteurDossier(Path(source.emplacement).expanduser())
    raise NotImplementedError(f"lecteur {source.type} : plan 2")
