"""Ce qui n'est jamais indexé : config, secrets, fichiers générés, binaires."""
# Changer ce fichier change ce qui est indexé : incrémenter VERSION_ANALYSEUR (rag/config.py).

import re
from dataclasses import dataclass
from fnmatch import fnmatch
from itertools import islice

_NOMS_SECRETS = [".env", ".env.*", "*.pem", "*.key", "*.p12", "id_rsa*", "id_ed25519*",
                 "seal.key", ".runner.env", ".openbao.env", ".portainer.env",
                 ".npmrc", ".netrc", "*.pfx", "*.jks", "*.tfstate", "*.tfstate.backup",
                 "*.tfvars", "credentials.json", "kubeconfig", "*.kdbx"]  # en minuscules
_CONTENU_SECRET = re.compile(
    rb"-----BEGIN [A-Z ]*PRIVATE KEY( BLOCK)?-----|(?<![A-Za-z0-9_-])sk-(?:proj-|ant-|svcacct-)?[A-Za-z0-9_-]{20,}|ghp_[A-Za-z0-9]{36}"
    rb"|github_pat_[A-Za-z0-9_]{20,}|AKIA[0-9A-Z]{16}"
    rb"|hv[sb]\.[A-Za-z0-9_-]{20,}|(?<![A-Za-z0-9_.-])s\.[A-Za-z0-9]{24,}"
    rb"|(?<![A-Za-z0-9_-])glpat-[A-Za-z0-9_-]{20,}"
    # Slack : « xox?-<n>-… » (rotation, xoxe.xoxp-1-…) ou deux segments d'au moins 8 caractères,
    # jamais « xoxb-your-token-here » ; jetons d'application « xapp-<n>-… ». Classes disjointes
    # des séparateurs : temps linéaire.
    rb"|(?<![A-Za-z0-9_-])xox[abeposr]-(?:\d-[A-Za-z0-9-]{20,}|[A-Za-z0-9]{8,}-[A-Za-z0-9-]{8,})"
    rb"|(?<![A-Za-z0-9_-])xapp-\d-[A-Za-z0-9]+-\d+-[A-Za-z0-9]{16,}"
)
_GENERES = ["uv.lock", "Cargo.lock", "package-lock.json", "pnpm-lock.yaml", "*.min.js", "*.map"]
_DOSSIERS_GENERES = {"node_modules", "dist", "target", "graphify-out", ".venv"}


RAISONS_ACCEPTABLES = ("secret", "genere")  # verdicts qu'une décision de quarantaine peut lever
REGLES_ACCEPTABLES = frozenset({"nom_secret", "contenu_secret", "genere"})
CORRESPONDANCES_MAX = 20


@dataclass(frozen=True)
class Verdict:
    raison: str
    detail: dict | None = None  # règle et déclencheur, pour secret et genere ; jamais de contenu


def _masquer(correspondance: bytes) -> str:
    texte = correspondance.decode("utf-8", errors="replace")
    if texte.startswith("-----BEGIN"):  # l'en-tête PEM ne contient pas la clé
        return texte
    return f"{texte[:6]}… ({len(texte)} caractères)"


def _secrets_du_contenu(debut: bytes) -> dict | None:
    trouvees = list(islice(_CONTENU_SECRET.finditer(debut), CORRESPONDANCES_MAX + 1))
    if not trouvees:
        return None
    return {"regle": "contenu_secret", "tronque": len(trouvees) > CORRESPONDANCES_MAX,
            "correspondances": [{"ligne": debut.count(b"\n", 0, m.start()) + 1,
                                 "extrait": _masquer(m.group())}
                                for m in trouvees[:CORRESPONDANCES_MAX]]}


def verdict(chemin: str, debut: bytes, exclure: list[str],
            levees: frozenset[str] = frozenset()) -> Verdict | None:
    """Verdict détaillé ; `levees` : règles de REGLES_ACCEPTABLES ignorées pour ce fichier
    (quarantaine). Les autres règles s'appliquent toujours, dans le même ordre."""
    if any(fnmatch(chemin, g) for g in exclure):
        return Verdict("config")
    parties = chemin.split("/")
    nom = parties[-1]
    if "nom_secret" not in levees and (
            m := next((m for m in _NOMS_SECRETS if fnmatch(nom.lower(), m)), None)):
        return Verdict("secret", {"regle": "nom_secret", "motif": m})
    if "contenu_secret" not in levees and (d := _secrets_du_contenu(debut)):
        return Verdict("secret", d)
    if "genere" not in levees:
        if m := next((m for m in _GENERES if fnmatch(nom, m)), None):
            return Verdict("genere", {"regle": "genere", "motif": m})
        if dossiers := sorted(_DOSSIERS_GENERES & set(parties[:-1])):
            return Verdict("genere", {"regle": "genere", "motif": dossiers[0] + "/"})
    debut = debut[:8192]  # le binaire se juge sur le début ; les secrets sur tout le contenu
    if b"\x00" in debut:
        return Verdict("binaire")
    try:
        debut.decode("utf-8")
    except UnicodeDecodeError as e:
        if e.start < len(debut) - 3:  # un caractère coupé en fin de tranche n'est pas du binaire
            return Verdict("binaire")
    return None


def motif_exclusion(chemin: str, debut: bytes, exclure: list[str]) -> str | None:
    """`debut` : le contenu (complet de préférence, pour les secrets) ou b\"\" (nom seul)."""
    v = verdict(chemin, debut, exclure)
    return v.raison if v else None


def affiner(raison: str | None, chemin: str, debut: bytes, exclure: list[str],
            levees: frozenset[str] = frozenset()) -> tuple[str | None, dict | None]:
    """Détaille un verdict de `motif_exclusion` et lui applique les règles levées.

    Rend `(raison, detail)` ; `(None, None)` si le fichier ne tenait qu'à des règles levées."""
    if raison not in RAISONS_ACCEPTABLES:
        return raison, None
    v = verdict(chemin, debut, exclure, levees)
    if v is None:
        return (None, None) if levees else (raison, None)
    return v.raison, v.detail
