"""Ce qui n'est jamais indexé : config, secrets, fichiers générés, binaires."""
# Changer ce fichier change ce qui est indexé : incrémenter VERSION_ANALYSEUR (rag/config.py).

import re
from fnmatch import fnmatch

_NOMS_SECRETS = [".env", ".env.*", "*.pem", "*.key", "*.p12", "id_rsa*", "id_ed25519*",
                 "seal.key", ".runner.env", ".openbao.env", ".portainer.env",
                 ".npmrc", ".netrc", "*.pfx", "*.jks", "*.tfstate", "*.tfstate.backup",
                 "*.tfvars", "credentials.json", "kubeconfig", "*.kdbx"]  # en minuscules
_CONTENU_SECRET = re.compile(
    rb"-----BEGIN [A-Z ]*PRIVATE KEY( BLOCK)?-----|(?<![A-Za-z0-9_-])sk-(?:proj-|ant-|svcacct-)?[A-Za-z0-9_-]{20,}|ghp_[A-Za-z0-9]{36}"
    rb"|github_pat_[A-Za-z0-9_]{20,}|AKIA[0-9A-Z]{16}"
    rb"|hv[sb]\.[A-Za-z0-9_-]{20,}|(?<![A-Za-z0-9_.-])s\.[A-Za-z0-9]{24,}"
    rb"|(?<![A-Za-z0-9_-])glpat-[A-Za-z0-9_-]{20,}"
    # Slack : deux segments d'au moins 8 caractères, pas « xoxb-your-token-here »
    rb"|(?<![A-Za-z0-9_-])xox[abposr]-[A-Za-z0-9]{8,}-[A-Za-z0-9-]{8,}"
)
_GENERES = ["uv.lock", "Cargo.lock", "package-lock.json", "pnpm-lock.yaml", "*.min.js", "*.map"]
_DOSSIERS_GENERES = {"node_modules", "dist", "target", "graphify-out", ".venv"}


def motif_exclusion(chemin: str, debut: bytes, exclure: list[str]) -> str | None:
    """`debut` : le contenu (complet de préférence, pour les secrets) ou b\"\" (nom seul)."""
    if any(fnmatch(chemin, g) for g in exclure):
        return "config"
    parties = chemin.split("/")
    nom = parties[-1]
    if any(fnmatch(nom.lower(), m) for m in _NOMS_SECRETS) or _CONTENU_SECRET.search(debut):
        return "secret"
    if any(fnmatch(nom, m) for m in _GENERES) or _DOSSIERS_GENERES & set(parties[:-1]):
        return "genere"
    debut = debut[:8192]  # le binaire se juge sur le début ; les secrets sur tout le contenu
    if b"\x00" in debut:
        return "binaire"
    try:
        debut.decode("utf-8")
    except UnicodeDecodeError as e:
        if e.start < len(debut) - 3:  # un caractère coupé en fin de tranche n'est pas du binaire
            return "binaire"
    return None
