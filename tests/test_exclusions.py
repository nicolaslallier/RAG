import pytest

from rag.exclusions import motif_exclusion


@pytest.mark.parametrize("chemin,debut,raison", [
    (".env", b"A=1", "secret"), ("deploy/.env.example", b"A=change-me", "secret"),
    ("cle.pem", b"x", "secret"), ("notes/api.md", b"cle: sk-abcdefghijklmnopqrstuvwx", "secret"),
    ("uv.lock", b"x", "genere"), ("front/node_modules/a/b.js", b"x", "genere"),
    ("img.png", b"\x89PNG\x00", "binaire"), (".obsidian/app.json", b"{}", "config"),
    ("_versions/2026-11-20/Poulet.md", b"# Poulet", "config"),
    ("02 - Domaines/Cuisine/Poulet.md", b"# Poulet", None), ("rag/api.py", b"import os", None),
    (".ENV", b"A=1", "secret"), ("ID_RSA", b"x", "secret"),
    ("k.asc", b"-----BEGIN PGP PRIVATE KEY BLOCK-----", "secret"),
    ("x.md", "é".encode()[:1], None),
])
def test_motif_exclusion(chemin, debut, raison):
    assert motif_exclusion(chemin, debut, [".obsidian/**", "_versions/**"]) == raison
