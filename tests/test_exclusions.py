import pytest

from rag.exclusions import motif_exclusion


@pytest.mark.parametrize("chemin,debut,raison", [
    (".env", b"A=1", "secret"), ("deploy/.env.example", b"A=change-me", "secret"),
    ("cle.pem", b"x", "secret"), ("notes/api.md", b"cle: sk-abcdefghijklmnopqrstuvwx", "secret"),
    ("uv.lock", b"x", "genere"), ("front/node_modules/a/b.js", b"x", "genere"),
    ("img.png", b"\x89PNG\x00", "binaire"), (".obsidian/app.json", b"{}", "config"),
    ("_versions/2026-11-20/Poulet.md", b"# Poulet", "config"),
    ("02 - Domaines/Cuisine/Poulet.md", b"# Poulet", None), ("rag/api.py", b"import os", None),
    ("veille.md", b"https://x.com/2026/elon-musk-other-companies-and-more-stuff", None),
    ("k.md", b"OPENAI=sk-proj-abcdefghijklmnopqrstuv", "secret"),
    ("l.md", b"RAG_LITELLM_CLE=sk-AbCdEfGhIjKlMnOpQrStUv12", "secret"),
    ("u.md", b"https://host/v1/sk-ant-abcdefghijklmnopqrstuvwxyz", "secret"),
    ("v.md", b"cle=/sk-abcdefghijklmnopqrstuvwxyz", "secret"),
    ("bao.md", b"VAULT_TOKEN=hvs.CAESIabcdefghijklmnopqrstuv", "secret"),
    ("bao2.md", b"jeton: s.abcdefghijklmnopqrstuvwx", "secret"),
    (".ENV", b"A=1", "secret"), ("ID_RSA", b"x", "secret"),
    ("k.asc", b"-----BEGIN PGP PRIVATE KEY BLOCK-----", "secret"),
    ("x.md", "é".encode()[:1], None),
])
def test_motif_exclusion(chemin, debut, raison):
    assert motif_exclusion(chemin, debut, [".obsidian/**", "_versions/**"]) == raison
