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
    (".npmrc", b"x", "secret"), ("home/.netrc", b"x", "secret"), ("cert.PFX", b"x", "secret"),
    ("c.p12", b"x", "secret"), ("k.jks", b"x", "secret"),
    ("infra/terraform.tfstate", b"x", "secret"), ("terraform.tfstate.backup", b"x", "secret"),
    ("prod.tfvars", b"x", "secret"), ("gcp/credentials.json", b"x", "secret"),
    ("kubeconfig", b"x", "secret"), ("mdp.kdbx", b"x", "secret"),
    # faux jetons construits par concaténation : jamais de littéral réaliste dans le dépôt
    ("ci.md", b"jeton: glpat-" + b"a" * 20, "secret"),
    ("slack.md", b"xoxb-" + b"1" * 12 + b"-" + b"a" * 24, "secret"),
    ("docs/kubeconfig.md", b"x", None), ("slack-doc.md", b"xoxb-your-token-here", None),
    ("gl.md", b"prefixe-glpat-court", None),
    ("xoxe1.md", b"xoxe.xoxp-1-" + b"A1b2" * 10, "secret"),
    ("xoxe2.md", b"t=xoxe.xoxb-1-" + b"Z9" * 15, "secret"),
    ("xoxe3.md", b"xoxe-1-" + b"My0xLTI" * 6, "secret"),
    ("xoxa.md", b"xoxa-2-" + b"1" * 12 + b"-" + b"a" * 24, "secret"),
    ("xapp.md", b"xapp-1-" + b"A0123ABCD" + b"-" + b"1" * 13 + b"-" + b"f0" * 16, "secret"),
    ("xoxp.md", b"xoxp-" + b"1" * 12 + b"-" + b"2" * 12 + b"-" + b"a" * 32, "secret"),
    ("xapp-doc.md", b"xapp-1-your-app-token", None), ("xoxe-doc.md", b"xoxe-1-court", None),
    ("collé.md", b"motxoxb-" + b"1" * 12 + b"-" + b"a" * 24, None),
])
def test_motif_exclusion(chemin, debut, raison):
    assert motif_exclusion(chemin, debut, [".obsidian/**", "_versions/**"]) == raison


def test_motifs_slack_sans_redos():
    import time
    debut = time.monotonic()
    for brut in (b"xoxb-" + b"a" * 50_000, b"xoxb-" + b"a-" * 25_000,
                 b"xapp-1-" + b"a" * 50_000 + b"-", b"xapp-1-a-" + b"1" * 50_000 + b"-x",
                 b"xoxe-1-" + b"-" * 50_000):
        motif_exclusion("x.md", brut, [])
    assert time.monotonic() - debut < 2
