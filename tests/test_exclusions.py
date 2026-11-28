import pytest

from rag.exclusions import Verdict, affiner, motif_exclusion, verdict


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


JETON = "sk-" + "a" * 24  # faux jeton construit : jamais de littéral réaliste dans le dépôt
EX = [".obsidian/**"]


@pytest.mark.parametrize("chemin,debut,raison,detail", [
    ("deploy/.env.example", b"A=1", "secret", {"regle": "nom_secret", "motif": ".env.*"}),
    ("uv.lock", b"x", "genere", {"regle": "genere", "motif": "uv.lock"}),
    ("front/dist/a.js", b"x", "genere", {"regle": "genere", "motif": "dist/"}),
    ("img.png", b"\x89PNG\x00", "binaire", None),
    (".obsidian/app.json", b"{}", "config", None),
])
def test_verdict_donne_la_regle(chemin, debut, raison, detail):
    assert verdict(chemin, debut, EX) == Verdict(raison, detail)


def test_verdict_rien_a_redire():
    assert verdict("rag/api.py", b"import os", EX) is None


def test_verdict_liste_toutes_les_correspondances_masquees():
    contenu = f"# Doc\nA={JETON}\n\n\nB={JETON}x\n".encode()
    d = verdict("docs/plan.md", contenu, EX).detail
    assert d == {"regle": "contenu_secret", "tronque": False, "correspondances": [
        {"ligne": 2, "extrait": "sk-aaa… (27 caractères)"},
        {"ligne": 5, "extrait": "sk-aaa… (28 caractères)"}]}
    assert JETON not in str(d)


def test_verdict_plafonne_a_vingt_correspondances():
    d = verdict("a.md", "\n".join([JETON] * 25).encode(), EX).detail
    assert len(d["correspondances"]) == 20 and d["tronque"] is True
    assert d["correspondances"][-1]["ligne"] == 20


def test_verdict_montre_l_entete_pem_entier():
    entete = "-----BEGIN RSA " + "PRIVATE KEY-----"  # construit, comme les faux jetons
    d = verdict("k.asc", f"x\n{entete}\nabc".encode(), EX).detail
    assert d["correspondances"] == [{"ligne": 2, "extrait": entete}]


def test_regle_levee_ne_leve_qu_elle():
    secret = f"K={JETON}".encode()
    assert verdict(".env.local", b"A=1", EX, frozenset({"nom_secret"})) is None
    v = verdict(".env.local", secret, EX, frozenset({"nom_secret"}))
    assert v.raison == "secret" and v.detail["regle"] == "contenu_secret"
    assert verdict(".env.local", secret, EX, frozenset({"nom_secret", "contenu_secret"})) is None
    assert verdict("dist/app.bin", b"\x00\x01", EX, frozenset({"genere"})) == Verdict("binaire")
    assert verdict("dist/cle.js", secret, EX, frozenset({"genere"})).raison == "secret"
    assert verdict(".obsidian/x.md", b"x", EX, frozenset({"genere"})) == Verdict("config")


def test_affiner():
    secret = f"K={JETON}".encode()
    assert affiner(None, "a.md", b"x", EX) == (None, None)
    assert affiner("binaire", "a.png", b"\x00", EX) == ("binaire", None)
    raison, detail = affiner("secret", "a.md", secret, EX)
    assert raison == "secret" and detail["regle"] == "contenu_secret"
    assert affiner("secret", "a.md", secret, EX, frozenset({"contenu_secret"})) == (None, None)
    # un verdict que `verdict` ne retrouve pas (règle remplacée dans un test) est gardé tel quel
    assert affiner("secret", "a.md", b"rien", EX) == ("secret", None)
