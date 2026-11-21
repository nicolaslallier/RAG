from pathlib import Path

import pytest

from rag.config import charger_config
from rag.decoupage_code import decouper_code, langage

CONF = charger_config(Path("config/rag.yaml")).indexation

PY = '''import os

def aide():
    return 1

class Indexeur:
    """Doc."""
    def lancer(self):
        return aide()
'''


def test_python_par_symbole():
    m = decouper_code("RAG", "rag/indexeur.py", PY, CONF)
    assert [(x.symbole, x.ligne) for x in m if x.symbole] == [("aide", 3), ("Indexeur", 6)]
    assert m[0].fil == "RAG > rag/indexeur.py > aide" and "def aide" in m[0].texte


def test_decorateur_garde_avec_la_classe():
    m = decouper_code("R", "a.py", "import x\n\n@dataclass\nclass A:\n    b: int = 1\n", CONF)
    a = next(x for x in m if x.symbole == "A")
    assert a.texte.startswith("@dataclass") and a.ligne == 3


def test_grosse_classe_eclatee_en_methodes():
    src = "class C:\n    x = 1\n" + "".join(
        f"    def m{i}(self):\n" + "        a = 1\n" * 150 for i in range(4))
    s = [x.symbole for x in decouper_code("R", "c.py", src, CONF)]
    assert s[0] == "C" and {"C.m0", "C.m3"} <= set(s)


def test_symbole_trop_long_decoupe_en_recursif():
    src = "def f():\n" + "    a = 1\n" * 2000
    m = decouper_code("R", "f.py", src, CONF)
    assert len(m) > 1 and all(x.symbole == "f" and x.ligne == 1 for x in m)
    assert [x.ordre for x in m] == list(range(len(m)))


def test_rust_et_typescript():
    rs = decouper_code("Moria", "src/lib.rs",
                       "pub struct Nain { pv: u32 }\nimpl Nain { pub fn creuser(&self) {} }\n"
                       "pub fn main() {}", CONF)
    assert {"Nain", "main"} <= {x.symbole for x in rs}
    ts = decouper_code("AI", "a.ts", "export const charger = async () => 1;\nexport function f() {}"
                       "\nclass K {}", CONF)
    assert {"charger", "f", "K"} <= {x.symbole for x in ts}


def test_impl_trait_porte_le_nom_du_type():
    rs = decouper_code("M", "a.rs", "struct Bar;\nimpl Foo for Bar { fn x(&self) {} }\n", CONF)
    assert [x.symbole for x in rs if x.symbole] == ["Bar", "Bar"]


@pytest.mark.parametrize("chemin,texte", [
    ("a.py", "def cassé(:\n  ???"), ("App.vue", "<template><p/></template>\n" * 30),
    ("x.yaml", "a: 1\n" * 50), ("Dockerfile", "FROM x\nRUN y\n"), ("vide.py", ""),
    ("LISEZ.md", "# T\n\ntexte " * 40)])
def test_repli_recursif_sans_exception(chemin, texte):
    m = decouper_code("R", chemin, texte, CONF)
    assert all(x.symbole is None and x.fil == f"R > {chemin}" for x in m)
    assert (m == []) == (texte.strip() == "")


def test_langage():
    assert [langage(c) for c in ("a.py", "b.rs", "c.tsx", "Dockerfile.dev", "d.vue", "e.bin")] == [
        "python", "rust", "typescript", "dockerfile", "vue", "autre"]
    assert [langage(c) for c in ("x/y.mjs", "k.yml", "s.sh", "t.toml", "q.sql", "j.json")] == [
        "javascript", "yaml", "shell", "toml", "sql", "json"]
