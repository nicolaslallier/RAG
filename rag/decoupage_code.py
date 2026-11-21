"""Découpage du code par symbole de premier niveau (tree-sitter), repli récursif sinon."""
# Changer ce fichier change ce qui est indexé : incrémenter VERSION_ANALYSEUR (rag/config.py).

from pathlib import PurePosixPath

from rag.config import IndexationConf
from rag.decoupage import Morceau, compter_tokens, decouper_recursif

_EXT = {".py": "python", ".rs": "rust", ".ts": "typescript", ".tsx": "typescript",
        ".js": "javascript", ".mjs": "javascript", ".jsx": "javascript", ".vue": "vue",
        ".yaml": "yaml", ".yml": "yaml", ".md": "markdown", ".sh": "shell", ".bash": "shell",
        ".json": "json", ".toml": "toml", ".sql": "sql"}
# ponytail: .vue en récursif ; analyser le bloc <script> si le banc le demande
_ANALYSEUR = {"python": "python", "rust": "rust", "typescript": "tsx", "javascript": "javascript"}
_SYMBOLES = {
    "python": {"function_definition", "class_definition"},
    "rust": {"function_item", "struct_item", "enum_item", "trait_item", "impl_item"},
    "typescript": {"function_declaration", "class_declaration"},
    "javascript": {"function_declaration", "class_declaration"},
}
_CONTENEURS = {"class_definition", "impl_item", "class_declaration"}
_METHODES = {"function_definition", "decorated_definition", "function_item", "method_definition"}
_FONCTIONS_JS = {"arrow_function", "function_expression", "function"}


def langage(chemin: str) -> str:
    p = PurePosixPath(chemin)
    if p.name.startswith("Dockerfile"):
        return "dockerfile"
    return _EXT.get(p.suffix.lower(), "autre")


def _txt(n) -> str:
    return n.text.decode("utf-8", errors="replace") if n else ""


def _nom(n) -> str | None:
    if n.type == "impl_item":
        t = n.child_by_field_name("type")
        while t is not None and t.type in ("generic_type", "scoped_type_identifier"):
            t = t.child_by_field_name("type") or t.child_by_field_name("name")
        return _txt(t) or None
    if n.type == "lexical_declaration":
        for d in n.named_children:
            if d.type == "variable_declarator" and (v := d.child_by_field_name("value")) \
                    and v.type in _FONCTIONS_JS:
                return _txt(d.child_by_field_name("name")) or None
        return None
    return _txt(n.child_by_field_name("name")) or None


def _symboles(racine, lang: str):
    """(nœud englobant, nœud de définition, nom) de premier niveau."""
    for n in racine.named_children:
        englobant = n
        if n.type == "decorated_definition":
            n = n.child_by_field_name("definition")
        elif n.type == "export_statement":
            n = n.child_by_field_name("declaration") or n
            if n.type == "lexical_declaration":
                if nom := _nom(n):
                    yield englobant, n, nom
                continue
        if n is not None and n.type in _SYMBOLES[lang] and (nom := _nom(n)):
            yield englobant, n, nom


def _methodes(defn):
    corps = defn.child_by_field_name("body")
    for m in corps.named_children if corps else []:
        d = m.child_by_field_name("definition") if m.type == "decorated_definition" else m
        if m.type in _METHODES and d is not None and (nom := _nom(d)):
            yield m, nom


def decouper_code(depot: str, chemin: str, texte: str, conf: IndexationConf) -> list[Morceau]:
    racine_fil = f"{depot} > {chemin}"
    lang = langage(chemin)
    repli = decouper_recursif(racine_fil, texte, conf.recursif_tokens, conf.recouvrement)
    if lang not in _ANALYSEUR or not texte.strip():
        return repli
    from tree_sitter_language_pack import get_parser

    src = texte.encode("utf-8")
    arbre = get_parser(_ANALYSEUR[lang]).parse(src)
    if arbre.root_node.has_error:
        return repli
    out: list[Morceau] = []

    def ajouter(symbole: str, debut: int, fin: int, ligne: int):
        t = src[debut:fin].decode("utf-8", errors="replace")
        fil = f"{racine_fil} > {symbole}"
        parts = ([t] if compter_tokens(t) <= conf.plafond_tokens else
                 [m.texte for m in decouper_recursif(fil, t, conf.recursif_tokens,
                                                     conf.recouvrement)])
        for p in parts:
            out.append(Morceau(len(out), fil, p, symbole=symbole, ligne=ligne))

    couvert = []
    for englobant, defn, nom in _symboles(arbre.root_node, lang):
        a, b = englobant.start_byte, englobant.end_byte
        couvert.append((a, b))
        ligne = englobant.start_point[0] + 1
        meths = list(_methodes(defn)) if defn.type in _CONTENEURS else []
        if compter_tokens(src[a:b].decode("utf-8", errors="replace")) <= conf.plafond_tokens \
                or not meths:
            ajouter(nom, a, b, ligne)
            continue
        ajouter(nom, a, meths[0][0].start_byte, ligne)
        for m, mnom in meths:
            ajouter(f"{nom}.{mnom}", m.start_byte, m.end_byte, m.start_point[0] + 1)
    if not out:
        return repli
    reste, pos = [], 0
    for a, b in couvert:
        reste.append(src[pos:a])
        pos = b
    reste.append(src[pos:])
    r = b"".join(reste).decode("utf-8", errors="replace").strip()
    if compter_tokens(r) >= 20:
        for m in decouper_recursif(racine_fil, r, conf.recursif_tokens, conf.recouvrement):
            out.append(Morceau(len(out), racine_fil, m.texte, ligne=1))
    return out
