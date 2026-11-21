"""Découpage des notes en morceaux : par titres, récursif, ou parent-enfant."""

import math
import re
from dataclasses import dataclass, field

from rag.config import IndexationConf

_TITRE = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")
_CLOTURE = re.compile(r"^\s*(```|~~~)")
_CODE = re.compile(r"(```.*?(?:```|\Z))", re.DOTALL)
_SEPS = ["\n\n", "\n", ". ", " "]


@dataclass
class Morceau:
    ordre: int
    fil: str
    texte: str
    parent: int | None = None
    cherchable: bool = True
    symbole: str | None = None
    ligne: int | None = None


def compter_tokens(texte: str) -> int:
    # ponytail: approximation 4 caractères par token ; brancher le tokenizer de Qwen3 si le banc
    # montre des morceaux tronqués
    return math.ceil(len(texte) / 4)


def _couper(t: str, cible: int, seps: list[str]) -> list[str]:
    """Unités ≤ cible (sauf mot géant), séparateur gardé en fin d'unité."""
    if compter_tokens(t) <= cible or not seps:
        return [t] if t else []
    sep, morceaux = seps[0], t.split(seps[0])
    out = []
    for i, p in enumerate(morceaux):
        out += _couper(p + (sep if i < len(morceaux) - 1 else ""), cible, seps[1:])
    return out


def _unites(texte: str, cible: int) -> list[str]:
    out = []
    for i, part in enumerate(_CODE.split(texte)):
        out += [part] if i % 2 else _couper(part, cible, _SEPS)  # un bloc de code reste entier
    return out


def _textes_recursifs(texte: str, cible: int, recouvrement: float) -> list[str]:
    chunks, courant, neuf = [], "", False
    for u in _unites(texte, cible):
        if neuf and compter_tokens(courant + u) > cible:
            chunks.append(courant.strip())
            mots, queue = courant.split(), []
            while mots and len(" ".join([mots[-1], *queue])) <= recouvrement * cible * 4:
                queue.insert(0, mots.pop())
            courant, neuf = (" ".join(queue) + " ") if queue else "", False
        courant += u
        neuf = neuf or bool(u.strip())
    if neuf:
        chunks.append(courant.strip())
    return [c for c in chunks if c]


def decouper_recursif(fil: str, texte: str, cible: int, recouvrement: float) -> list[Morceau]:
    return [Morceau(i, fil, t) for i, t in enumerate(_textes_recursifs(texte, cible, recouvrement))]


@dataclass
class _Section:
    chemin: list[str]
    niveau: int
    lignes: list[str] = field(default_factory=list)

    @property
    def texte(self) -> str:
        return "\n".join(self.lignes).strip()


def _sections(titre: str, corps: str, fusion: int) -> list[tuple[str, str]]:
    secs, pile, code = [_Section([], 0)], [], False
    for ligne in corps.split("\n"):
        if _CLOTURE.match(ligne):
            code = not code
        m = None if code else _TITRE.match(ligne)
        if m:
            niveau = len(m.group(1))
            pile = [(n, t) for n, t in pile if n < niveau] + [(niveau, m.group(2))]
            secs.append(_Section([t for _, t in pile], niveau))
        else:
            secs[-1].lignes.append(ligne)
    secs = [s for s in secs if s.texte]
    out, i = [], 0
    while i < len(secs):
        s = secs[i]
        while (compter_tokens(s.texte) < fusion and i + 1 < len(secs)
               and secs[i + 1].chemin[:-1] == s.chemin[:-1]):
            n = secs[i + 1]
            s.lignes += ["", "#" * n.niveau + " " + n.chemin[-1], "", *n.lignes]
            i += 1
        out.append((" > ".join([titre, *s.chemin]), s.texte))
        i += 1
    return out


def decouper_note(titre: str, corps: str, conf: IndexationConf) -> list[Morceau]:
    if conf.strategie == "recursif":
        return decouper_recursif(titre, corps, conf.recursif_tokens, conf.recouvrement)
    out: list[Morceau] = []
    for fil, texte in _sections(titre, corps, conf.fusion_sous_tokens):
        if conf.strategie == "parent_enfant":
            p = len(out)
            out.append(Morceau(p, fil, texte, cherchable=False))
            for t in _textes_recursifs(texte, conf.enfant_tokens, conf.recouvrement):
                out.append(Morceau(len(out), fil, t, parent=p))
        elif compter_tokens(texte) > conf.plafond_tokens:
            for t in _textes_recursifs(texte, conf.cible_tokens, 0):
                out.append(Morceau(len(out), fil, t))
        else:
            out.append(Morceau(len(out), fil, texte))
    return out
