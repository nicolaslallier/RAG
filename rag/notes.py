"""Analyse d'une note Obsidian : frontmatter, commentaires %%…%%, tags, [[liens]]."""

import datetime as dt
import json
import re
from dataclasses import dataclass
from pathlib import PurePosixPath

import yaml

_FM = re.compile(r"\A---[ \t]*\n(.*?)\n---[ \t]*(?:\n|\Z)", re.DOTALL)
_COMMENTAIRE = re.compile(r"%%.*?(?:%%|\Z)", re.DOTALL)  # non fermé : masque jusqu'à la fin
_CODE = re.compile(r"```.*?(?:```|\Z)|`[^`\n]*`", re.DOTALL)
_LIEN = re.compile(r"\[\[([^\]|#]*)(?:[#|][^\]]*)?\]\]")
_TAG = re.compile(r"(?<![\w&/#])#([\w/-]*[^\W\d][\w/-]*)")  # au moins un non-chiffre


_FM_MAX = 64 * 1024


class _ChargeurSansAlias(yaml.SafeLoader):
    """Refuse ancres et alias YAML (amplification « billion laughs »)."""

    def compose_node(self, parent, index):
        if self.check_event(yaml.AliasEvent) or getattr(self.peek_event(), "anchor", None):
            raise yaml.YAMLError("alias ou ancre YAML refusés")
        return super().compose_node(parent, index)


def _frontmatter(brut: str) -> dict | None:
    if len(brut) > _FM_MAX:
        return None
    try:
        d = yaml.load(brut, Loader=_ChargeurSansAlias)  # SafeLoader dérivé
        if not isinstance(d, dict):
            return None
        d = {str(k): v for k, v in d.items()}
        return json.loads(json.dumps(d, default=str, allow_nan=False))
    except Exception:  # noqa: BLE001 - un frontmatter n'arrête jamais l'indexation du coffre
        return None


@dataclass
class Note:
    titre: str
    alias: list[str]
    tags: list[str]
    type: str | None
    origine: str | None
    date: dt.date | None
    frontmatter: dict
    corps: str
    liens: list[str]


def _liste(v) -> list[str]:
    if v is None:
        return []
    if isinstance(v, str):
        return [x.strip() for x in v.split(",") if x.strip()] if "," in v else [v.strip()]
    if isinstance(v, list):
        return [str(x).strip() for x in v if x is not None and str(x).strip()]
    return [str(v)]


def _uniques(xs):
    return list(dict.fromkeys(xs))


def analyser_note(chemin: str, texte: str) -> Note:
    texte = texte.replace("\x00", "")
    fm, corps = {}, texte
    m = _FM.match(texte)
    if m:
        brut = _frontmatter(m.group(1))
        if brut is not None:
            fm, corps = brut, texte[m.end():]
    corps = _COMMENTAIRE.sub("", corps)
    sans_code = _CODE.sub("", corps)
    tags = [t.lstrip("#") for t in _liste(fm.get("tags"))] + _TAG.findall(_LIEN.sub("", sans_code))
    alias = _liste(fm.get("aliases", fm.get("alias")))
    date = None
    try:
        date = dt.date.fromisoformat(str(fm.get("date"))[:10]) if fm.get("date") else None
    except ValueError:
        pass
    return Note(
        titre=PurePosixPath(chemin).stem,
        alias=alias,
        tags=_uniques(t for t in tags if t),
        type=str(fm["type"]) if fm.get("type") else None,
        origine=str(fm["source"]) if fm.get("source") else None,
        date=date,
        frontmatter=fm,
        corps=corps,
        liens=_uniques(c.strip() for c in _LIEN.findall(sans_code) if c.strip()),
    )
