"""Client LiteLLM (seul fournisseur de modèles) : embeddings et génération."""

import json
import ssl
import time
from collections.abc import Iterator
from pathlib import Path

import httpx

LOT = 32
DIMENSIONS = 1024
DELAI_COURT = 15.0    # embed_requete, aiguillage : une question n'attend pas
DELAI_LONG = 300.0    # génération, embeddings de documents par lots
REPRISES = 2          # embeddings seulement
ATTENTES = (0.5, 1.5)
_TRANSITOIRES = {502, 503, 504}
_CONNEXION = (httpx.NetworkError, httpx.ConnectTimeout, httpx.RemoteProtocolError)


def _delai(lecture: float) -> httpx.Timeout:
    return httpx.Timeout(lecture, connect=5.0, pool=5.0)


class ErreurModele(Exception):
    pass


class _Tronque:
    """Dernier élément de `generer_flux` quand le modèle s'arrête sur sa limite de longueur."""

    def __repr__(self) -> str:
        return "TRONQUE"


TRONQUE = _Tronque()


def _fragment(data: str) -> tuple[str | None, str | None]:
    """(texte, finish_reason) d'un événement du flux ; ErreurModele si erreur ou forme imprévue."""
    try:
        ev = json.loads(data)
        if ev.get("error"):   # sans le message du fournisseur : il peut contenir des détails
            raise ErreurModele("LiteLLM flux : erreur du fournisseur")
        choix = (ev.get("choices") or [{}])[0]
        contenu = (choix.get("delta") or {}).get("content")
        fin = choix.get("finish_reason")
    except (ValueError, AttributeError, TypeError, IndexError, KeyError):
        raise ErreurModele("LiteLLM flux : événement inattendu") from None
    if not isinstance(contenu, str | None) or not isinstance(fin, str | None):
        raise ErreurModele("LiteLLM flux : événement inattendu")
    return contenu, fin


def contexte_tls(ca: Path | None) -> ssl.SSLContext:
    """CA maison en plus des CA système. Python 3.13 exige par défaut (VERIFY_X509_STRICT) une
    extension keyUsage que le certificat de la CA maison n'a pas : on garde la vérification de la
    chaîne et du nom, sans ce contrôle strict."""
    ctx = ssl.create_default_context()
    if ca:
        ctx.load_verify_locations(cafile=str(Path(ca).expanduser()))
        ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
    return ctx


class Modeles:
    def __init__(self, url: str, cle: str, ca: Path | None = None,
                 client: httpx.Client | None = None):
        self.url = url.rstrip("/")
        self.client = client or httpx.Client(timeout=_delai(DELAI_LONG),
                                             verify=contexte_tls(ca))
        self.entetes = {"Authorization": f"Bearer {cle}"}

    def _post(self, route: str, corps: dict, *, delai: float = DELAI_LONG,
              reprises: int = 0) -> dict:
        for essai in range(reprises + 1):
            dernier = essai == reprises
            attente = ATTENTES[min(essai, len(ATTENTES) - 1)]
            try:
                r = self.client.post(f"{self.url}{route}", json=corps, headers=self.entetes,
                                     timeout=_delai(delai))
                if r.status_code in _TRANSITOIRES and not dernier:
                    time.sleep(attente)
                    continue
                r.raise_for_status()
                rep = r.json()
            except httpx.HTTPStatusError as e:  # message sans la clé ni les en-têtes
                raise ErreurModele(f"LiteLLM {route} : HTTP {e.response.status_code}") from None
            except _CONNEXION as e:
                if dernier:
                    raise ErreurModele(f"LiteLLM {route} : {type(e).__name__}") from None
                time.sleep(attente)
                continue
            except (httpx.HTTPError, ValueError) as e:
                raise ErreurModele(f"LiteLLM {route} : {type(e).__name__}") from None
            if not isinstance(rep, dict):
                raise ErreurModele(f"LiteLLM {route} : réponse inattendue")
            return rep
        raise ErreurModele(f"LiteLLM {route} : reprises épuisées")  # inatteignable

    def _embed(self, textes: list[str], modele: str, delai: float) -> list[list[float]]:
        out: list[list[float]] = []
        for i in range(0, len(textes), LOT):
            lot = textes[i:i + LOT]
            rep = self._post("/v1/embeddings", {"model": modele, "input": lot}, delai=delai,
                             reprises=REPRISES)
            try:
                vecs = [d["embedding"] for d in sorted(rep["data"], key=lambda d: d["index"])]
                ok = len(vecs) == len(lot) and all(
                    isinstance(v, list) and len(v) == DIMENSIONS for v in vecs)
            except (KeyError, TypeError):
                ok = False
            if not ok:
                raise ErreurModele(f"embeddings inattendus (lot de {len(lot)})")
            out += vecs
        return out

    def embed_documents(self, textes: list[str], modele: str) -> list[list[float]]:
        return self._embed(textes, modele, DELAI_LONG)

    def embed_requete(self, question: str, modele: str, instruction: str) -> list[float]:
        return self._embed([f"Instruct: {instruction}\nQuery: {question}"], modele,
                           DELAI_COURT)[0]

    def generer(self, modele: str, messages: list[dict], temperature: float,
                json_mode: bool = False, delai: float | None = None) -> str:
        corps = {"model": modele, "messages": messages, "temperature": temperature,
                 "stream": False}
        if json_mode:
            corps["response_format"] = {"type": "json_object"}
        rep = self._post("/v1/chat/completions", corps, delai=delai or DELAI_LONG)
        try:
            contenu = rep["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError):
            contenu = None
        if not isinstance(contenu, str):
            raise ErreurModele("réponse de génération inattendue")
        return contenu

    def generer_flux(self, modele: str, messages: list[dict],
                     temperature: float) -> Iterator[str | _Tronque]:
        corps = {"model": modele, "messages": messages, "temperature": temperature,
                 "stream": True}
        fin = None
        try:
            with self.client.stream("POST", f"{self.url}/v1/chat/completions", json=corps,
                                    headers=self.entetes, timeout=_delai(DELAI_LONG)) as r:
                r.raise_for_status()
                for ligne in r.iter_lines():
                    if not ligne.startswith("data:"):
                        continue
                    data = ligne[5:].strip()
                    if data == "[DONE]":
                        fin = fin or "stop"
                        break
                    contenu, raison = _fragment(data)
                    fin = raison or fin
                    if contenu:
                        yield contenu
        except httpx.HTTPStatusError as e:
            raise ErreurModele(f"LiteLLM flux : HTTP {e.response.status_code}") from None
        except httpx.HTTPError as e:
            raise ErreurModele(f"LiteLLM flux : {type(e).__name__}") from None
        if fin is None:   # connexion coupée proprement, sans [DONE] ni finish_reason
            raise ErreurModele("LiteLLM flux : interrompu avant la fin")
        if fin == "length":
            yield TRONQUE


def modeles_depuis(reglages) -> Modeles:
    return Modeles(reglages.litellm_url, reglages.litellm_cle, reglages.litellm_ca)
