"""Client LiteLLM (seul fournisseur de modèles) : embeddings et génération."""

import json
import ssl
from collections.abc import Iterator
from pathlib import Path

import httpx

LOT = 32
DIMENSIONS = 1024


class ErreurModele(Exception):
    pass


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
        self.client = client or httpx.Client(timeout=300, verify=contexte_tls(ca))
        self.entetes = {"Authorization": f"Bearer {cle}"}

    def _post(self, route: str, corps: dict) -> dict:
        try:
            r = self.client.post(f"{self.url}{route}", json=corps, headers=self.entetes)
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError as e:  # message sans la clé ni les en-têtes
            raise ErreurModele(f"LiteLLM {route} : HTTP {e.response.status_code}") from None
        except (httpx.HTTPError, ValueError) as e:
            raise ErreurModele(f"LiteLLM {route} : {type(e).__name__}") from None

    def embed_documents(self, textes: list[str], modele: str) -> list[list[float]]:
        out: list[list[float]] = []
        for i in range(0, len(textes), LOT):
            lot = textes[i:i + LOT]
            data = self._post("/v1/embeddings", {"model": modele, "input": lot}).get("data", [])
            vecs = [d["embedding"] for d in sorted(data, key=lambda d: d["index"])]
            if len(vecs) != len(lot) or any(len(v) != DIMENSIONS for v in vecs):
                raise ErreurModele(f"embeddings inattendus ({len(vecs)} pour {len(lot)})")
            out += vecs
        return out

    def embed_requete(self, question: str, modele: str, instruction: str) -> list[float]:
        return self.embed_documents([f"Instruct: {instruction}\nQuery: {question}"], modele)[0]

    def generer(self, modele: str, messages: list[dict], temperature: float,
                json_mode: bool = False) -> str:
        corps = {"model": modele, "messages": messages, "temperature": temperature,
                 "stream": False}
        if json_mode:
            corps["response_format"] = {"type": "json_object"}
        try:
            return self._post("/v1/chat/completions", corps)["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError):
            raise ErreurModele("réponse de génération inattendue") from None

    def generer_flux(self, modele: str, messages: list[dict], temperature: float) -> Iterator[str]:
        corps = {"model": modele, "messages": messages, "temperature": temperature,
                 "stream": True}
        try:
            with self.client.stream("POST", f"{self.url}/v1/chat/completions", json=corps,
                                    headers=self.entetes) as r:
                r.raise_for_status()
                for ligne in r.iter_lines():
                    if not ligne.startswith("data:"):
                        continue
                    data = ligne[5:].strip()
                    if data == "[DONE]":
                        return
                    choix = json.loads(data).get("choices") or [{}]
                    if fragment := (choix[0].get("delta") or {}).get("content"):
                        yield fragment
        except httpx.HTTPStatusError as e:
            raise ErreurModele(f"LiteLLM flux : HTTP {e.response.status_code}") from None
        except (httpx.HTTPError, ValueError) as e:
            raise ErreurModele(f"LiteLLM flux : {type(e).__name__}") from None


def modeles_depuis(reglages) -> Modeles:
    return Modeles(reglages.litellm_url, reglages.litellm_cle, reglages.litellm_ca)
