"""Réglages (variables d'environnement RAG_*) et config versionnée (YAML)."""

import hashlib
import json
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Incrémenter à chaque changement de notes.py, decoupage.py ou exclusions.py : l'empreinte change,
# donc tout est réanalysé (une note devenue « secret » est alors retirée de toutes les versions).
VERSION_ANALYSEUR = 2

PUBLIC: frozenset[str] = frozenset({"public-maison"})
TOUT: frozenset[str] = frozenset({"public-maison", "prive", "travail"})


# Bornes des entrées, partagées par l'API HTTP et le serveur MCP.
QUESTION_MAX = 2000
NOM_MAX = 500
LIMITE_MAX = 50


class ErreurConfig(Exception):
    pass


class Reglages(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RAG_", env_file=".env", extra="ignore")

    database_url: str
    litellm_url: str
    litellm_cle: str = ""
    litellm_ca: Path | None = None
    jeton: str = ""
    jeton_prive: str = ""
    jeton_reindexer: str = ""
    github_jeton: str = ""
    cle_secrets: str = ""   # clé Fernet : chiffre les jetons des sources gérées
    sources_amorce: Path | None = None   # YAML d'amorçage des sources git en base
    graphify_commande: str = "graphify"   # en dev : spike/graphify/en-conteneur.sh (sans réseau)
    donnees_dir: Path = Path("donnees")
    config: Path = Path("config/rag.yaml")
    s3_endpoint: str = "http://s3:8333"
    s3_access_key: str = "rag"
    s3_secret_key: str = ""
    coffre_bucket: str = "obsidian"
    coffre_prefixe: str = ""


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SourceConf(_Strict):
    nom: str = Field(pattern=r"^[\w.-]+$", max_length=100)  # sert de nom de dossier de clone

    @field_validator("nom")
    @classmethod
    def _pas_de_point(cls, v: str) -> str:
        if v in (".", ".."):
            raise ValueError("nom de source refusé")
        return v
    type: Literal["obsidian", "code"]
    emplacement: str
    branche: str = "main"
    exclure: list[str] = []
    repos_minutes: int = 0
    visibilite: Literal["public-maison", "prive", "travail"]


class IndexationConf(_Strict):
    strategie: Literal["titres", "recursif", "parent_enfant"]
    cible_tokens: int
    plafond_tokens: int
    fusion_sous_tokens: int
    recursif_tokens: int
    recouvrement: float
    enfant_tokens: int
    modele_embedding: str
    dimensions: int
    taille_max_octets: int


class Poids(_Strict):
    vecteur: float
    texte: float
    flou: float


class RerankConf(_Strict):
    actif: bool
    modele: str
    candidats: int
    garder: int


class GrapheConf(_Strict):
    expansion: bool
    depuis: int
    attenuation: float
    plafond: int


class RechercheConf(_Strict):
    top_vecteur: int
    top_texte: int
    rrf_k: int
    poids: Poids
    flou: bool
    seuil_flou: float
    instruction_requete: str
    rerank: RerankConf
    graphe: GrapheConf
    bonus_source: Annotated[float, Field(ge=0)] = 0.0   # × (1 + bonus) si la question nomme la source
    poids_source: dict[Literal["obsidian", "code"], Annotated[float, Field(ge=0)]] = {}   # par type de source ; 1.0 absent


class ReponseConf(_Strict):
    modele: str
    temperature: float
    passages: int
    seuil_refus_rerank: float | None
    seuil_refus_cosinus: float | None


class BancConf(_Strict):
    modele_juge: str
    modele_synthetique: str


class Config(_Strict):
    sources: list[SourceConf]
    indexation: IndexationConf
    recherche: RechercheConf
    reponse: ReponseConf
    banc: BancConf


def charger_config(chemin: Path) -> Config:
    return Config.model_validate(yaml.safe_load(Path(chemin).read_text(encoding="utf-8")))


def empreinte_indexation(c: IndexationConf) -> str:
    brut = json.dumps(c.model_dump() | {"version_analyseur": VERSION_ANALYSEUR}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(brut.encode()).hexdigest()


def plafond_lecture(c: IndexationConf) -> int:
    """Au-delà, un fichier n'est jamais lu (indexeur et copie graphify)."""
    return 10 * c.taille_max_octets
