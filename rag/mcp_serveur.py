"""Serveur MCP (HTTP « streamable », sans état) : sept outils de lecture, mêmes jetons que l'API."""

import asyncio
import json
import logging
from typing import Annotated, Literal

from mcp.server.fastmcp import Context, FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import Field

from rag.config import LIMITE_MAX, NOM_MAX, QUESTION_MAX
from rag.gestion import PAGE_MAX, SourceInconnue
from rag.modeles import ErreurModele
from rag.recherche import Filtres

log = logging.getLogger("rag.mcp")

Question = Annotated[str, Field(max_length=QUESTION_MAX)]
Nom = Annotated[str, Field(max_length=NOM_MAX)]
Tags = Annotated[list[Annotated[str, Field(max_length=NOM_MAX)]] | None, Field(max_length=20)]


def _niveaux(moteur, ctx: Context) -> frozenset[str]:
    requete = ctx.request_context.request
    return moteur.niveaux(requete.headers.get("authorization", "") if requete else "")


def creer_mcp(moteur) -> FastMCP:
    """`moteur` fournit niveaux(authorization) et les fonctions bloquantes chercher, lister,
    repondre, chemin, expliquer, sources, fichiers (déjà liées à la base, à la config et aux modèles)."""
    # Hôte appelé « rag:8000 » par LiteLLM sur infra-net : la protection DNS-rebinding (liste
    # d'hôtes) est inutile, chaque requête exige déjà le jeton Bearer.
    mcp = FastMCP("rag", stateless_http=True, json_response=True, streamable_http_path="/mcp",
                  transport_security=TransportSecuritySettings(
                      enable_dns_rebinding_protection=False))

    async def appeler(outil: str, f, *a, **k):
        """Appel bloquant dans un fil. Seules les erreurs métier repartent telles quelles ;
        le reste est journalisé ici et le client ne reçoit qu'un message générique.
        Ni la question ni les arguments ne sont journalisés."""
        try:
            return await asyncio.to_thread(f, *a, **k)
        except SourceInconnue:
            raise ValueError("source inconnue") from None
        except ErreurModele:
            log.warning("%s : modèle indisponible", outil)
            raise ValueError("modèle indisponible") from None
        except Exception:
            log.exception("%s : erreur interne", outil)
            raise ValueError("erreur interne") from None

    @mcp.tool(description="Cherche les passages des notes et du code les plus proches d'une "
                          "question, avec lien et origine.")
    async def rag_chercher(question: Question, ctx: Context, type: Nom | None = None,
                           tags: Tags = None, source: Nom | None = None,
                           limite: Annotated[int, Field(ge=1, le=LIMITE_MAX)] = 8) -> str:
        f = Filtres(type=type, tags=tags, sources=[source] if source else None)
        r = await appeler("rag_chercher", moteur.chercher, question, f, limite,
                          _niveaux(moteur, ctx))
        return json.dumps([{"cle": x.cle, "fil": x.fil, "texte": x.texte, "lien": x.lien,
                            "origine": x.origine, "score": x.score} for x in r],
                          ensure_ascii=False)

    @mcp.tool(description="Liste exhaustivement les notes ou fichiers qui répondent à une "
                          "demande de liste (filtres extraits de la question s'ils manquent).")
    async def rag_lister(question: Question, ctx: Context, type: Nom | None = None,
                         tags: Tags = None) -> str:
        f = Filtres(type=type, tags=tags) if type or tags else None
        r = await appeler("rag_lister", moteur.lister, question, f, _niveaux(moteur, ctx))
        return json.dumps(r, ensure_ascii=False, default=str)

    @mcp.tool(description="Répond à une question d'après les notes, avec citations [n] "
                          "vérifiées, ou refuse si les notes ne le disent pas.")
    async def rag_repondre(question: Question, ctx: Context) -> str:
        r = await appeler("rag_repondre", moteur.repondre, question, _niveaux(moteur, ctx))
        return json.dumps({"refus": r.refus, "texte": r.texte, "sources": r.sources},
                          ensure_ascii=False)

    @mcp.tool(description="Trouve le chemin le plus court entre deux éléments du graphe "
                          "(notes, fichiers, symboles).")
    async def rag_chemin(de: Nom, vers: Nom, ctx: Context) -> str:
        return json.dumps(await appeler("rag_chemin", moteur.chemin, de, vers, _niveaux(moteur, ctx)),
                          ensure_ascii=False, default=str)

    @mcp.tool(description="Explique un élément du graphe : sa nature, sa communauté et ses "
                          "voisins.")
    async def rag_expliquer(nom: Nom, ctx: Context) -> str:
        return json.dumps(await appeler("rag_expliquer", moteur.expliquer, nom, _niveaux(moteur, ctx)),
                          ensure_ascii=False, default=str)

    @mcp.tool(description="Liste les sources indexées (coffre, dépôts) avec leurs nombres de "
                          "fichiers indexés et exclus, et la passe d'indexation en cours.")
    async def rag_sources(ctx: Context) -> str:
        return json.dumps(await appeler("rag_sources", moteur.sources, _niveaux(moteur, ctx)),
                          ensure_ascii=False, default=str)

    @mcp.tool(description="Liste les fichiers d'une source, indexés ou exclus (avec la "
                          "raison), 50 par page.")
    async def rag_fichiers(source: Nom, ctx: Context,
                           etat: Literal["indexe", "exclu_auto", "exclu_manuel"] | None = None,
                           contient: Nom | None = None,
                           page: Annotated[int, Field(ge=1, le=PAGE_MAX)] = 1) -> str:
        r = await appeler("rag_fichiers", moteur.fichiers, source, etat, contient, page,
                          _niveaux(moteur, ctx))
        return json.dumps(r, ensure_ascii=False, default=str)

    return mcp


