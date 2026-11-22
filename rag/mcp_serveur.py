"""Serveur MCP (HTTP « streamable », sans état) : cinq outils de lecture, mêmes jetons que l'API."""

import asyncio
import json
from typing import Annotated

from mcp.server.fastmcp import Context, FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import Field

from rag.config import LIMITE_MAX, NOM_MAX, QUESTION_MAX
from rag.recherche import Filtres

Question = Annotated[str, Field(max_length=QUESTION_MAX)]
Nom = Annotated[str, Field(max_length=NOM_MAX)]
Tags = Annotated[list[Annotated[str, Field(max_length=NOM_MAX)]] | None, Field(max_length=20)]


def _niveaux(etat, ctx: Context) -> frozenset[str]:
    requete = ctx.request_context.request
    return etat.niveaux(requete.headers.get("authorization", "") if requete else "")


def creer_mcp(etat) -> FastMCP:
    """`etat` fournit niveaux(authorization) et les fonctions bloquantes chercher, lister,
    repondre, chemin, expliquer (déjà liées à la base, à la config et aux modèles)."""
    # Hôte appelé « rag:8000 » par LiteLLM sur infra-net : la protection DNS-rebinding (liste
    # d'hôtes) est inutile, chaque requête exige déjà le jeton Bearer.
    mcp = FastMCP("rag", stateless_http=True, json_response=True, streamable_http_path="/mcp",
                  transport_security=TransportSecuritySettings(
                      enable_dns_rebinding_protection=False))

    async def appeler(f, *a, **k):
        return await asyncio.to_thread(f, *a, **k)

    @mcp.tool(description="Cherche les passages des notes et du code les plus proches d'une "
                          "question, avec lien et origine.")
    async def rag_chercher(question: Question, ctx: Context, type: Nom | None = None,
                           tags: Tags = None, source: Nom | None = None,
                           limite: Annotated[int, Field(ge=1, le=LIMITE_MAX)] = 8) -> str:
        f = Filtres(type=type, tags=tags, sources=[source] if source else None)
        r = await appeler(etat.chercher, question, f, limite,
                          _niveaux(etat, ctx))
        return json.dumps([{"cle": x.cle, "fil": x.fil, "texte": x.texte, "lien": x.lien,
                            "origine": x.origine, "score": x.score} for x in r],
                          ensure_ascii=False)

    @mcp.tool(description="Liste exhaustivement les notes ou fichiers qui répondent à une "
                          "demande de liste (filtres extraits de la question s'ils manquent).")
    async def rag_lister(question: Question, ctx: Context, type: Nom | None = None,
                         tags: Tags = None) -> str:
        f = Filtres(type=type, tags=tags) if type or tags else None
        r = await appeler(etat.lister, question, f, _niveaux(etat, ctx))
        return json.dumps(r, ensure_ascii=False, default=str)

    @mcp.tool(description="Répond à une question d'après les notes, avec citations [n] "
                          "vérifiées, ou refuse si les notes ne le disent pas.")
    async def rag_repondre(question: Question, ctx: Context) -> str:
        r = await appeler(etat.repondre, question, _niveaux(etat, ctx))
        return json.dumps({"refus": r.refus, "texte": r.texte, "sources": r.sources},
                          ensure_ascii=False)

    @mcp.tool(description="Trouve le chemin le plus court entre deux éléments du graphe "
                          "(notes, fichiers, symboles).")
    async def rag_chemin(de: Nom, vers: Nom, ctx: Context) -> str:
        return json.dumps(await appeler(etat.chemin, de, vers, _niveaux(etat, ctx)),
                          ensure_ascii=False, default=str)

    @mcp.tool(description="Explique un élément du graphe : sa nature, sa communauté et ses "
                          "voisins.")
    async def rag_expliquer(nom: Nom, ctx: Context) -> str:
        return json.dumps(await appeler(etat.expliquer, nom, _niveaux(etat, ctx)),
                          ensure_ascii=False, default=str)

    return mcp


