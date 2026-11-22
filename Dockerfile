# Image du RAG maître : API (`rag servir`) et indexeur (`rag travailleur`).
# Les poids du reranker ne sont pas dans l'image : téléchargés au premier démarrage sous HF_HOME.
FROM ghcr.io/astral-sh/uv:0.9.8@sha256:08f409e1d53e77dfb5b65c788491f8ca70fe1d2d459f41c89afa2fcbef998abe AS uv

FROM python:3.13-slim@sha256:e48f59cdba69a3cbd9ed2cf0f348d90d45a8d350d98134cf1070bc106ee9ed90
RUN apt-get update && apt-get upgrade -y \
 && apt-get install -y --no-install-recommends git ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 # pip n'est pas utilisé (uv installe tout) ; ses dépendances embarquées (msgpack, urllib3, setuptools) ont des CVE.
 && python -m pip uninstall -y pip
COPY --from=uv /uv /usr/local/bin/uv

# Le reranker (torch CPU) est dans l'image ; `--build-arg GROUPES=` construit sans, pour un essai rapide.
ARG GROUPES="--group rerank"
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
# Deux temps : les dépendances (couche en cache), puis le projet.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev $GROUPES --no-install-project
COPY rag ./rag
COPY config ./config
# CA de l'Infra (certificat public) : LiteLLM et S3 sont joints en TLS par leurs vhosts.
COPY deploy/infra-ca.crt /etc/rag/infra-ca.crt
RUN uv sync --frozen --no-dev $GROUPES

RUN useradd --uid 1000 --home-dir /donnees --no-create-home rag \
 && mkdir /donnees && chown 1000:1000 /donnees
USER 1000
ENV PATH=/app/.venv/bin:$PATH HOME=/donnees HF_HOME=/donnees/modeles RAG_DONNEES_DIR=/donnees \
    RAG_CONFIG=/app/config/rag.yaml GIT_TERMINAL_PROMPT=0
VOLUME /donnees
EXPOSE 8000
CMD ["sh", "-c", "rag migrer && exec rag servir"]
