# RAG maître

Index et recherche hybride (vecteur + plein texte + flou) sur le coffre Obsidian Maison et le code
des dépôts `nicolaslallier/*`, avec graphe de connaissances. Deux processus dans une même image :
l'API (`rag servir`, port 8000, joignable seulement sur `infra-net`) et l'indexeur
(`rag travailleur`). Les modèles passent par le LiteLLM du homelab, rien ne sort pour l'inférence.
Spec : `docs/superpowers/specs/2026-11-21-rag-maitre-design.md`.

## Développer

    docker compose -f compose.dev.yml up -d     # Postgres 18 + pgvector sur localhost:5434
    uv sync
    uv run ruff check . && uv run pytest -m "not vivant and not lent"

Les tests utilisent `RAG_TEST_DATABASE_URL` (défaut `postgresql://rag:rag@localhost:5434/rag_test`,
créée au besoin). Les tests `vivant` appellent le vrai LiteLLM (ils sautent sans `RAG_LITELLM_CLE`) :
`uv run pytest -m vivant`. Les réglages se lisent dans l'environnement ou dans un `.env` (jamais
commité). Pour travailler sur le vrai coffre (lecture seule) :

    export RAG_DATABASE_URL=postgresql://rag:rag@localhost:5434/rag RAG_LITELLM_URL=... RAG_CONFIG=config/dev.yaml
    uv run rag migrer && uv run rag indexer --complet
    uv run rag chercher "recette de poulet"

## Réponse diffusée (SSE)

`POST /repondre` ou `/question` avec `"flux": true` émet des événements `data: <json>` :
`sources`, puis des `texte` (`delta`), puis `fin`. En flux, la réponse n'est vérifiée qu'à la
fin et il n'y a pas de seconde tentative : **un client doit traiter `fin.citations_valides:
false` comme un refus** (masquer le texte reçu, afficher « Je n'ai pas trouvé cela dans vos
notes. »). Un événement `erreur` signale une panne du modèle en cours de flux.

Image : `docker build -t rag:local .` (le poids du reranker n'est pas dans l'image, il se télécharge
au premier démarrage sous `/donnees/modeles`).

## Banc

    uv run rag banc --jeu banc/etalon.yaml --etiquette essai            # indexe puis mesure
    uv run rag banc --jeu banc/etalon.yaml --etiquette essai --sans-indexer
    uv run rag banc --jeu banc/etalon.yaml --etiquette v1 --reference    # fixe la référence

Règle : un paramètre à la fois, comparé à la référence ; consigner le résultat dans `docs/`.

## Contrat avec l'Infra

| Élément | Valeur | Fourni par |
|---|---|---|
| Réseau | `infra-net` (externe) ; aucun port publié, aucun vhost NGINX | Infra (existe) |
| Pile | Portainer `rag`, environnement local du homelab (`192.168.2.10`) ; services `rag` (API, port 8000) et `rag-indexeur` ; fichier `deploy/portainer-stack.yml` du dépôt RAG | RAG |
| Image | `ghcr.io/nicolaslallier/rag:sha-<7 car.>` (paquet privé) | RAG |
| Base | base `rag`, rôle `rag`, hôte `postgres:5432` ; mot de passe `RAG_DB_PASSWORD` (lettres et chiffres seulement) ; extension `vector` créée par `make provision-app`, `unaccent` et `pg_trgm` créées par la migration du RAG | Infra provisionne, RAG migre |
| OpenBao | secret KV v2 `infra/rag` (chemin d'API `infra/data/rag`) ; politique `rag-deploy` (lecture de ce secret seulement) ; clés : `RAG_DB_PASSWORD`, `RAG_LITELLM_CLE`, `RAG_JETON`, `RAG_JETON_PRIVE` (facultative), `RAG_JETON_REINDEXER`, `RAG_GITHUB_JETON`, `RAG_S3_SECRET_KEY`, `RAG_COFFRE_PREFIXE` (facultative) | Infra (politique), Nicolas (valeurs) |
| LiteLLM | `http://litellm:4000` ; clé d'alias `rag`, équipe `agents` (modèles locaux seulement) ; modèles utilisés : `qwen3-embedding:0.6b`, `gemma4:26b-a4b-it-qat`, `qwen3.8:27b-mlx` (juge) | Infra |
| S3 (coffre) | `http://s3:8333`, bucket `obsidian`, identité `rag` en **`Read,List`**, secret `RAG_S3_SECRET_KEY` ; préfixe du coffre `RAG_COFFRE_PREFIXE` = valeur de `ORGANIZER_VAULT_PREFIX` de l'Infra | Infra |
| GitHub (code) | `RAG_GITHUB_JETON` : jeton à portée fine, `Contents: Read` sur les dépôts `nicolaslallier/*` | Nicolas |
| MCP | LiteLLM `mcp_servers.rag` → `http://rag:8000/mcp`, `Authorization: Bearer` = `RAG_MCP_TOKEN` (même valeur que `RAG_JETON`) ; outils `rag_chercher`, `rag_lister`, `rag_repondre`, `rag_chemin`, `rag_expliquer` ; équipes `[admin]` | Infra (config), RAG (serveur) |
| Runner | `runner-rag`, label `rag`, dépôt `nicolaslallier/RAG`, `GH_RUNNER_TOKEN_RAG`, sans socket Docker | Infra |
| Env. des conteneurs | `RAG_DATABASE_URL`, `RAG_LITELLM_URL`, `RAG_LITELLM_CLE`, `RAG_JETON`, `RAG_JETON_PRIVE`, `RAG_JETON_REINDEXER`, `RAG_GITHUB_JETON`, `RAG_S3_ENDPOINT`, `RAG_S3_ACCESS_KEY`, `RAG_S3_SECRET_KEY`, `RAG_COFFRE_BUCKET`, `RAG_COFFRE_PREFIXE`, `RAG_DONNEES_DIR`, `RAG_CONFIG` | RAG |
| Niveaux de visibilité | `RAG_JETON` → `public-maison` ; `RAG_JETON_PRIVE` → `public-maison` + `prive` ; `RAG_JETON_REINDEXER` → réindexation seulement, aucune lecture | RAG |

Valeurs en double à garder égales (comme `PORTAIL_DB_PASSWORD`) : `RAG_DB_PASSWORD` et `RAG_S3_SECRET_KEY` vivent dans `infra/env` (lus par `make provision-app` et `make s3-provision`) **et** dans `infra/rag` (lus par le déploiement du RAG) ; `RAG_MCP_TOKEN` (`infra/env`, lu par LiteLLM) = `RAG_JETON` (`infra/rag`).

## Étapes manuelles avant le premier déploiement

Dans cet ordre :

1. Nicolas crée `nicolaslallier/RAG` (privé) et pousse `main`.
2. PR A de l'Infra fusionnée et ses étapes manuelles faites : base et rôle `rag`, identité S3 `rag`
   en `Read,List`, clé LiteLLM `rag`, secret OpenBao `infra/rag` rempli, politique et jeton
   `rag-deploy`, runner `runner-rag` (label `rag`). Les jetons se génèrent par
   `openssl rand -hex 32` (un par jeton, tous différents) ; `RAG_DB_PASSWORD` par
   `openssl rand -hex 24`.
3. GitHub → Settings → Environments → `production`, limité à la branche `main` : secrets
   `PORTAINER_API_KEY` et `BAO_TOKEN` (le jeton `rag-deploy`) ; variable `PORTAINER_ENDPOINT_ID`
   (environnement local du homelab) et, au besoin, `PORTAINER_URL`, `VAULT_URL`,
   `INFRA_CA_CERT_PATH` (défaut `/etc/rag/infra-ca.crt` dans le runner).
4. Portainer → Registries → ajouter `ghcr.io` avec un jeton GitHub `read:packages`, autorisé sur
   l'environnement du homelab (le paquet `rag` est privé).
5. Règle de branche sur `main` : contrôle `ci` requis, pas de poussée directe.

## Exploitation

- État des passes : `GET /etat` (jeton de lecture) depuis un conteneur d'`infra-net` :
  `curl -H "Authorization: Bearer $RAG_JETON" http://rag:8000/etat`.
- Relancer une passe : `POST /reindexer` avec `RAG_JETON_REINDEXER` (`{"source": "<nom>"}` facultatif).
- Journaux : Loki, `{container=~"rag-.*"}`.
- Tourner un jeton : changer la valeur dans `infra/rag`, relancer le workflow `deploy`
  (Actions → deploy → Run workflow). `RAG_JETON` se change en même temps que `RAG_MCP_TOKEN` côté
  Infra (`infra/env`), sinon LiteLLM perd l'accès MCP.

## Sauvegarde

Rien à sauvegarder côté base : `rag` se reconstruit entièrement par `rag indexer --complet`. Seuls
`banc/` et `config/` (dans git) sont irremplaçables.
