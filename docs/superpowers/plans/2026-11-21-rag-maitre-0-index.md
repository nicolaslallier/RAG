# RAG maître (projet 1) — index des plans

Spec : `docs/superpowers/specs/2026-11-21-rag-maitre-design.md`. Le projet est découpé en quatre plans dans ce dépôt et un plan pour le dépôt Infra. Chaque plan livre un logiciel qui marche et se teste seul.

## Ordre de construction

| # | Plan | Dépôt | Livre | Tâches |
|---|---|---|---|---|
| 1 | `2026-11-21-rag-maitre-1-socle-coffre-banc.md` | RAG | Schéma, lecteurs (dossier, S3), indexeur incrémental du coffre, recherche hybride + flou + filtres + niveaux de visibilité, CLI et API `/chercher`, banc de mesure, jeu étalon proposé puis corrigé par Nicolas, jeu synthétique, référence de base. Spike graphify en tâche 2. | 13 |
| 2 | `2026-11-21-rag-maitre-2-code-graphe.md` | RAG | Découpage du code par symbole, clones `origin/main` des dépôts, graphe (graphify ou plan B, `[[liens]]`, sémantique de nuit), `chemin`, `expliquer`, expansion (interrupteur), doublons / orphelines / liens entrants, non-régression du coffre. | 9 |
| 3 | `2026-11-21-rag-maitre-3-reponse-aiguillage-mcp.md` | RAG | Rerank, refus calibré, `lister`, aiguillage, `repondre` avec citations vérifiées et diffusion, banc de génération, travailleur (5 min, nuit, réindexer), `/etat`, MCP, mesure de chaque levier, clôture v1. | 10 |
| I-A | `2026-11-21-rag-maitre-infra.md`, tâches 1 à 6 | Infra | Clé LiteLLM `rag`, base, identité S3 lecture seule, politique OpenBao, runner, README. | 6 |
| 4 | `2026-11-21-rag-maitre-4-deploiement.md` | RAG | Image, pile Portainer, scripts, workflows, premier déploiement vérifié. | 6 |
| I-B | `2026-11-21-rag-maitre-infra.md`, tâche 7 | Infra | Serveur MCP `rag` dans LiteLLM. | 1 |

Dépendances entre dépôts :
- Les plans 1 à 3 se font **en local** (base de dev en conteneur, coffre lu dans `~/Obsidian/Maison`, LiteLLM joint par `https://litellm.infra.famillelallier.net`). Seul prérequis côté Infra : la **tâche 1 du plan Infra** (clé `rag`), à faire avant la tâche 6 du plan 1 (premier appel réel à LiteLLM). La PR A de l'Infra peut donc partir en parallèle du plan 1.
- Le plan 4 attend : les plans 1 à 3, la PR A de l'Infra et ses étapes manuelles, et la création du dépôt `nicolaslallier/RAG` sur GitHub **par Nicolas**.
- La PR B de l'Infra attend le premier déploiement du RAG.

Points d'arrêt où Nicolas décide : plan 1 tâche 11 (corriger le jeu étalon), plan 2 tâche 8 (questions sur le code), plan 3 tâche 10 (config finale et cibles v1), plan 4 tâche 6 (fusion et déploiement).

## Décisions prises en écrivant les plans (laissées au builder par le spec)

- **Pile** : uv, Python 3.13, FastAPI, psycopg 3 en SQL direct (pas d'ORM), migrations en fichiers SQL numérotés. `halfvec(1024)`, index HNSW dès le départ avec `hnsw.iterative_scan`.
- **Où vit la pile de déploiement** : dans le dépôt RAG (`deploy/portainer-stack.yml`), comme le Portail ; l'Infra ne porte que la configuration partagée.
- **Synchronisation du code** : interrogation toutes les 5 minutes, pas de webhook (l'hôte n'a aucune entrée publique).
- **Coffre** : lu dans S3 par une identité `Read,List` ; changement détecté par etag, confirmé par sha256 ; une note modifiée depuis moins de 10 minutes attend la passe suivante (sauf « réindexer maintenant » et la passe de nuit).
- **Reranker** : dans le service (sentence-transformers, CPU), groupe de dépendances optionnel ; GPU seulement si la mesure sur l'hôte l'exige.
- **Graphe** : `noeuds` et `liens` dans le même Postgres ; `chemin` par parcours en largeur côté service.
- **Signal de fin de nuit** : un statut (`GET /etat`), pas un webhook.
- **Visibilité** : colonne `sources.visibilite` dès la première migration ; deux jetons de lecture (`RAG_JETON`, `RAG_JETON_PRIVE`) ; `niveaux` est un paramètre obligatoire de toute fonction de lecture.
- **Format du banc** : `banc/etalon.yaml`, `banc/synthetique.jsonl`, `banc/reference.json`, résultats horodatés hors git.
- **Écart avec le spec** : `symbole` est porté par `morceaux`, pas par `documents`.

## Contrat entre les dépôts RAG et Infra

Ce bloc est identique dans `2026-11-21-rag-maitre-0-index.md`, `…-4-deploiement.md` et `…-infra.md`. Le changer, c'est le changer aux trois endroits.

Amendé le 2026-11-22 par `docs/superpowers/plans/2026-11-22-rag-maitre-4b-colima.md` (pile sur Colima du Mac Studio).

| Élément | Valeur | Fourni par |
|---|---|---|
| Réseau **(changé)** | Réseau par défaut de la pile, dans la VM Colima du Mac Studio ; **pas d'`infra-net`**. Un seul port publié : `192.168.2.66:8000` (service `rag`), lié à cette adresse seulement, jamais `0.0.0.0` ; `rag-indexeur` et `rag-postgres` ne publient rien ; aucun vhost NGINX | RAG |
| Pile **(changé)** | Portainer `rag`, **environnement 6** (Colima du Mac Studio, par l'agent `192.168.2.66:9001`) ; services `rag` (API, port 8000), `rag-indexeur` et **`rag-postgres`** ; fichier `deploy/portainer-stack.yml` du dépôt RAG, envoyé comme texte | RAG |
| Image **(changé)** | `ghcr.io/nicolaslallier/rag:sha-<7 car.>` (paquet privé), **`linux/arm64`** ; contient la CA de l'Infra (certificat public) en `/etc/rag/infra-ca.crt` | RAG |
| Base **(changé)** | **Postgres dédié dans la pile** : service `rag-postgres`, `pgvector/pgvector:0.8.6-pg18-bookworm` épinglé par digest (arm64), volume nommé `rag-pgdata`, aucun port publié, joignable seulement sur le réseau de la pile (`rag-postgres:5432`) ; base `rag`, rôle `rag` (propriétaire de son instance) ; mot de passe `RAG_DB_PASSWORD` (lettres et chiffres seulement) ; `vector`, `unaccent` et `pg_trgm` créées par la migration du RAG. La base `rag` du Postgres de l'Infra (`192.168.2.10`) **n'est plus utilisée ; elle est laissée en place, rien n'est supprimé** | **RAG** |
| OpenBao | secret KV v2 `infra/rag` (chemin d'API `infra/data/rag`) ; politique `rag-deploy` (lecture de ce secret seulement) ; clés : `RAG_DB_PASSWORD`, `RAG_LITELLM_CLE`, `RAG_JETON`, `RAG_JETON_PRIVE` (facultative), `RAG_JETON_REINDEXER`, `RAG_GITHUB_JETON`, `RAG_S3_SECRET_KEY`, `RAG_COFFRE_PREFIXE` (facultative) | Infra (politique), Nicolas (valeurs) |
| LiteLLM **(changé)** | **`https://litellm.infra.famillelallier.net`**, vérifié par la CA de l'Infra (`RAG_LITELLM_CA`) ; clé d'alias `rag`, équipe `agents` (modèles locaux seulement) ; modèles utilisés : `qwen3-embedding:0.6b`, `gemma4:26b-a4b-it-qat`, `qwen3.8:27b-mlx` (juge) | Infra |
| S3 (coffre) **(changé)** | **`https://s3.infra.famillelallier.net`**, vérifié par la CA de l'Infra (`AWS_CA_BUNDLE`) ; bucket `obsidian`, identité `rag` en **`Read,List`**, secret `RAG_S3_SECRET_KEY` ; préfixe du coffre `RAG_COFFRE_PREFIXE` = valeur de `ORGANIZER_VAULT_PREFIX` de l'Infra | Infra |
| GitHub (code) | `RAG_GITHUB_JETON` : jeton à portée fine, `Contents: Read` sur les dépôts `nicolaslallier/*` | Nicolas |
| MCP **(changé)** | LiteLLM `mcp_servers.rag` → **`http://192.168.2.66:8000/mcp`**, `Authorization: Bearer` = `RAG_MCP_TOKEN` (même valeur que `RAG_JETON`, niveau `public-maison`) ; outils `rag_chercher`, `rag_lister`, `rag_repondre`, `rag_chemin`, `rag_expliquer` ; équipes `[admin]` | Infra (config), RAG (serveur) |
| Runner **(précisé)** | `runner-rag`, label `rag`, dépôt `nicolaslallier/RAG`, `GH_RUNNER_TOKEN_RAG`, sans socket Docker ; **reste sur `192.168.2.10`** : il n'appelle que Portainer et OpenBao, par URL ; variable `PORTAINER_ENDPOINT_ID` = `6` | Infra |
| Env. des conteneurs **(changé)** | `RAG_DATABASE_URL`, `RAG_LITELLM_URL`, **`RAG_LITELLM_CA`**, `RAG_LITELLM_CLE`, `RAG_JETON`, `RAG_JETON_PRIVE`, `RAG_JETON_REINDEXER`, `RAG_GITHUB_JETON`, `RAG_S3_ENDPOINT`, `RAG_S3_ACCESS_KEY`, `RAG_S3_SECRET_KEY`, **`AWS_CA_BUNDLE`**, `RAG_COFFRE_BUCKET`, `RAG_COFFRE_PREFIXE`, `RAG_DONNEES_DIR`, `RAG_CONFIG` ; pour `rag-postgres` : `POSTGRES_USER`, `POSTGRES_DB`, `POSTGRES_PASSWORD` (= `RAG_DB_PASSWORD`) | RAG |
| Niveaux de visibilité **(précisé)** | `RAG_JETON` → `public-maison` ; `RAG_JETON_PRIVE` → `public-maison` + `prive`, **à n'utiliser que depuis le Mac Studio lui-même** (le port 8000 est en HTTP) ; `RAG_JETON_REINDEXER` → réindexation seulement, aucune lecture | RAG |

Valeurs en double à garder égales **(changé)** : `RAG_S3_SECRET_KEY` (`infra/env` et `infra/rag`) ; `RAG_MCP_TOKEN` (`infra/env`) = `RAG_JETON` (`infra/rag`). **`RAG_DB_PASSWORD` n'est plus un double** : seule la copie d'`infra/rag` compte ; celle d'`infra/env` ne sert plus qu'à la base inutilisée de l'Infra.

## Risques ouverts

1. **graphify hors Claude Code** : `graphify update` (AST, sans LLM) existe en ligne de commande et s'installe par `pip install graphifyy` ; le spike (plan 1, tâche 2) doit le prouver en conteneur sans réseau. Son extraction sémantique vise Gemini ou l'agent hôte par défaut : la viser sur LiteLLM n'est pas acquis, d'où la branche « maison » du plan 2, tâche 7.
2. **Dépôt privé** : le banc contient des questions tirées du coffre, donc le dépôt RAG et son image restent privés. Le modèle du Portail (dépôt public cloné par Portainer) ne s'applique pas tel quel : la pile est envoyée comme texte et Portainer a besoin d'un registre GHCR configuré. À confirmer par Nicolas ; l'autre choix serait un dépôt public avec le banc rangé ailleurs.
3. **Latence du rerank sur CPU** (hôte Windows, GTX 1650 partagée avec Ollama) : 30 passages par un modèle de 568 M de paramètres peuvent dépasser le budget de 2 s. Mesuré au plan 4, tâche 7 ; replis prévus.
4. **Instruction de requête Qwen3 à travers LiteLLM → Ollama** : vérifiée par un test réel au plan 1, tâche 6. Si elle se perd en route, il faut le savoir avant toute mesure.
5. **Refus sans rerank** : la similarité cosinus sépare peut-être mal les questions sans réponse ; le calibrage (plan 3, tâche 2) le dira.
6. **Coffre S3 et copie locale** peuvent différer (retard de Remotely Save, préfixe) : la référence du banc est posée en local, vérifiée en production au plan 4.
7. **Petit jeu étalon** (≈ 91 notes, 30 à 50 questions) : un écart de 0,03 de rappel, c'est une ou deux questions. Le jeu synthétique sert de second avis ; ne pas sur-interpréter.
8. **Extensions Postgres** : `unaccent` et `pg_trgm` sont créées par le rôle `rag` (extensions « trusted » de PG 18). Si la base en décide autrement, les faire créer par `make psql` une fois.
9. **Trivy et torch** : l'image embarque torch CPU ; une faille HIGH sans correctif n'arrête pas le déploiement (`ignore-unfixed`), une faille corrigée en amont le fera.
10. **Approximation des tokens** (4 caractères par token) : assumée et marquée dans le code ; à remplacer si le banc montre des morceaux tronqués à l'embedding.
