# RAG maître

Index et recherche hybride (vecteur + plein texte + flou) sur le coffre Obsidian Maison et le code
des dépôts `nicolaslallier/*`, avec graphe de connaissances. Deux processus dans une même image :
l'API (`rag servir`, port 8000, publié sur `192.168.2.40` seulement) et l'indexeur
(`rag travailleur`), avec un Postgres dédié, dans Colima sur le MacBook. Les modèles passent par le LiteLLM du homelab, rien ne sort pour l'inférence.
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

Image (`linux/arm64`, comme la cible) : `docker build -t rag:local .` (le poids du reranker n'est pas dans l'image, il se télécharge
au premier démarrage sous `/donnees/modeles`).

## Réponse diffusée (SSE)

`POST /repondre` ou `/question` avec `"flux": true` émet des événements `data: <json>` :
`sources`, puis des `texte` (`delta`), puis `fin`. En flux, la réponse n'est vérifiée qu'à la
fin et il n'y a pas de seconde tentative : **un client doit traiter `fin.citations_valides:
false` comme un refus** (masquer le texte reçu, afficher « Je n'ai pas trouvé cela dans vos
notes. »). Un événement `erreur` signale une panne du modèle en cours de flux.

## Banc

    uv run rag banc --jeu banc/etalon.yaml --etiquette essai            # indexe puis mesure
    uv run rag banc --jeu banc/etalon.yaml --etiquette essai --sans-indexer
    uv run rag banc --jeu banc/etalon.yaml --etiquette v1 --reference    # fixe la référence

Règle : un paramètre à la fois, comparé à la référence ; consigner le résultat dans `docs/`.

## Contrat avec l'Infra

| Élément | Valeur | Fourni par |
|---|---|---|
| Réseau **(changé)** | Réseau par défaut de la pile, dans la VM Colima du MacBook ; **pas d'`infra-net`**. Un seul port publié : `192.168.2.40:8000` (service `rag`), lié à cette adresse seulement, jamais `0.0.0.0` ; `rag-indexeur` et `rag-postgres` ne publient rien ; aucun vhost NGINX | RAG |
| Pile **(changé)** | Portainer `rag`, **environnement Mac-book** (Colima du MacBook, par l'agent `192.168.2.40:9001`) ; services `rag` (API, port 8000), `rag-indexeur` et **`rag-postgres`** ; fichier `deploy/portainer-stack.yml` du dépôt RAG, envoyé comme texte | RAG |
| Image **(changé)** | `ghcr.io/nicolaslallier/rag:sha-<7 car.>` (paquet privé), **`linux/arm64`** ; contient la CA de l'Infra (certificat public) en `/etc/rag/infra-ca.crt` | RAG |
| Base **(changé)** | **Postgres dédié dans la pile** : service `rag-postgres`, `pgvector/pgvector:0.8.6-pg18-bookworm` épinglé par digest (arm64), volume nommé `rag-pgdata`, aucun port publié, joignable seulement sur le réseau de la pile (`rag-postgres:5432`) ; base `rag`, rôle `rag` (propriétaire de son instance) ; mot de passe `RAG_DB_PASSWORD` (lettres et chiffres seulement) ; `vector`, `unaccent` et `pg_trgm` créées par la migration du RAG. La base `rag` du Postgres de l'Infra (`192.168.2.10`) **n'est plus utilisée ; elle est laissée en place, rien n'est supprimé** | **RAG** |
| OpenBao | secret KV v2 `infra/rag` (chemin d'API `infra/data/rag`) ; politique `rag-deploy` (lecture de ce secret seulement) ; clés : `RAG_DB_PASSWORD`, `RAG_LITELLM_CLE`, `RAG_JETON`, `RAG_JETON_PRIVE` (facultative), `RAG_JETON_REINDEXER`, `RAG_GITHUB_JETON` (facultative), `RAG_CLE_SECRETS` (clé Fernet des jetons par source ; sans elle aucun jeton ne peut être enregistré), `RAG_S3_SECRET_KEY`, `RAG_COFFRE_PREFIXE` (facultative) | Infra (politique), Nicolas (valeurs) |
| LiteLLM **(changé)** | **`https://litellm.infra.famillelallier.net`**, vérifié par la CA de l'Infra (`RAG_LITELLM_CA`) ; clé d'alias `rag`, équipe `agents` (modèles locaux seulement) ; modèles utilisés : `qwen3-embedding:0.6b`, `gemma4:26b-a4b-it-qat`, `qwen3.8:27b-mlx` (juge) | Infra |
| S3 (coffre) **(changé)** | **`https://s3.infra.famillelallier.net`**, vérifié par la CA de l'Infra (`AWS_CA_BUNDLE`) ; bucket `obsidian`, identité `rag` en **`Read,List`**, secret `RAG_S3_SECRET_KEY` ; préfixe du coffre `RAG_COFFRE_PREFIXE` = valeur de `ORGANIZER_VAULT_PREFIX` de l'Infra | Infra |
| GitHub (code) | `RAG_GITHUB_JETON` **(facultatif)** : jeton à portée fine, `Contents: Read` sur les dépôts `nicolaslallier/*` ; ne sert plus qu'aux sources github.com sans jeton propre | Nicolas |
| MCP **(changé)** | LiteLLM `mcp_servers.rag` → **`http://192.168.2.40:8000/mcp`**, `Authorization: Bearer` = `RAG_MCP_TOKEN` (même valeur que `RAG_JETON`, niveau `public-maison`) ; **sept** outils : `rag_chercher`, `rag_lister`, `rag_repondre`, `rag_chemin`, `rag_expliquer`, **`rag_sources`** et **`rag_fichiers`** (ces deux derniers en lecture seule) ; équipes `[admin]` | Infra (config), RAG (serveur) |
| Runner **(précisé)** | `runner-rag`, label `rag`, dépôt `nicolaslallier/RAG`, `GH_RUNNER_TOKEN_RAG`, sans socket Docker ; **reste sur `192.168.2.10`** : il n'appelle que Portainer et OpenBao, par URL ; variable `PORTAINER_ENDPOINT_ID` = `6` | Infra |
| Env. des conteneurs **(changé)** | `RAG_DATABASE_URL`, `RAG_LITELLM_URL`, **`RAG_LITELLM_CA`**, `RAG_LITELLM_CLE`, `RAG_JETON`, `RAG_JETON_PRIVE`, `RAG_JETON_REINDEXER`, `RAG_GITHUB_JETON`, `RAG_CLE_SECRETS`, `RAG_SOURCES_AMORCE`, `RAG_S3_ENDPOINT`, `RAG_S3_ACCESS_KEY`, `RAG_S3_SECRET_KEY`, **`AWS_CA_BUNDLE`**, `RAG_COFFRE_BUCKET`, `RAG_COFFRE_PREFIXE`, `RAG_DONNEES_DIR`, `RAG_CONFIG` ; pour `rag-postgres` : `POSTGRES_USER`, `POSTGRES_DB`, `POSTGRES_PASSWORD` (= `RAG_DB_PASSWORD`) | RAG |
| Niveaux de visibilité **(changé)** | `RAG_JETON` → `public-maison` ; `RAG_JETON_PRIVE` → `public-maison` + `prive` + `travail` **et gestion de l'index par l'interface `/ui/`**, **à n'utiliser que depuis le MacBook lui-même** (le port 8000 est en HTTP) ; `RAG_JETON_REINDEXER` → réindexation seulement, aucune lecture | RAG |

Valeurs en double à garder égales **(changé)** : `RAG_S3_SECRET_KEY` (`infra/env` et `infra/rag`) ; `RAG_MCP_TOKEN` (`infra/env`) = `RAG_JETON` (`infra/rag`). **`RAG_DB_PASSWORD` n'est plus un double** : seule la copie d'`infra/rag` compte ; celle d'`infra/env` ne sert plus qu'à la base inutilisée de l'Infra.

## Gestion de l'index

Interface web servie par l'API elle-même (même processus, même port, aucun service ajouté) :
`http://192.168.2.40:8000/ui/`, depuis le MacBook. On s'y connecte en collant un jeton ; il reste
dans un cookie `rag_session` (`HttpOnly`, `SameSite=Strict`, limité à `/ui`).

- `RAG_JETON` : lecture seule. Voit les sources `public-maison`, leurs fichiers (indexés ou exclus,
  avec la raison), le détail des morceaux d'un fichier et la liste des exclusions.
- `RAG_JETON_PRIVE` : voit aussi les sources `prive` et `travail` et **peut écrire** : exclure un fichier ou un
  motif (aperçu puis confirmation, seconde confirmation au-delà de 100 documents), annuler une
  exclusion, importer et exporter les exclusions, demander une réindexation. Sans
  `RAG_JETON_PRIVE` dans l'environnement, l'interface est en lecture seule.
- Les écritures n'existent que sous `/ui/` : ni l'API par jeton `Bearer` ni le MCP ne peuvent
  exclure ou annuler.
- Côté MCP (lecture seule, mêmes niveaux de visibilité que le jeton) : `rag_sources` rend
  `{"sources": [...], "en_cours": {"genre", "debut"} | null}` (les sources et leur état, et la
  passe d'indexation en cours, comme l'écran Sources), `rag_fichiers` liste les fichiers d'une
  source (indexés ou exclus, avec la raison).
- Un motif d'exclusion suit `fnmatch` (le `*` traverse les `/`), 1 à 1000 caractères ; la raison est
  facultative (500 caractères au plus). Le contenu d'un fichier exclu n'est jamais lu ni affiché.

### Sources git gérées dans l'interface

Les sources de code ne sont plus dans `config/rag.yaml` : elles vivent dans la base et se gèrent
depuis l'écran Sources (`RAG_JETON_PRIVE` requis) : « Ajouter une source git », « Modifier » et
« Supprimer » par source, « Exporter » et « Importer » pour l'ensemble. En ligne de commande :
`rag sources lister|exporter|importer <fichier>`.

- **Ajout** : la source est indexée tout de suite (elle apparaît « en attente » jusqu'à la passe).
- **Modification** : un changement de dépôt ou de branche déclenche une réindexation. La branche est
  vérifiée avant d'enregistrer : une branche absente donne un message d'erreur et rien n'est changé.
  Si l'**hôte** change, le jeton enregistré est retiré, sauf si un nouveau jeton est saisi.
- **Suppression** (après confirmation) : efface complètement la source (fichiers, morceaux, clone
  local, effacé au tour suivant du travailleur).
- **Jetons par source** : chiffrés en base avec `RAG_CLE_SECRETS`, une clé Fernet. La générer par
  `uv run python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`
  et la créer dans OpenBao (`infra/rag`) avec les autres secrets de la pile `rag`. Une clé invalide
  empêche l'API et le travailleur de démarrer ; sans clé, aucun jeton par source ne s'enregistre.
  **Perdre la clé** : les jetons enregistrés sont illisibles, il faut les ressaisir.
  Une clé **valide mais différente** n'arrête pas le démarrage : les jetons stockés deviennent
  illisibles, les sources retombent en accès anonyme (avertissement au journal), les dépôts privés
  échouent à leur passe → ressaisir les jetons dans l'écran Sources.
- **`RAG_GITHUB_JETON` est devenu facultatif** : il ne sert que pour les sources github.com qui n'ont
  pas de jeton propre.
- **Amorçage unique** : `RAG_SOURCES_AMORCE` (`config/sources-maison.yaml`, les 50 sources de la
  maison) est lu une seule fois par `rag migrer` ; un marqueur dans la table `instance` empêche de
  recommencer, même si des sources sont supprimées ensuite. `RAG_SOURCES_AMORCE` est posé par le
  fichier de pile lui-même (`deploy/portainer-stack.yml`), pas fourni par l'Infra. Une fois la prod
  amorcée, on peut le retirer de la pile.

## Étapes manuelles avant le premier déploiement

Avant tout : Nicolas crée `nicolaslallier/RAG` (privé), la PR A de l'Infra est fusionnée et ses
étapes faites (clé LiteLLM `rag`, identité S3 `rag` en `Read,List`, secret `infra/rag`, politique et
jeton `rag-deploy`, `runner-rag`), règle de branche sur `main` (contrôle `ci` requis). Les jetons
se génèrent par `openssl rand -hex 32` (tous différents), `RAG_DB_PASSWORD` par `openssl rand -hex 24`.

Aucune n'est faite par un agent.

1. **Portainer → Environments** : vérifier que l'environnement de la VM Colima du MacBook (`192.168.2.40:9001`) note son identifiant (variable `PORTAINER_ENDPOINT_ID`). Si ce n'est pas 6, le dire avant la fusion : `PORTAINER_ENDPOINT_ID` (étape 3) prend la vraie valeur.
2. **Portainer → Registries** : `ghcr.io` avec un jeton `read:packages` ; puis Environments → cet environnement → Registries : **autoriser `ghcr.io` sur l'environnement Mac-book** (le paquet `rag` est privé ; sans cela le déploiement échoue au tirage de l'image).
3. **GitHub → `nicolaslallier/RAG` → Settings → Environments → `production`** : variable `PORTAINER_ENDPOINT_ID` = `6` ; secrets `PORTAINER_API_KEY` et `BAO_TOKEN` comme au plan 4.
4. **OpenBao** : rien à changer. `RAG_DB_PASSWORD` d'`infra/rag` devient le mot de passe du Postgres dédié à sa première création.
5. **MacBook** : Colima démarre avec la session (`brew services start colima` ou équivalent), le Mac ne dort pas, la réservation DHCP de `192.168.2.40` reste.
6. Après la fusion de la PR #5 : suivre R-7.
7. Après le premier déploiement : fusionner I-2, mettre `RAG_MCP_TOKEN` dans `infra/env`, `make up stack=platform` (inchangé).
8. **Avant d'activer le rerank** (`rerank.actif: true`) : libérer la mémoire de la VM en arrêtant puis supprimant les bases de dev devenues inutiles — `docker --context colima rm -f pg-coffre pg-usine-doc`, et `rag-dev-postgres` **une fois les plans 1 à 4 finis** (plus aucun test local n'en a besoin). Vérifier par `docker --context colima stats --no-stream`. N'agrandir la VM (`colima stop && colima start --memory 10`) que si la mesure de R-7 le demande encore.
9. **La base `rag` du Postgres de l'Infra** : ne rien faire. Elle reste, vide.

## Exploitation

- La pile tourne dans la VM Colima du MacBook : si le Mac ou Colima s'arrête, le service s'arrête.
- Santé : `GET /sante` répond `{"ok": true}` sans jeton (503 si la base est injoignable) ; avec
  `RAG_JETON` ou `RAG_JETON_PRIVE`, il ajoute `documents` (sources visibles par ce jeton) et `version`.
- État des passes : `curl -H "Authorization: Bearer $RAG_JETON" http://192.168.2.40:8000/etat`.
  Le port est en HTTP : `RAG_JETON_PRIVE` ne s'utilise que depuis le MacBook lui-même.
- Relancer une passe : `POST /reindexer` avec `RAG_JETON_REINDEXER` (`{"source": "<nom>"}` facultatif).
  Il répond toujours `202 {"ok": true}`, que la source existe ou non ; suivre l'effet par `/etat`.
- Un corps de requête de plus de 64 Ko est refusé (413) avant toute lecture, donc avant le jeton,
  sauf les imports de l'interface (1 Mo). Les filtres sont bornés (100 sources, 20 tags).
- Toute réponse porte `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`,
  `Referrer-Policy: same-origin` et une CSP stricte (`default-src 'self'`, sans script ni style en
  ligne) : un gabarit de l'interface ne doit contenir ni `<script>` en ligne, ni `style=`, ni `hx-on`.
- Journaux : Portainer (environnement Mac-book) ou `docker --context colima logs rag-rag-1` sur le Mac ;
  **pas dans Loki** (Alloy ne lit que l'hôte `192.168.2.10`).
- Tourner un jeton : changer la valeur dans `infra/rag`, relancer le workflow `deploy`
  (Actions → deploy → Run workflow). `RAG_JETON` se change en même temps que `RAG_MCP_TOKEN` côté
  Infra (`infra/env`), sinon LiteLLM perd l'accès MCP.
- Tourner `RAG_DB_PASSWORD` : `POSTGRES_PASSWORD` n'est lu qu'à la création du volume `rag-pgdata`.
  D'abord `docker --context colima exec -it rag-rag-postgres-1 psql -U rag -c "ALTER ROLE rag PASSWORD '…'"`,
  puis changer `infra/rag` et redéployer (ou supprimer le volume et réindexer).
- Rotation de la CA de l'Infra : copier le nouveau `certs/infra-ca.crt` de l'Infra dans
  `deploy/infra-ca.crt` et commiter (elle est dans l'image, en `/etc/rag/infra-ca.crt`).

## Sauvegarde

`rag-pgdata` ne contient que **deux** données irremplaçables : les **exclusions manuelles** et les
**sources git** (posées par l'interface `/ui/`). Tout le reste se reconstruit par `rag indexer --complet` depuis le coffre
(S3) et GitHub. **Exporter après chaque changement d'exclusion** : bouton « Exporter (YAML) » de
l'écran Exclusions, ou

    docker --context colima exec rag-rag-1 rag exclusions exporter > exclusions-rag.yaml

**Exporter aussi les sources après chaque changement** : bouton « Exporter » de l'écran Sources, ou

    docker --context colima exec rag-rag-1 rag sources exporter > sources-rag.yaml

(l'export ne contient jamais les jetons : à ressaisir après une restauration). Garder ces fichiers
hors de la VM. **Après une perte de la base**, vider `RAG_SOURCES_AMORCE` dans la pile *avant* de
redémarrer : sinon `rag migrer` ré-amorce depuis `sources-maison.yaml` et ressuscite les sources
supprimées depuis l'interface. Importer ensuite le dernier export des sources (et celui des
exclusions). Après un `colima delete` : relancer la pile (même précaution), réimporter
(boutons « Importer » des écrans Exclusions et Sources) puis réindexer. Seuls `banc/` et `config/` (dans git),
ces exclusions et ces sources sont irremplaçables. Le volume vit dans la VM : `colima delete` l'efface (comme
`rag-donnees`), une réindexation complète est alors à prévoir.
