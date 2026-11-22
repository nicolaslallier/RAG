# RAG maître — plan 4b : la pile `rag` sur Colima (amendement du plan 4 et du contrat)

> Amende `2026-11-21-rag-maitre-4-deploiement.md` (PR #5, branche `claude/rag-plan-4`) et le bloc « Contrat entre les dépôts RAG et Infra » recopié dans `…-0-index.md`, `…-4-deploiement.md` et `…-infra.md`. Rien d'autre du plan 4 ne change : là où ce document se tait, le plan 4 reste la référence.

**Décision de Nicolas (2026-11-22) :** la pile `rag` tourne « Sur Colima du Mac » : le Mac Studio, `192.168.2.35`. Le plan 4 la mettait sur l'hôte `192.168.2.10` (Windows/WSL), sur `infra-net`, sans port publié.

**Ce document :** les faits constatés (lecture seule), les options comparées et ce que Nicolas a retenu (« OK aux recommandations », avec le Postgres dédié), le contrat amendé, les tâches à modifier dans la PR #5, les tâches Infra et les étapes manuelles. Aucun code n'est changé ici.

## 0. La cible

Le **Mac Studio** (`192.168.2.35`, `hostname` = `Nicolass-Mac-Studio`, `hw.model` = `Mac16,9`, M4 Max, 14 cœurs, 36 Go ; `mac-infra` dans `monitoring/prometheus/targets/macos.yml`, `mac-studio.famillelallier.net` → `192.168.2.35`). Sa VM Colima (`default`, aarch64, 8 CPU, 8 Gio, 100 Gio) est **pontée et porte sa propre adresse LAN, `192.168.2.66`** : c'est sur cette adresse que Docker publie les ports (précédent : `dev-gateway` sur `192.168.2.66:2222`), pas sur `192.168.2.35`. Un port publié sans adresse (`0.0.0.0`) est aussi relayé sur `192.168.2.35` (9001 y répond) ; un port lié à `192.168.2.66` ne l'est pas (2222 y est fermé). Vu du LAN : `192.168.2.35` ouvre 22, 5000, 9001, 9100 et 11434 (Ollama : `qwen3.8:27b-mlx`, `gemma4:26b-a4b-it-qat`, `llama3.2:1b`) ; `192.168.2.66` ouvre 9001 (agent Portainer `portainer/agent:2.45.0`, en marche depuis le 2026-11-08, `restart: always`), 9100 et 2222 ; le port 8000 y est libre. Cette VM est l'**environnement Portainer 6** (`stacks.conf` : `dev … 6`, `colima-studio … 6`).

## 1. Faits constatés (2026-11-22, lecture seule)

### Portainer et le runner

- Le **serveur Portainer est sur `192.168.2.10`** (`docker-compose.portainer.yml` : `${LAN_IP}:9443`, `:9000`, `:8000`) ; `https://portainer.infra.famillelallier.net/api/system/status` répond 200 avec la CA maison, depuis le Mac et depuis un conteneur de Colima.
- L'environnement 6 est la VM Colima du Mac Studio. Preuves : `stacks.conf`, l'en-tête de `docker-compose.colima.yml` (« colima-studio environment 6, the Mac Studio's VM (192.168.2.66) »), `CLAUDE.md` (« Dev sessions »). **Non vérifié par l'API** (aucune clé utilisée) : Nicolas confirme l'identifiant dans Portainer → Environments.
- `runner-rag` (`docker-compose.runners.yml`, hôte `192.168.2.10`) n'a ni socket Docker ni `infra-net` : il appelle Portainer et OpenBao **par URL**, avec `extra_hosts` vers `192.168.2.10` et la CA en `/etc/rag/infra-ca.crt`. Portainer relaie vers l'agent de l'environnement visé. **Le runner n'a donc pas à bouger** : seule la variable `PORTAINER_ENDPOINT_ID` change.
- OpenBao est joignable du Mac (`https://vault.infra.famillelallier.net/v1/sys/health` → 200), mais rien sur le Mac n'en a besoin.
- `scripts/portainer-stack.sh` de la PR #5 crée la pile par `POST /stacks/create/standalone/string?endpointId=<id>` : il marche tel quel sur un environnement à agent.

### Ce que l'Infra publie déjà sur le LAN

Essais faits depuis le Mac **et** depuis un conteneur de Colima (`docker exec rag-dev-postgres …`) : mêmes résultats.

| Service | Adresse LAN | TLS | Preuve |
|---|---|---|---|
| Postgres | `192.168.2.10:5432` | **non** | `docker-compose.yml` (nginx) : `"${LAN_IP}:5432:5432"`, `nginx/stream.d/postgres.conf` relaie vers `postgres:5432` ; `pg_isready -h 192.168.2.10` depuis Colima : « accepting connections » ; `openssl s_client -starttls postgres` : « no peer certificate available » |
| LiteLLM | `https://litellm.infra.famillelallier.net` | oui, CA maison | `/health/liveliness` → 200 ; le port 4000 n'est pas publié |
| S3 (SeaweedFS) | `https://s3.infra.famillelallier.net` | oui, CA maison | → 403 sans signature (attendu) |
| S3, port direct | `http://192.168.2.10:8333` | non | `docker-compose.data.yml` : `"8333:8333" # S3 + STS (every request signed)` ; → 403 sans signature |
| OpenBao | `https://vault.infra.famillelallier.net` | oui | 200 ; le port 8200 n'est pas publié |
| Portainer | `https://portainer.infra.famillelallier.net`, `192.168.2.10:9443` | oui | 200 |
| GitHub, Hugging Face | Internet | oui | `getent hosts github.com` résout depuis un conteneur de Colima |

La résolution des noms `*.infra.famillelallier.net` marche dans les conteneurs de Colima (résolveur `192.168.5.1` de la VM → DNS du LAN → `192.168.2.10`).

**Aucune nouvelle exposition n'est donc nécessaire côté Infra pour les sorties du RAG** (et le Postgres du LAN, sans TLS, n'est finalement pas utilisé : voir 2.1). Le précédent existe : `CLAUDE.md` (« DarkAngel: runs on the Mac Studio ») décrit DarkAngel joignant Postgres par `${LAN_IP}:5432` depuis cette même VM.

**Ce qui manque, c'est le sens inverse** : LiteLLM (sur `192.168.2.10`) doit joindre le serveur MCP du RAG (`http://rag:8000/mcp` dans le contrat). Hors d'`infra-net`, le nom `rag` n'existe plus : il faut un port publié sur la VM. Précédent : `dev-gateway` publie `192.168.2.66:2222` (lié à l'adresse LAN de la VM, pas à `0.0.0.0`).

### Ressources

- VM Colima : 7,7 Gio vus par Docker, **0,9 Gio utilisés, 7,0 Gio disponibles** (`free -m`). Occupants : `rag-dev-postgres` 527 Mio, `pg-coffre` 93 Mio, `pg-usine-doc` 112 Mio, agent 86 Mio, `dev-*` 26 Mio. Disque : 23 Go d'images et 6 Go de cache de build sur 100 Gio.
- Sessions de dev sur la même VM : `max_active = 2` × `mem_limit 1536m` = 3 Gio au pire (`CLAUDE.md`, « Dev sessions »).
- Mac hôte : 36 Go, dont **30,9 Go « wired », 0,25 Go libres, 4,2 Go compressés** au moment de la mesure (Ollama garde `qwen3.8:27b-mlx`, 21 Go, et `gemma4` chargés). La contrainte réelle est l'hôte, pas la VM.
- Reranker : `config/rag.yaml` de la branche du plan 3 porte `rerank: {actif: false, …}` : tant que c'est le cas, l'API ne charge pas torch (quelques centaines de Mio). Actif, `bge-reranker-v2-m3` (568 M de paramètres, fp32) pèse environ 2,3 Go plus torch.
- arm64 : `uv.lock` (branche du plan 3) contient `torch-2.15.0+cpu-cp313-cp313-manylinux_2_28_aarch64.whl` et 70 roues `manylinux…aarch64` ; l'image de base et `uv` sont multi-architectures. **Aucune compilation à prévoir.** `deploy.yml` de la PR #5 construit `linux/amd64` seulement : cette image ne démarre pas sur Colima.

### Contradictions relevées dans l'Infra (à corriger là-bas, hors de ce plan)

1. `README.md`, « Runtime » : « This stack runs on **Docker Desktop for Mac** » et « Migrating from Colima ». `CLAUDE.md`, « Runtime: Docker Desktop » : l'hôte de déploiement est **le portable Windows** (`192.168.2.10`, `INFRA_HOST` du `Makefile`). Le README décrit un état ancien.
2. `CLAUDE.md` décrit Postgres, `data`, Keycloak, `auth` et Grafana sur le Mac mini (environnement 7) dans plusieurs sections, puis dit dans « Single-host layout » que tout est revenu sur le portable et que « the layout above wins ». `stacks.conf` confirme (`-` partout sauf `dev` et `colima-studio`) ; les ports 5432 et 8333 du mini sont fermés.
3. `data.famillelallier.net` et `s3.famillelallier.net` résolvent encore vers **`192.168.2.68`** (le mini), où 8333 est fermé. `docker-compose.dev.yml` fait passer `dev-egress` par `s3.famillelallier.net:8333` : cette route est probablement cassée. Le RAG ne doit pas utiliser ces deux noms.
4. `CLAUDE.md` dit que la VM du Mac Studio « also carries DarkAngel » : aucun conteneur DarkAngel n'y tourne (il reste un volume `darkangel_darkangel-web`), et `192.168.2.66:8088` est fermé.
5. `colima-studio-cadvisor-1` redémarre en boucle (`Restarting (2)`) : pas de métriques par conteneur pour cette VM tant que ce n'est pas réparé.
6. `docker-compose.colima.yml` parle encore de la pile `colima-mini` (environnement 7), absente de `stacks.conf`.

## 2. Options comparées

### 2.1 Base de données

| | A. Postgres de l'Infra par le LAN | B. Postgres dédié dans la pile Colima | C. A + TLS sur Postgres |
|---|---|---|---|
| Comment | `postgresql://rag:…@192.168.2.10:5432/rag` (relais nginx existant) | service `rag-postgres` (`pgvector/pgvector:0.8.6-pg18-bookworm`, la même image que la base de dev), volume dans la VM, aucun port publié | comme A, plus un certificat serveur sur Postgres et `sslmode=verify-full` |
| Travail | une URL à changer ; la base `rag` et le rôle existent déjà | un service, un volume, un test ; la migration crée déjà `vector` (`001_socle.sql`) | chantier Infra (certificat, `postgresql.conf`, tous les clients) |
| Exposition | aucune nouvelle : le port est déjà ouvert au LAN | aucune | aucune nouvelle |
| Risque sécurité | **mot de passe du rôle et contenu en clair sur le LAN** (y compris les morceaux de notes `prive`). Un appareil du LAN capable d'écouter (ARP) les lit. Risque déjà accepté pour DarkAngel. Le mot de passe passe par SCRAM (non rejouable), le contenu non. | rien ne circule : le meilleur des trois | chiffré de bout en bout |
| Mémoire dans la VM | 0 | + 0,3 à 0,6 Gio | 0 |
| Dépendances | le portable doit être allumé — mais LiteLLM et S3 y sont déjà, donc pas de dépendance de plus | la recherche textuelle survit à une panne de Postgres côté Infra, pas la recherche vectorielle (embedding par LiteLLM) | comme A |
| Exploitation | pgAdmin, sauvegarde nocturne, exporter : déjà là | ni pgAdmin ni sauvegarde ; sans gravité, la base se reconstruit par `rag indexer --complet` | comme A |
| Ce qui devient inutile | rien | la base `rag` de l'Infra, `RAG_DB_PASSWORD` dans `infra/env`, `rag` dans `APP_DATABASES` | rien |

**Décision de Nicolas : B, le Postgres dédié.** Rien de `prive` ne doit passer en clair sur le LAN, et A y faisait passer le contenu des notes. La base `rag` déjà créée dans le Postgres de l'Infra n'est plus utilisée ; elle est laissée en place, rien n'est supprimé.

### 2.2 S3 (coffre)

| | Vhost TLS | Port direct |
|---|---|---|
| Adresse | `https://s3.infra.famillelallier.net` | `http://192.168.2.10:8333` |
| En clair | non | le contenu des notes oui ; le secret non (SigV4 ne l'envoie pas) |
| Travail | `AWS_CA_BUNDLE=/etc/rag/infra-ca.crt` dans la pile (lu par boto3, aucun code) | une URL |
| Risque | Python 3.13 active `VERIFY_X509_STRICT` ; la CA maison l'a déjà fait échouer pour httpx (PR #2). boto3 passe par urllib3 : **à prouver par un essai réel** avant de s'y fier | aucun |

**Retenu : le vhost TLS**, avec un test vivant ; si la CA est refusée, corriger `client_s3` (lui passer le contexte de `contexte_tls`), jamais le port en clair.

### 2.3 Entrée : comment LiteLLM (et plus tard l'agent-doc) joint le RAG

| | A. Port publié sur l'adresse LAN de la VM | B. Relais nginx sur `192.168.2.10` avec l'alias `rag` | C. Vhost TLS `rag.infra.famillelallier.net` |
|---|---|---|---|
| Pile RAG | `ports: ["192.168.2.66:8000:8000"]` sur le service `rag` seulement | comme A | comme A |
| Infra | `url: http://192.168.2.66:8000/mcp` dans `litellm/config.yaml` | `listen 8000` dans nginx, alias `rag` sur `infra-net` : le contrat garde `http://rag:8000/mcp` | un vhost de plus |
| Risque | `RAG_JETON` et les réponses en clair entre `.10` et `.66` ; l'API devient joignable par tout le LAN, protégée par ses jetons (32 caractères au moins, refus au démarrage sinon) ; le routeur ne redirige pas 8000 | le même en clair sur le tronçon nginx → VM, et un service de plus dans nginx | chiffre le tronçon client → nginx seulement |

**Décision de Nicolas : A, accepté.** Le port est lié à `192.168.2.66` seulement, jamais à `0.0.0.0` (un port sans adresse serait aussi relayé sur `192.168.2.35`). Conséquence : `RAG_JETON_PRIVE` ne s'utilise que depuis le Mac Studio lui-même, pour que rien de `prive` ne traverse le LAN en clair.

### 2.4 Image

| | arm64 seul | multi-arch (amd64 + arm64) |
|---|---|---|
| Durée de build | un seul passage | double |
| Utilité d'amd64 | aucune : plus rien ne tourne sur `.10` | garde un repli vers `.10` |

**Retenu : `linux/arm64` seul**, construit sur un runner GitHub `ubuntu-24.04-arm` (natif) ; si ce type de runner n'est pas offert au dépôt privé, `docker/setup-qemu-action` sur `ubuntu-latest` (plus lent, mais il n'y a que des roues à dépaqueter).

### 2.5 Mémoire

Les limites (`rag` 4g, `rag-indexeur` 2g, `rag-postgres` 1g) sont des plafonds, pas des réservations. Estimation dans la VM de 8 Gio :

| | rerank inactif (config actuelle) | rerank actif |
|---|---|---|
| `rag` (API) | ≈ 0,3 Gio | ≈ 3 Gio |
| `rag-indexeur` | < 1 Gio | < 1 Gio |
| `rag-postgres` (comme `rag-dev-postgres` aujourd'hui : 527 Mio) | ≈ 0,5 Gio | ≈ 0,5 Gio |
| Déjà là (bases de dev, agent, `dev-*`) | 0,9 Gio | 0,9 Gio |
| Deux sessions de dev au plafond | 3 Gio | 3 Gio |
| **Total au pire** | **≈ 5,7 Gio : ça tient** | **≈ 8,4 Gio : ça ne tient pas** |

**Décision de Nicolas : garder 4g et 2g, ne pas agrandir la VM.** L'hôte n'a pas de marge (0,25 Go libres) : chaque Gio donné à la VM est pris à Ollama. Avant d'activer le rerank, **libérer d'abord les bases de dev de la VM** (`pg-coffre`, `pg-usine-doc`, puis `rag-dev-postgres` une fois les plans finis : ≈ 0,7 Gio, ce qui ramène le pire cas à ≈ 7,7 Gio), mesurer (R-7), et n'agrandir la VM qu'ensuite si la mesure l'exige. C'est une étape manuelle de Nicolas (section 6) : **aucun agent ne supprime un conteneur ou un volume.** Dans la VM Linux, torch n'a pas accès au GPU d'Apple (pas de MPS) : le rerank reste sur CPU, 8 cœurs M4.

## 3. Contrat amendé

Ce tableau remplace le bloc des trois fichiers. Les lignes changées sont marquées **(changé)**.

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

Contraintes globales du plan 4 amendées :

- « Le service n'est joignable que sur `infra-net` : aucun `ports:` » devient : **un seul `ports:`, sur le service `rag`, lié à `192.168.2.66` ; aucun autre service n'en a ; aucun vhost NGINX.**
- **Rien de `prive` ne passe en clair sur le LAN** : la base est dans la pile, le coffre et les modèles sont joints en TLS, le MCP de LiteLLM n'a que le jeton `public-maison`, et `RAG_JETON_PRIVE` ne s'utilise que depuis le Mac Studio. Ce qui passe en clair entre `192.168.2.10` et `192.168.2.66` : `RAG_JETON` et les réponses `public-maison` du MCP (accepté par Nicolas).
- « Rien ne sort du homelab pour les modèles » : inchangé.
- Le repli « GPU (GTX 1650) » du plan 4, tâche 6, step 7, disparaît : pas de GPU dans la VM.

### Ce qui devient inutile côté Infra

- Rien à ajouter à `infra-net` pour le RAG ; pas d'alias, pas de relais, pas de port à ouvrir.
- **La base `rag` du Postgres de l'Infra, `rag` dans `APP_DATABASES` et `RAG_DB_PASSWORD` dans `infra/env` ne servent plus.** On les laisse : rien n'est supprimé, ni par un agent ni par ce plan. La sauvegarde nocturne de l'Infra continuera de vider une base vide, sans conséquence.
- La phrase « on `infra-net` … no published port » et la ligne « Database » du README de l'Infra et de `CLAUDE.md` sont fausses pour le RAG : à réécrire (tâche I-1).
- **Reste nécessaire, sans changement** : `runner-rag` sur `192.168.2.10`, la clé LiteLLM `rag`, l'identité S3 `rag`, le secret `infra/rag` et la politique `rag-deploy`.

### Sauvegarde

Aucune, et c'est voulu : `rag-pgdata` ne contient rien d'irremplaçable, la base se reconstruit par `rag indexer --complet` depuis le coffre (S3) et GitHub. Seuls `banc/` et `config/` (dans git) sont irremplaçables, comme au plan 4. À savoir : le volume vit dans la VM, donc `colima delete` l'efface (comme `rag-donnees`) ; une réindexation complète est alors à prévoir.

## 4. Tâches à modifier dans la PR #5 (dépôt RAG)

Même discipline que le plan 4 : test qui échoue, code, test vert, commit.

- [ ] **R-0 : le contrat.** Remplacer le bloc des trois fichiers (`…-0-index.md`, `…-4-deploiement.md`, `…-infra.md`) par le tableau de la section 3 ; dans la spec, ligne « Auto-hébergé sur l'hôte Docker du homelab (`192.168.2.10`) » et ligne « Hôte : Windows, GTX 1650 », renvoyer à ce document.
- [ ] **R-1 : la CA dans l'image** (plan 4, tâche 1). Ajouter `deploy/infra-ca.crt` (copie de `~/Code/Infra/certs/infra-ca.crt`, certificat public, pas de clé) et `COPY deploy/infra-ca.crt /etc/rag/infra-ca.crt` dans le `Dockerfile`. Test : `docker run --rm rag:local python -c "import ssl; ssl.create_default_context().load_verify_locations('/etc/rag/infra-ca.crt')"` sort en 0. Une rotation de la CA de l'Infra demande un commit ici : l'écrire dans le README.
- [ ] **R-2 : la pile** (plan 4, tâche 2). Dans `deploy/portainer-stack.yml` :
  - nouveau service `rag-postgres` :
    ```yaml
    rag-postgres:
      image: pgvector/pgvector:0.8.6-pg18-bookworm@sha256:<digest de l'index multi-arch>
      environment:
        POSTGRES_USER: rag
        POSTGRES_DB: rag
        POSTGRES_PASSWORD: ${RAG_DB_PASSWORD:?}
      volumes: [rag-pgdata:/var/lib/postgresql]
      shm_size: 256m           # construction parallèle de l'index HNSW
      mem_limit: 1g
      security_opt: ["no-new-privileges:true"]
      restart: unless-stopped
      healthcheck:
        test: [CMD-SHELL, "pg_isready -U rag -d rag"]
        interval: 10s
        timeout: 5s
        retries: 5
        start_period: 30s
    ```
    Pas de `ports:`, pas de `networks:` (réseau par défaut de la pile). Le digest : celui que l'Infra épingle déjà dans `docker-compose.postgres.yml` (`sha256:2ba9ca5f2e7daa0f0e7723cba1ee9167bab54efd3640516a44ac1a928dd67e7a`). Il couvre `linux/arm64` : c'est le digest de l'image arm64 que `rag-dev-postgres` fait déjà tourner sur cette VM (`docker image inspect`). `read_only`, `cap_drop: [ALL]` et `user: 1000` ne s'appliquent pas à ce service : l'image officielle démarre en root pour préparer son volume, puis descend à `postgres`.
  - `RAG_DATABASE_URL: postgresql://rag:${RAG_DB_PASSWORD:?}@rag-postgres:5432/rag` ; `depends_on: {rag-postgres: {condition: service_healthy}}` sur `rag` et `rag-indexeur` (l'attente de la base de la PR #5 reste, pour un redémarrage de Postgres seul) ;
  - `RAG_LITELLM_URL: https://litellm.infra.famillelallier.net` et `RAG_LITELLM_CA: /etc/rag/infra-ca.crt` ;
  - `RAG_S3_ENDPOINT: https://s3.infra.famillelallier.net` et `AWS_CA_BUNDLE: /etc/rag/infra-ca.crt` ;
  - retirer `networks: [infra-net]` de l'ancre et le bloc `networks:` du bas ; ajouter `rag-pgdata:` aux volumes ;
  - service `rag` : `ports: ["192.168.2.66:8000:8000"]` ; les deux autres : rien.
  - `tests/test_pile.py` : `test_aucun_port_publie_et_reseau_externe` devient `test_un_seul_port_lie_au_lan` (`rag` publie exactement `192.168.2.66:8000:8000`, ni `rag-indexeur` ni `rag-postgres` n'ont de `ports`, rien ne nomme `infra-net`, aucune liaison `0.0.0.0` ni port nu) ; `test_durcissement` : `user`, `read_only` et `cap_drop` pour `rag` et `rag-indexeur`, `no-new-privileges`, `mem_limit` et `restart` pour les trois ; `test_noms_du_contrat` : les nouvelles URL, les deux variables de CA, `@rag-postgres:5432/rag`, services = `{rag, rag-indexeur, rag-postgres}`, image de `rag-postgres` épinglée par `@sha256:` ; un test `test_la_base_n_est_pas_sur_le_lan` (`192.168.2.10` absent de `RAG_DATABASE_URL`).
  - La migration `001_socle.sql` crée déjà `vector`, `unaccent` et `pg_trgm` : rien à changer. `rag` est le rôle initial de l'instance, il en a le droit.
  - À écrire dans le README : `POSTGRES_PASSWORD` n'est lu qu'à la création du volume. Tourner `RAG_DB_PASSWORD` demande donc `ALTER ROLE rag PASSWORD …` dans le conteneur avant de redéployer (ou de supprimer le volume et de réindexer).
- [ ] **R-3 : boto3 et la CA maison** (nouveau). Test vivant (`-m vivant`) : `client_s3(Reglages()).list_objects_v2(Bucket="obsidian", MaxKeys=1)` contre `https://s3.infra.famillelallier.net` avec `AWS_CA_BUNDLE`. S'il échoue sur `VERIFY_X509_STRICT`, faire passer `client_s3` par le contexte de `rag.modeles.contexte_tls` (paramètre `verify=` de boto3 ou `Reglages.s3_ca`), sans désactiver la vérification et **sans retomber sur `http://192.168.2.10:8333`** (le contenu du coffre y passerait en clair).
- [ ] **R-4 : l'image arm64** (plan 4, tâche 4). `deploy.yml`, job `image` : `platforms: linux/arm64` et `runs-on: ubuntu-24.04-arm` (repli : `ubuntu-latest` + `docker/setup-qemu-action` épinglé par sha) ; retirer le commentaire « amd64 seul (hôte de déploiement) ». Trivy analyse le digest poussé, inchangé. `tests/test_workflows.py` : un test `test_l_image_est_arm64`. La CI (`ci.yml`) reste sur `ubuntu-latest` : elle ne lance pas l'image.
- [ ] **R-5 : scripts** (plan 4, tâche 3). Aucun changement de code : `RAG_DB_PASSWORD` reste une clé obligatoire. Commentaire d'en-tête de `scripts/portainer-stack.sh` : « Portainer de l'Infra, environnement de Colima ».
- [ ] **R-6 : README** (plan 4, tâche 5). Tableau du contrat de la section 3 ; étapes manuelles de la section 6 ; « Exploitation » : les journaux se lisent dans Portainer (environnement 6) ou par `docker --context colima logs` sur le Mac — **pas dans Loki** (Alloy ne lit que l'hôte `192.168.2.10`) ; `RAG_JETON_PRIVE` ne s'utilise que depuis le Mac Studio ; le service s'arrête si le Mac Studio ou Colima s'arrête ; « Sauvegarde » : le paragraphe de la section 3.
- [ ] **R-7 : vérification du premier déploiement** (plan 4, tâche 6). Remplacer :
  - step 3 : « Sur l'hôte » → sur le Mac Studio, `docker --context colima exec rag-rag-1 …` ; `docker network inspect infra-net` → supprimé ; `docker --context colima port rag-rag-1` montre **seulement** `8000/tcp -> 192.168.2.66:8000`, et rien pour `rag-rag-postgres-1` ni `rag-rag-indexeur-1` ; depuis une autre machine du LAN, `curl -s -o /dev/null -w '%{http_code}' http://192.168.2.66:8000/etat` → `401`, `nc -z 192.168.2.66 5432` et `nc -z 192.168.2.35 8000` échouent.
  - step 4 : `curl` depuis le Mac sur `http://192.168.2.66:8000/etat`.
  - step 7 : « Latence sur l'hôte réel » = la VM Colima (CPU arm64, 8 cœurs) ; replis : `rerank.candidats: 15`, puis rerank réservé à `repondre` ; plus de repli GPU. Ajouter : `docker --context colima stats --no-stream` pendant le banc, noter le pic de `rag-rag-1` et de `rag-rag-postgres-1` dans `docs/mesures.md`.
  - step 8 : inchangé, après la PR B amendée (I-2).

## 5. Tâches Infra

- [ ] **I-1 (PR de doc, peut partir tout de suite).** `README.md` (« RAG (RAG repo) ») et `CLAUDE.md` : la pile est sur l'environnement 6, hors d'`infra-net`, avec son propre Postgres et un port `192.168.2.66:8000` ; recopier le tableau de la section 3 (en anglais, selon la convention du dépôt) ; noter que la base `rag`, `rag` dans `APP_DATABASES` et `RAG_DB_PASSWORD` d'`infra/env` ne servent plus et sont laissés en place. Recopier ce plan dans `Infra/docs/superpowers/plans/`. **Ne rien retirer** de `.env.example`, `check-env.sh` ni `APP_DATABASES` dans cette PR : `make up` exige encore ces variables.
- [ ] **I-2 (PR B du plan Infra, tâche 7, amendée).** `litellm/config.yaml` : `url: http://192.168.2.66:8000/mcp` ; `scripts/test_mcp_policy.py` : `test_the_rag_server_is_registered_in_litellm` attend cette URL. Prérequis réécrit : depuis `192.168.2.10`, `curl -s -o /dev/null -w '%{http_code}' -X POST http://192.168.2.66:8000/mcp` répond `401`. Le reste de la tâche 7 ne change pas.
- [ ] **I-3 (facultatif, hors RAG).** Corriger les six contradictions de la section 1, dont l'enregistrement DNS `s3.famillelallier.net` / `data.famillelallier.net` → `192.168.2.68` et `colima-studio-cadvisor-1` qui redémarre en boucle.
- Rien à faire sur : `docker-compose.runners.yml`, `stacks.conf`, nginx, OpenBao, `teams.yaml`, le provisionnement S3.

## 6. Étapes manuelles pour Nicolas

Aucune n'est faite par un agent.

1. **Portainer → Environments** : vérifier que l'environnement de la VM Colima du Mac Studio (`192.168.2.66:9001`) porte bien l'identifiant **6**. Si ce n'est pas 6, le dire avant la fusion : `PORTAINER_ENDPOINT_ID` (étape 3) prend la vraie valeur.
2. **Portainer → Registries** : `ghcr.io` avec un jeton `read:packages` ; puis Environments → cet environnement → Registries : **autoriser `ghcr.io` sur l'environnement 6** (le paquet `rag` est privé ; sans cela le déploiement échoue au tirage de l'image).
3. **GitHub → `nicolaslallier/RAG` → Settings → Environments → `production`** : variable `PORTAINER_ENDPOINT_ID` = `6` ; secrets `PORTAINER_API_KEY` et `BAO_TOKEN` comme au plan 4.
4. **OpenBao** : rien à changer. `RAG_DB_PASSWORD` d'`infra/rag` devient le mot de passe du Postgres dédié à sa première création.
5. **Mac Studio** : Colima démarre avec la session (`brew services start colima` ou équivalent), le Mac ne dort pas, la réservation DHCP de `192.168.2.66` reste.
6. Après la fusion de la PR #5 : suivre R-7.
7. Après le premier déploiement : fusionner I-2, mettre `RAG_MCP_TOKEN` dans `infra/env`, `make up stack=platform` (inchangé).
8. **Avant d'activer le rerank** (`rerank.actif: true`) : libérer la mémoire de la VM en arrêtant puis supprimant les bases de dev devenues inutiles — `docker --context colima rm -f pg-coffre pg-usine-doc`, et `rag-dev-postgres` **une fois les plans 1 à 4 finis** (plus aucun test local n'en a besoin). Vérifier par `docker --context colima stats --no-stream`. N'agrandir la VM (`colima stop && colima start --memory 10`) que si la mesure de R-7 le demande encore.
9. **La base `rag` du Postgres de l'Infra** : ne rien faire. Elle reste, vide.

## 7. Risques ouverts

1. **Identifiant de l'environnement** : `6` vient de la doc de l'Infra, pas de l'API de Portainer ; vérifié par Nicolas (étape manuelle 1).
2. **boto3 et la CA maison** sous Python 3.13 : R-3 le prouve ou le corrige.
3. **Mémoire** : 0,25 Go libres sur l'hôte avec les modèles chargés ; avec le rerank actif, le pire cas dépasse la VM tant que les bases de dev ne sont pas libérées (étape manuelle 8).
4. **Runner arm64 de GitHub** pour un dépôt privé : s'il manque, le build passe par QEMU (plus lent).
5. **Disponibilité** : le RAG dépend de deux machines (Mac Studio pour le service et sa base, portable pour LiteLLM, S3, Portainer).
6. **Adresse en dur** (`192.168.2.66`) à deux endroits (pile RAG, `litellm/config.yaml`) : un changement de bail DHCP casse le MCP sans bruit. Réservation DHCP obligatoire.
7. **Port 8000 en HTTP sur le LAN** (accepté) : `RAG_JETON` y circule en clair ; un appel fait avec `RAG_JETON_PRIVE` depuis une autre machine ferait passer du `prive` en clair, d'où la règle « depuis le Mac Studio seulement ». Pour un appelant `prive` distant, il faudra d'abord du TLS.
8. **Rotation de `RAG_DB_PASSWORD`** : sans effet sur un volume existant (voir R-2).
