# RAG maître — plan 4 : image, pile Portainer et déploiement (dépôt RAG)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Faire tourner le RAG maître sur le homelab : une image, une pile Portainer `rag` à deux services sur `infra-net`, déployée à chaque fusion sur `main` avec les secrets lus dans OpenBao.

**Architecture:** Même modèle que le Portail (dépôt AI) : la pile vit dans le dépôt de l'application (`deploy/portainer-stack.yml`), un workflow construit l'image vers GHCR, puis un runner auto-hébergé rend les secrets depuis OpenBao et appelle l'API de Portainer. Différence voulue : le dépôt RAG est **privé**, donc le fichier de pile est envoyé à Portainer comme texte (pas de clone Git par Portainer) et le paquet GHCR est tiré avec un registre configuré dans Portainer.

**Tech Stack:** Docker, uv, GitHub Actions, GHCR, Portainer CE (API), OpenBao (KV v2), bash + jq + curl.

**Spec:** `docs/superpowers/specs/2026-11-21-rag-maitre-design.md`. Prérequis : plans 1 à 3 terminés ; PR A du plan Infra fusionnée et ses étapes manuelles faites ; **le dépôt `nicolaslallier/RAG` créé sur GitHub par Nicolas (privé)** — ce plan ne le crée pas et ne pousse rien sans son accord.

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

## Global Constraints

- Le service n'est joignable que sur `infra-net` : aucun `ports:` dans la pile, aucun vhost NGINX.
- Aucun secret dans le dépôt, dans une image, dans un argument de commande ni dans un journal. Les scripts ne lisent les secrets que par variable d'environnement ou fichier en mode 600.
- Dépôt et paquet GHCR **privés** (le banc contient des questions tirées du coffre).
- Aucun workflow avec `pull_request_target`, `issue_comment` ou `workflow_run` ; le label `rag` (runner auto-hébergé) n'est utilisé que par le job `deploy`, sur `main`, dans l'environnement `production`.
- Actions GitHub épinglées par sha de commit (reprendre ceux de `git -C ~/Code/AI show origin/main:.github/workflows/deploy.yml`) ; images de base épinglées par digest.
- Conteneurs : utilisateur non root, `read_only: true`, `cap_drop: [ALL]`, `no-new-privileges`, limite de mémoire.
- Rien ne sort du homelab pour les modèles. Seule sortie Internet du service : GitHub (clones) et, au premier démarrage, le téléchargement des poids du reranker depuis Hugging Face (un fichier, pas un appel de modèle).
- Un merge sur `main` redéploie la production : PR en brouillon, fusion par Nicolas seulement.

## Review Focus

1. **Clé absente ou vide dans OpenBao** : le déploiement s'arrête avant d'appeler Portainer, nomme la clé, n'affiche aucune valeur. → test tâche 3.
2. **Deux conteneurs démarrent ensemble et migrent en même temps** : un seul applique les migrations, l'autre attend. → test tâche 1.
3. **Premier démarrage sur un volume vide, ou Postgres / LiteLLM injoignables** : l'API démarre et `/sante` répond 503 sans boucle de redémarrage ; le travailleur réessaie. → tests tâche 1.
4. **Une variable ajoutée à la pile mais oubliée dans le script de rendu** (ou l'inverse) : détecté par un test, pas en production. → test tâche 2.
5. **Jeton trop court ou jetons égaux** (`RAG_JETON` = `RAG_JETON_REINDEXER`) : refusé au déploiement et au démarrage. → tests tâches 1 et 3.

---

### Task 1: Image du service

**Files:**
- Create: `Dockerfile`, `.dockerignore`
- Modify: `rag/db.py` (verrou de migration), `rag/api.py`, `rag/cli.py` (contrôles de démarrage)
- Test: `tests/test_demarrage.py`

**Interfaces:**
- Produces:
  - `migrer(conn)` prend `pg_advisory_lock(7240)` avant de lire `schema_migrations` et le relâche à la fin.
  - `verifier_reglages(r: Reglages, role: Literal["api", "travailleur"]) -> None`, appelée par `rag servir` et `rag travailleur` : lève `ErreurConfig` (message qui nomme la variable, jamais la valeur) si `RAG_JETON` ou `RAG_JETON_REINDEXER` fait moins de 32 caractères, si deux des trois jetons sont égaux, si `RAG_LITELLM_CLE` est vide ; pour le travailleur, si une source `s3` est configurée sans `RAG_S3_SECRET_KEY`, ou une source `code` en `https://github.com/` sans `RAG_GITHUB_JETON`.
  - `rag travailleur` : si la base est injoignable au démarrage, réessaie toutes les 30 s (journal d'une ligne), sans sortir.
  - Image : `python:3.13-slim@sha256:<digest>` ; `apt-get upgrade` puis `git` ; `uv` copié depuis `ghcr.io/astral-sh/uv:<version>@sha256:<digest>` ; `uv sync --frozen --no-dev --group rerank` en deux temps (dépendances, puis projet) ; utilisateur `rag` (uid 1000) ; `/donnees` créé et donné à 1000 ; `ENV PATH=/app/.venv/bin:$PATH HOME=/donnees HF_HOME=/donnees/modeles RAG_DONNEES_DIR=/donnees RAG_CONFIG=/app/config/rag.yaml GIT_TERMINAL_PROMPT=0` ; `EXPOSE 8000` ; `CMD ["sh", "-c", "rag migrer && exec rag servir"]`. `rag servir` lance uvicorn avec `--no-access-log` et `--no-proxy-headers`.
  - `.dockerignore` : `.git`, `.venv`, `donnees`, `banc`, `tests`, `docs`, `spike`, `.env*`.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_migrations_concurrentes(url_test):            # deux fils, deux connexions, sur une base vidée de son schéma
    r = list(ThreadPoolExecutor(2).map(lambda _: migrer(connexion(url_test)), range(2)))
    assert sorted(map(len, r)) == [0, N_MIGRATIONS]
@pytest.mark.parametrize("env,nom", [({"jeton": "court"}, "RAG_JETON"), ({"jeton_reindexer": J32}, "RAG_JETON_REINDEXER"),   # égal à jeton
                                     ({"litellm_cle": ""}, "RAG_LITELLM_CLE"), ({"jeton_prive": J32}, "RAG_JETON_PRIVE")])
def test_reglages_refuses(env, nom):
    with pytest.raises(ErreurConfig) as e: verifier_reglages(REGLAGES_OK.model_copy(update=env), "api")
    assert nom in str(e.value) and J32 not in str(e.value)
def test_travailleur_exige_ses_secrets(): ...          # source s3 sans RAG_S3_SECRET_KEY ; source github sans RAG_GITHUB_JETON
def test_sante_503_si_la_base_tombe(client_base_morte): assert client_base_morte.get("/sante").status_code == 503
def test_un_401_ne_journalise_pas_le_jeton(client, caplog):
    client.post("/chercher", json={"question": "x"}, headers={"Authorization": "Bearer secret-essai-123"}); assert "secret-essai-123" not in caplog.text
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_demarrage.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter** le code, puis le `Dockerfile` (résoudre les digests : `docker buildx imagetools inspect python:3.13-slim` et `ghcr.io/astral-sh/uv:<dernière version>`).
- [ ] **Step 4: Relancer** — attendu : SUCCÈS. Puis l'image : `docker build -t rag:local .` ; `docker run --rm rag:local rag --help` liste les sous-commandes ; `docker run --rm rag:local id -u` affiche `1000` ; `docker run --rm --read-only --tmpfs /tmp -v rag-essai:/donnees --network host -e RAG_DATABASE_URL=postgresql://rag:rag@localhost:5434/rag -e RAG_LITELLM_URL=http://x -e RAG_LITELLM_CLE=x -e RAG_JETON=$(openssl rand -hex 32) -e RAG_JETON_REINDEXER=$(openssl rand -hex 32) -e RAG_CONFIG=/app/config/dev.yaml rag:local` puis `curl -s localhost:8000/sante` rend `"ok": true`. Noter la taille de l'image.
- [ ] **Step 5: Commit** `git commit -am "feat: image du service, migrations sous verrou, contrôles de démarrage"`

---

### Task 2: Fichier de pile Portainer

**Files:**
- Create: `deploy/portainer-stack.yml`
- Test: `tests/test_pile.py`

**Interfaces:**
- Produces `deploy/portainer-stack.yml` :

```yaml
x-rag: &rag
  image: ghcr.io/nicolaslallier/rag:${IMAGE_TAG:?}
  environment: &env
    RAG_DATABASE_URL: postgresql://rag:${RAG_DB_PASSWORD:?}@postgres:5432/rag
    RAG_LITELLM_URL: http://litellm:4000
    RAG_LITELLM_CLE: ${RAG_LITELLM_CLE:?}
    RAG_JETON: ${RAG_JETON:?}
    RAG_JETON_PRIVE: ${RAG_JETON_PRIVE:-}
    RAG_JETON_REINDEXER: ${RAG_JETON_REINDEXER:?}
    RAG_GITHUB_JETON: ${RAG_GITHUB_JETON:?}
    RAG_S3_ENDPOINT: http://s3:8333
    RAG_S3_ACCESS_KEY: rag
    RAG_S3_SECRET_KEY: ${RAG_S3_SECRET_KEY:?}
    RAG_COFFRE_BUCKET: obsidian
    RAG_COFFRE_PREFIXE: ${RAG_COFFRE_PREFIXE:-}
    RAG_DONNEES_DIR: /donnees
    RAG_CONFIG: /app/config/rag.yaml
  volumes: [rag-donnees:/donnees]
  networks: [infra-net]
  read_only: true
  cap_drop: [ALL]
  security_opt: ["no-new-privileges:true"]
  tmpfs: [/tmp]
  restart: unless-stopped

services:
  rag:
    <<: *rag
    mem_limit: 4g            # reranker (568M de paramètres) + torch CPU
    healthcheck:
      test: [CMD, python, -c, "import urllib.request; urllib.request.urlopen('http://localhost:8000/sante', timeout=3)"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 120s     # premier démarrage : téléchargement du reranker
  rag-indexeur:
    <<: *rag
    command: ["sh", "-c", "rag migrer && exec rag travailleur"]
    mem_limit: 2g

networks:
  infra-net: {external: true, name: infra-net}
volumes:
  rag-donnees:
```

- [ ] **Step 1: Écrire les tests qui échouent**

```python
PILE = yaml.safe_load(Path("deploy/portainer-stack.yml").read_text()); BRUT = Path("deploy/portainer-stack.yml").read_text()
def test_aucun_port_publie_et_reseau_externe():
    assert all("ports" not in s for s in PILE["services"].values())
    assert PILE["networks"]["infra-net"] == {"external": True, "name": "infra-net"}
    assert all(s["networks"] == ["infra-net"] for s in PILE["services"].values())
def test_durcissement(): ...                           # chaque service : read_only, cap_drop ALL, no-new-privileges, mem_limit, restart
def test_noms_du_contrat():
    e = PILE["services"]["rag"]["environment"]
    assert e["RAG_S3_ACCESS_KEY"] == "rag" and e["RAG_COFFRE_BUCKET"] == "obsidian" and e["RAG_LITELLM_URL"] == "http://litellm:4000"
    assert "@postgres:5432/rag" in e["RAG_DATABASE_URL"] and set(PILE["services"]) == {"rag", "rag-indexeur"}
def test_les_variables_de_la_pile_sont_celles_du_script_de_rendu():
    pile = set(re.findall(r"\$\{([A-Z_]+)[:}]", BRUT)) - {"IMAGE_TAG"}
    script = Path("scripts/vault-render-env.sh").read_text()
    carte = set(re.search(r'^CLES="([^"]+)"', script, re.M).group(1).split())
    assert pile == carte
def test_aucun_secret_en_clair(): assert not re.search(r"(sk-|ghp_|github_pat_)[A-Za-z0-9]", BRUT)
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_pile.py -v` — attendu : ÉCHEC (le dernier test de cohérence passera à la tâche 3).
- [ ] **Step 3: Écrire** le fichier. Vérifier : `IMAGE_TAG=x RAG_DB_PASSWORD=x RAG_LITELLM_CLE=x RAG_JETON=x RAG_JETON_REINDEXER=x RAG_GITHUB_JETON=x RAG_S3_SECRET_KEY=x docker compose -f deploy/portainer-stack.yml config -q` sort en 0, et en 1 si on retire une variable obligatoire.
- [ ] **Step 4: Relancer** — attendu : SUCCÈS sauf `test_les_variables_de_la_pile_sont_celles_du_script_de_rendu`.
- [ ] **Step 5: Commit** `git add deploy tests/test_pile.py && git commit -m "feat: pile Portainer rag (API et indexeur sur infra-net)"`

---

### Task 3: Scripts de déploiement

**Files:**
- Create: `scripts/vault-render-env.sh`, `scripts/portainer-stack.sh`
- Test: `tests/test_scripts.py`

**Interfaces:**
- Partir des scripts du Portail (`git -C ~/Code/AI show origin/main:scripts/vault-render-env.sh` et `…:scripts/portainer-stack.sh`) et garder leurs protections : jeton passé à curl par fichier de config lu sur stdin, jamais en argv ; `--cacert` ; `umask 077` ; `::add-mask::` ; refus d'une valeur avec retour à la ligne ; renouvellement du jeton OpenBao à chaque déploiement.
- `scripts/vault-render-env.sh` — écrit `$RAG_STACK_ENV`. Différences avec le Portail :
  - `VAULT_SECRET_PATH` par défaut `infra/data/rag` ;
  - une ligne `CLES="RAG_DB_PASSWORD RAG_LITELLM_CLE RAG_JETON RAG_JETON_PRIVE RAG_JETON_REINDEXER RAG_GITHUB_JETON RAG_S3_SECRET_KEY RAG_COFFRE_PREFIXE"` (variable de la pile = clé du coffre, même nom) ; `FACULTATIVES=" RAG_JETON_PRIVE RAG_COFFRE_PREFIXE "` ;
  - refuse : `RAG_JETON`, `RAG_JETON_REINDEXER` (et `RAG_JETON_PRIVE` s'il n'est pas vide) de moins de 32 caractères ; deux de ces jetons égaux ; `RAG_DB_PASSWORD` qui n'est pas `^[A-Za-z0-9]+$`.
- `scripts/portainer-stack.sh up` — différences avec le Portail :
  - `STACK=rag`, `COMPOSE_FILE=deploy/portainer-stack.yml`, `REQUIRED="RAG_DB_PASSWORD RAG_LITELLM_CLE RAG_JETON RAG_JETON_REINDEXER RAG_GITHUB_JETON RAG_S3_SECRET_KEY IMAGE_TAG"` ;
  - le fichier de pile est envoyé comme texte, Portainer ne clone rien : création par `POST /stacks/create/standalone/string?endpointId=<id>` avec `{"Name": "rag", "StackFileContent": <contenu>, "Env": [...]}` ; mise à jour par `PUT /stacks/<id>?endpointId=<id>` avec `{"StackFileContent": <contenu>, "Env": [...], "Prune": true, "PullImage": true}` ; pile arrêtée (`Status == 2`) relancée d'abord. Le contenu entre dans `jq` par `--rawfile`, l'environnement par stdin.

- [ ] **Step 1: Écrire les tests qui échouent** (les scripts sont lancés contre un faux serveur HTTPS local — `http.server` + certificat auto-signé créé par la fixture — qui joue OpenBao et Portainer et enregistre les requêtes)

```python
def test_rendu_ecrit_les_cles_et_rien_sur_la_sortie(faux_coffre, tmp_path):
    r = lancer("scripts/vault-render-env.sh", env=ENV_OK(tmp_path)); lignes = (tmp_path / "rag.env").read_text().splitlines()
    assert r.returncode == 0 and "RAG_JETON=" + J1 in lignes and "RAG_JETON_PRIVE=" in lignes
    assert J1 not in r.stdout + r.stderr and oct((tmp_path / "rag.env").stat().st_mode)[-3:] == "600"
@pytest.mark.parametrize("coffre,attendu", [(SANS("RAG_S3_SECRET_KEY"), "RAG_S3_SECRET_KEY"), (AVEC(RAG_JETON="court"), "RAG_JETON"),
        (AVEC(RAG_JETON_REINDEXER=J1), "RAG_JETON_REINDEXER"), (AVEC(RAG_DB_PASSWORD="a/b"), "RAG_DB_PASSWORD"), (AVEC(RAG_JETON=J1 + "\nX=1"), "retour à la ligne")])
def test_rendu_refuse(coffre, attendu, tmp_path):
    r = lancer("scripts/vault-render-env.sh", env=ENV_OK(tmp_path), coffre=coffre)
    assert r.returncode != 0 and attendu in r.stderr and J1 not in r.stderr and faux_portainer.requetes == []
def test_cree_puis_met_a_jour_la_pile(faux_portainer, tmp_path):
    lancer("scripts/portainer-stack.sh", "up", env=...); c = faux_portainer.requetes[-1]
    assert c.chemin.startswith("/api/stacks/create/standalone/string") and "rag-indexeur" in c.json["StackFileContent"] and c.json["Name"] == "rag"
    faux_portainer.piles = [{"Id": 7, "Name": "rag", "EndpointId": 3, "Status": 1}]
    lancer("scripts/portainer-stack.sh", "up", env=...); m = faux_portainer.requetes[-1]
    assert (m.methode, m.chemin.split("?")[0]) == ("PUT", "/api/stacks/7") and m.json["PullImage"] is True and m.json["Prune"] is True
def test_aucun_secret_en_argv(): ...                   # ni `-H "X-Vault-Token` ni `-H "X-API-Key` dans les deux scripts
def test_shellcheck(): assert subprocess.run(["shellcheck", "scripts/vault-render-env.sh", "scripts/portainer-stack.sh"]).returncode == 0
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_scripts.py tests/test_pile.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Écrire** les deux scripts (`chmod +x`).
- [ ] **Step 4: Relancer** — attendu : SUCCÈS, y compris le test de cohérence pile ↔ script de la tâche 2.
- [ ] **Step 5: Commit** `git add scripts tests && git commit -m "feat: rendu des secrets depuis OpenBao et déploiement par l'API de Portainer"`

---

### Task 4: Workflows CI et déploiement

**Files:**
- Create: `.github/workflows/ci.yml`, `.github/workflows/deploy.yml`
- Test: `tests/test_workflows.py`

**Interfaces:**
- `ci.yml` : `on: [pull_request, workflow_call]`, `permissions: contents: read`, un job sur `ubuntu-latest` avec un service `pgvector/pgvector:0.8.6-pg18-bookworm` (utilisateur, mot de passe et base `rag`, port `5434:5432`, contrôle de santé `pg_isready`) ; étapes : checkout, `astral-sh/setup-uv`, `uv sync`, `uv run ruff check .`, `uv run ruff format --check .`, `uv run pytest -m "not vivant and not lent"`, `shellcheck scripts/*.sh`.
- `deploy.yml` : `on: push` sur `main` (`paths-ignore: ['**/*.md', 'docs/**', 'banc/**']`) et `workflow_dispatch` ; `concurrency: {group: deploy-rag, cancel-in-progress: false}` ; jobs :
  - `ci` : `uses: ./.github/workflows/ci.yml` ;
  - `image` (`needs: ci`, `ubuntu-latest`, `packages: write`) : buildx, connexion à GHCR, `docker/metadata-action` (`images: ghcr.io/nicolaslallier/rag`, `tags: type=sha`), construction `linux/amd64` poussée avec cache `gha`, puis Trivy sur le digest (`severity: CRITICAL,HIGH`, `ignore-unfixed: true`, `exit-code: 1`) ;
  - `deploy` (`needs: image`, `if: github.ref == 'refs/heads/main'`, `runs-on: [self-hosted, rag]`, `environment: production`, `timeout-minutes: 15`) : checkout sans identifiants persistants, puis, comme le Portail : `RAG_STACK_ENV="$RUNNER_TEMP/rag.env"` (supprimé par `trap`), `PORTAINER_API_KEY= scripts/vault-render-env.sh`, ajout de `IMAGE_TAG=sha-<7 car.>`, `BAO_TOKEN= scripts/portainer-stack.sh up`. Variables : `PORTAINER_URL`, `PORTAINER_ENDPOINT_ID`, `VAULT_URL`, `PORTAINER_CA_CERT` et `VAULT_CA_CERT` (défaut `/etc/rag/infra-ca.crt`) ; secrets de l'environnement `production` : `PORTAINER_API_KEY`, `BAO_TOKEN`.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
W = {n: yaml.safe_load(Path(f".github/workflows/{n}.yml").read_text()) for n in ("ci", "deploy")}
def test_aucun_declencheur_dangereux():
    for w in W.values(): assert not {"pull_request_target", "issue_comment", "workflow_run"} & set(w[True])     # PyYAML lit `on:` comme True
def test_le_runner_auto_heberge_ne_sert_qu_au_deploiement_de_main():
    auto = {n: j for w in W.values() for n, j in w["jobs"].items() if "self-hosted" in str(j.get("runs-on"))}
    assert set(auto) == {"deploy"} and auto["deploy"]["environment"] == "production" and "refs/heads/main" in auto["deploy"]["if"]
def test_actions_epinglees_par_sha():
    for f in Path(".github/workflows").glob("*.yml"):
        for u in re.findall(r"uses:\s*(\S+)", f.read_text()): assert u.startswith("./") or re.search(r"@[0-9a-f]{40}$", u), u
def test_la_ci_ne_lance_pas_les_tests_vivants(): assert 'not vivant and not lent' in Path(".github/workflows/ci.yml").read_text()
def test_un_seul_deploiement_a_la_fois(): assert W["deploy"]["concurrency"] == {"group": "deploy-rag", "cancel-in-progress": False}
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_workflows.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Écrire** les deux workflows.
- [ ] **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git add .github tests && git commit -m "ci: tests, image GHCR et déploiement Portainer sur main"`

---

### Task 5: README d'exploitation et étapes manuelles

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Écrire** les sections : « Ce que c'est » (5 lignes, lien vers le spec) ; « Développer » (base de dev, `.env`, tests, tests vivants) ; « Banc » (commandes, règle un paramètre à la fois) ; « Contrat avec l'Infra » (le tableau ci-dessus) ; « Étapes manuelles avant le premier déploiement », dans cet ordre, chacune avec sa commande :
  1. Nicolas crée `nicolaslallier/RAG` (privé) et pousse `main` ; décommenter la source `RAG` dans `config/rag.yaml`.
  2. PR A de l'Infra fusionnée, ses étapes manuelles faites (base, identité S3, clé LiteLLM, secret `infra/rag`, politique et jeton `rag-deploy`, `runner-rag`).
  3. GitHub → Settings → Environments → `production`, limité à `main` : secrets `PORTAINER_API_KEY` et `BAO_TOKEN` ; variables `PORTAINER_ENDPOINT_ID` (environnement local du homelab) et, au besoin, `PORTAINER_URL`, `VAULT_URL`, `INFRA_CA_CERT_PATH`.
  4. Portainer → Registries → ajouter `ghcr.io` avec un jeton `read:packages`, autorisé sur l'environnement du homelab (le paquet `rag` est privé).
  5. Règle de branche sur `main` : contrôle `ci` requis, pas de poussée directe.
  « Exploitation » : `GET /etat`, relancer une passe (`POST /reindexer`), lire les journaux (`{container=~"rag-.*"}` dans Loki), tourner un jeton (changer la valeur dans `infra/rag`, relancer le workflow ; `RAG_JETON` se change en même temps que `RAG_MCP_TOKEN` côté Infra) ; « Sauvegarde » : la base `rag` se reconstruit entièrement par `rag indexer --complet`, seuls `banc/` et `config/` (dans git) sont irremplaçables.
- [ ] **Step 2: Vérifier** que chaque nom du README existe : `grep -o 'RAG_[A-Z_]*' README.md | sort -u` est inclus dans les variables de `deploy/portainer-stack.yml` et de `rag/config.py`.
- [ ] **Step 3: Commit** `git commit -am "docs: README d'exploitation et étapes manuelles de déploiement"`

---

### Task 6: Premier déploiement et vérification (avec Nicolas)

Rien n'est poussé ni fusionné sans l'accord de Nicolas. Chaque step se fait avec lui.

- [ ] **Step 1: Pousser la branche et ouvrir la PR en brouillon** (après la création du dépôt par Nicolas). Attendu : `ci` vert.
- [ ] **Step 2: Fusion par Nicolas.** Suivre le workflow : `image` (Trivy vert), `deploy`. Si Trivy bloque sur une dépendance de torch sans correctif : le noter, ne pas le contourner sans accord.
- [ ] **Step 3: Vérifier la pile** dans Portainer (environnement 6) : `rag`, `rag-indexeur` et `rag-postgres` en marche, `rag` sain. Sur le Mac Studio : `docker --context colima exec rag-rag-1 rag chercher "recette de poulet"` rend la recette ; `docker --context colima port rag-rag-1` montre **seulement** `8000/tcp -> 192.168.2.66:8000`, rien pour `rag-rag-postgres-1` ni `rag-rag-indexeur-1` ; depuis une autre machine du LAN, `curl -s -o /dev/null -w '%{http_code}' http://192.168.2.66:8000/etat` → `401`, `nc -z 192.168.2.66 5432` et `nc -z 192.168.2.35 8000` échouent.
- [ ] **Step 4: Première passe.** `docker --context colima logs -f rag-rag-indexeur-1` : le coffre (S3) puis les dépôts s'indexent ; `curl` depuis le Mac sur `http://192.168.2.66:8000/etat` avec `RAG_JETON` : toutes les sources ont un `indexe_le`. Comparer le nombre de documents du coffre à celui du dev (dossier local) : un écart vient du préfixe (`RAG_COFFRE_PREFIXE`) ou d'une synchronisation Remotely Save en retard.
- [ ] **Step 5: Lecture seule S3, pour vrai.** Depuis l'hôte : `docker exec rag-rag-indexeur-1 python -c "from rag.config import Reglages; from rag.sources import client_s3; client_s3(Reglages()).put_object(Bucket='obsidian', Key='_essai-rag', Body=b'x')"` doit échouer en `AccessDenied`.
- [ ] **Step 6: Banc en production.** Copier `banc/` dans le conteneur (`docker cp banc rag-rag-1:/tmp/banc`) puis `docker exec rag-rag-1 rag banc --jeu /tmp/banc/etalon.yaml --etiquette prod --sans-indexer` ; comparer à la référence v1 : mêmes chiffres à 0,02 près (sinon : contenu du coffre S3 différent du local, à expliquer).
- [ ] **Step 7: Latence sur l'hôte réel** (la VM Colima : CPU arm64, 8 cœurs, sans GPU). Avec `rerank.actif` tel que retenu au plan 3 : 10 fois `rag chercher`, 3 fois `rag question` (une par nature). Budgets : moins de 2 s pour chercher et lister, moins de 30 s pour répondre. Pendant le banc, `docker --context colima stats --no-stream` : noter le pic de `rag-rag-1` et de `rag-rag-postgres-1` dans `docs/mesures.md`. Si le rerank dépasse 2 s : proposer à Nicolas, dans l'ordre, `rerank.candidats: 15`, puis le rerank réservé au mode `repondre` ; ne rien changer sans son accord.
- [ ] **Step 8: MCP** (après la PR B de l'Infra) : dans Claude Code, par LiteLLM, poser « Quelles recettes j'ai pour le poulet, avec les sources ? » ; la réponse vient de `rag_lister`.
- [ ] **Step 9: Nuit suivante** : `GET /etat` montre `derniere_nocturne.ok == true` et une `fin` après 03 h 00.

**Fin du plan 4 et du projet 1.**
