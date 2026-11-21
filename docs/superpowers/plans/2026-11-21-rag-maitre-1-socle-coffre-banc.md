# RAG maître — plan 1 : socle, coffre, recherche hybride, banc de mesure

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Indexer le coffre Maison dans Postgres+pgvector, chercher en hybride (vecteur + plein texte + flou) avec filtres, et mesurer la qualité avec un banc rejouable et une référence de base.

**Architecture:** Un paquet Python `rag/` sans ORM (SQL direct par psycopg), une base Postgres dédiée, LiteLLM comme seul fournisseur de modèles. L'indexeur lit une source par un « lecteur » (dossier local, S3 ; git au plan 2), ne retraite que ce qui a changé (signature puis sha256), et écrit des morceaux versionnés par config d'indexation. Le banc rejoue un jeu de questions et compare à une référence.

**Tech Stack:** Python 3.13, uv, FastAPI, psycopg 3, pydantic-settings, httpx, boto3, PyYAML, pytest, ruff ; Postgres 18 + pgvector 0.8 (`halfvec`), `unaccent`, `pg_trgm`.

**Spec:** `docs/superpowers/specs/2026-11-21-rag-maitre-design.md` (à lire en entier avant de commencer). Index des plans : `2026-11-21-rag-maitre-0-index.md`.

## Global Constraints

- **Rien ne sort du homelab** : tout appel de modèle passe par LiteLLM avec la clé `rag` (équipe `agents`, modèles locaux seulement). Aucun SDK ni URL de fournisseur infonuagique dans le code.
- Embeddings `qwen3-embedding:0.6b`, **1024 dimensions**, colonne `halfvec(1024)`. Génération `gemma4:26b-a4b-it-qat`. Juge du banc `qwen3.8:27b-mlx` (différent du générateur).
- Le coffre est lu dans **S3** (bucket `obsidian`) en **lecture seule** : aucun appel `put_object`, `delete_object`, `copy_object` dans le dépôt. En développement et en test, le coffre est le dossier local `~/Obsidian/Maison`, en lecture seule aussi.
- Une note modifiée depuis moins de **10 minutes** n'est pas (ré)indexée par la passe incrémentale (règle Remotely Save reprise de `agent-musique`).
- **Visibilité par source** : chaque source a un niveau, `public-maison` ou `prive` (colonne `sources.visibilite`, dès la première migration). Chaque jeton d'API donne droit à des niveaux ; **toute** requête qui lit des documents, des morceaux ou des nœuds filtre sur ces niveaux, dans le SQL, jamais par le modèle. Les fonctions de lecture prennent `niveaux` en paramètre obligatoire, sans valeur par défaut. En v1 toutes les sources sont `public-maison`.
- Le préfixe `_versions/` du coffre (copies gardées par agent-doc) n'est jamais indexé.
- Tous les leviers de qualité sont dans `config/rag.yaml` (versionné). Aucun réglage de recherche ou de découpage codé en dur.
- Chemins et noms comparés et stockés en **NFC**.
- Vérité du banc au niveau du **document**, clé `"<source>:<chemin>"`.
- Noms de tables et de code en français, comme le spec : `sources`, `documents`, `morceaux`, `noeuds`, `liens`, `index_versions`.
- Le dépôt RAG reste **privé** : `banc/` contient des questions tirées du coffre.
- Chaque commit : `uv run ruff check . && uv run pytest` verts. Messages de commit en français, style `feat: …`.
- Tests marqués `vivant` (vrai LiteLLM) : sautés si `RAG_LITELLM_CLE` est absent ; jamais lancés en CI.

## Review Focus

1. **Note sans frontmatter, ou frontmatter YAML invalide** : la note est indexée quand même (titre = nom du fichier, pas de tags), jamais d'exception qui arrête l'indexation du coffre. → test tâche 4.
2. **Listage partiel du coffre** (S3 vide, tronqué, mauvais préfixe : plus de la moitié des documents indexés absents) : l'indexeur refuse de supprimer et sort en erreur, sauf `forcer`. → test tâche 7.
3. **Chemins NFD de macOS** (`Modèles` décomposé) : même document que la forme NFC, ni doublon ni suppression/recréation à chaque passe. → tests tâches 3 et 7.
4. **Question vide de sens pour le plein texte** (`"?"`, `"c'est quoi ça"`, apostrophes, `&`, `:`) : pas d'erreur SQL, la branche vecteur répond seule. → test tâche 8.
5. **LiteLLM en panne au milieu d'une indexation** : le document en cours n'est pas écrit à moitié, les documents déjà faits restent, une relance reprend où c'était rendu. → test tâche 7.

---

## Structure des fichiers (plans 1 à 3)

```
pyproject.toml  uv.lock  .python-version  .gitignore  compose.dev.yml  README.md
config/rag.yaml            # config de prod (sources, leviers)
config/dev.yaml            # idem, coffre = dossier local, pas de dépôts de code
rag/config.py              # Reglages (env RAG_*) et Config (YAML)
rag/db.py                  # connexion(), migrer()
rag/migrations/001_socle.sql
rag/sources.py             # Entree, LecteurDossier, LecteurS3 (LecteurGit au plan 2)
rag/exclusions.py          # motif_exclusion()
rag/notes.py               # analyser_note()
rag/decoupage.py           # Morceau, 3 stratégies de notes, récursif
rag/modeles.py             # client LiteLLM (embeddings, génération)
rag/indexeur.py            # indexer_source()
rag/recherche.py           # chercher(), Filtres, Resultat
rag/api.py  rag/cli.py
rag/banc/metriques.py  rag/banc/execution.py  rag/banc/synthetique.py
banc/etalon.yaml  banc/synthetique.jsonl  banc/reference.json  banc/resultats/ (ignoré par git)
tests/conftest.py  tests/test_*.py
```

---

### Task 1: Ossature, config et schéma

**Files:**
- Create: `pyproject.toml`, `.python-version` (`3.13`), `.gitignore`, `compose.dev.yml`, `README.md`, `config/rag.yaml`, `config/dev.yaml`
- Create: `rag/__init__.py`, `rag/config.py`, `rag/db.py`, `rag/migrations/001_socle.sql`
- Test: `tests/conftest.py`, `tests/test_config.py`, `tests/test_db.py`

**Interfaces:**
- Produces:
  - `rag.config.Reglages(BaseSettings)` (préfixe d'env `RAG_`) : `database_url: str`, `litellm_url: str`, `litellm_cle: str = ""`, `litellm_ca: Path | None = None`, `jeton: str = ""` (donne `public-maison`), `jeton_prive: str = ""` (donne `public-maison` et `prive` ; vide = aucun jeton privé), `donnees_dir: Path = Path("donnees")`, `config: Path = Path("config/rag.yaml")`, `s3_endpoint: str = "http://s3:8333"`, `s3_access_key: str = "rag"`, `s3_secret_key: str = ""`, `coffre_bucket: str = "obsidian"`, `coffre_prefixe: str = ""`.
  - `rag.config.Config` (pydantic) avec `sources: list[SourceConf]`, `indexation: IndexationConf`, `recherche: RechercheConf`, `reponse: ReponseConf`, `banc: BancConf` ; champs = exactement les clés du YAML ci-dessous. `SourceConf`: `nom`, `type` (`"obsidian" | "code"`), `emplacement` (`"s3"`, un chemin de dossier, ou une URL git), `branche: str = "main"`, `exclure: list[str] = []`, `repos_minutes: int = 0`, `visibilite: Literal["public-maison", "prive"] = "public-maison"`.
  - `PUBLIC: frozenset[str] = frozenset({"public-maison"})` et `TOUT = frozenset({"public-maison", "prive"})` dans `rag/config.py`.
  - `charger_config(chemin: Path) -> Config` ; `empreinte_indexation(c: IndexationConf) -> str` (sha256 du JSON canonique, clés triées).
  - `rag.db.connexion(url: str) -> psycopg.Connection` (`row_factory=dict_row`) ; `rag.db.migrer(conn) -> list[str]` (noms des fichiers appliqués, table `schema_migrations(nom text primary key)`, fichiers `rag/migrations/*.sql` par ordre de nom, un fichier = une transaction).
  - Fixtures pytest : `bd` (connexion à `RAG_TEST_DATABASE_URL`, défaut `postgresql://rag:rag@localhost:5434/rag_test` ; la base est créée si absente, migrée une fois, tables vidées par `TRUNCATE sources, index_versions CASCADE` avant chaque test) ; `faux_modeles` (objet avec la même interface que `Modeles` de la tâche 6 : embeddings déterministes de 1024 dims, sac de mots haché dans les dimensions puis normalisé, pour que deux textes qui partagent des mots soient proches ; attribut `appels: int`).

`config/rag.yaml` (valeurs du spec ; `config/dev.yaml` est identique sauf `sources`, réduit à `{nom: coffre-maison, type: obsidian, emplacement: ~/Obsidian/Maison, exclure: [".obsidian/**", ".trash/**", "_versions/**"]}`) :

```yaml
sources:
  - {nom: coffre-maison, type: obsidian, emplacement: s3, repos_minutes: 10, visibilite: public-maison, exclure: [".obsidian/**", ".trash/**", "_versions/**"]}
indexation:
  strategie: titres          # titres | recursif | parent_enfant
  cible_tokens: 400
  plafond_tokens: 700
  fusion_sous_tokens: 80
  recursif_tokens: 400
  recouvrement: 0.12
  enfant_tokens: 200
  modele_embedding: "qwen3-embedding:0.6b"
  dimensions: 1024
  taille_max_octets: 300000
recherche:
  top_vecteur: 40
  top_texte: 40
  rrf_k: 60
  poids: {vecteur: 1.0, texte: 1.0, flou: 0.5}
  flou: true
  seuil_flou: 0.4
  instruction_requete: "Given a web search query, retrieve relevant passages that answer the query"
  rerank: {actif: false, modele: "BAAI/bge-reranker-v2-m3", candidats: 30, garder: 8}
  graphe: {expansion: false, depuis: 3, attenuation: 0.5, plafond: 5}
reponse:
  modele: "gemma4:26b-a4b-it-qat"
  temperature: 0.1
  passages: 8
  seuil_refus_rerank: null
  seuil_refus_cosinus: null
banc:
  modele_juge: "qwen3.8:27b-mlx"
  modele_synthetique: "gemma4:26b-a4b-it-qat"
```

`rag/migrations/001_socle.sql` (le schéma est une décision, il est donné en entier) :

```sql
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE TEXT SEARCH CONFIGURATION fr (COPY = french);
ALTER TEXT SEARCH CONFIGURATION fr
  ALTER MAPPING FOR hword, hword_part, word WITH unaccent, french_stem;

CREATE TABLE sources (
  id serial PRIMARY KEY,
  nom text NOT NULL UNIQUE,
  type text NOT NULL CHECK (type IN ('obsidian', 'code')),
  emplacement text NOT NULL,
  visibilite text NOT NULL DEFAULT 'public-maison' CHECK (visibilite IN ('public-maison', 'prive')),
  branche text,
  revision text,              -- dernier commit indexé (code) ; horodatage du listage (coffre)
  indexe_le timestamptz,
  demande_le timestamptz      -- « réindexer maintenant » (plan 3)
);
CREATE TABLE index_versions (
  id serial PRIMARY KEY,
  empreinte text NOT NULL UNIQUE,
  config jsonb NOT NULL,
  cree_le timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE documents (
  id bigserial PRIMARY KEY,
  source_id int NOT NULL REFERENCES sources ON DELETE CASCADE,
  chemin text NOT NULL,
  titre text NOT NULL,
  titre_norm text NOT NULL,   -- titre + alias, minuscules sans accents (flou pg_trgm)
  alias text[] NOT NULL DEFAULT '{}',
  tags text[] NOT NULL DEFAULT '{}',
  type text,
  origine text,               -- champ `source` du frontmatter (URL, livre, claude…)
  date_doc date,
  frontmatter jsonb NOT NULL DEFAULT '{}',
  liens_sortants text[] NOT NULL DEFAULT '{}',   -- cibles des [[liens]] (graphe, plan 2)
  signature text NOT NULL,    -- etag S3, mtime+taille, ou sha de blob git
  sha256 text NOT NULL,
  modifie_le timestamptz,
  UNIQUE (source_id, chemin)
);
CREATE TABLE morceaux (
  id bigserial PRIMARY KEY,
  document_id bigint NOT NULL REFERENCES documents ON DELETE CASCADE,
  version_id int NOT NULL REFERENCES index_versions ON DELETE CASCADE,
  ordre int NOT NULL,
  fil text NOT NULL,          -- fil d'Ariane « Note > H1 > H2 » ou « dépôt > chemin > symbole »
  texte text NOT NULL,
  parent_id bigint REFERENCES morceaux ON DELETE CASCADE,
  symbole text,
  ligne int,
  embedding halfvec(1024),    -- NULL pour un parent (stratégie parent_enfant) : non cherché
  tsv tsvector,
  UNIQUE (document_id, version_id, ordre)
);
CREATE INDEX ON morceaux USING hnsw (embedding halfvec_cosine_ops);
CREATE INDEX ON morceaux USING gin (tsv);
CREATE INDEX ON morceaux USING gin (lower(symbole) gin_trgm_ops);
CREATE INDEX ON documents USING gin (titre_norm gin_trgm_ops);
CREATE INDEX ON documents USING gin (tags);
CREATE INDEX ON documents (type);
```

Écart assumé par rapport au spec : `symbole` est sur `morceaux` (un document de code = un fichier, ses morceaux = ses symboles), pas sur `documents`.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
# tests/test_db.py
def test_migrer_est_idempotent(bd):
    assert migrer(bd) == []          # la fixture a déjà migré

def test_recherche_plein_texte_sans_accents(bd):
    r = bd.execute("SELECT to_tsvector('fr','Pâté chinois') @@ plainto_tsquery('fr','pate') AS ok").fetchone()
    assert r["ok"] is True

def test_embedding_est_un_halfvec_1024(bd):
    r = bd.execute("SELECT format_type(atttypid, atttypmod) t FROM pg_attribute "
                   "WHERE attrelid='morceaux'::regclass AND attname='embedding'").fetchone()
    assert r["t"] == "halfvec(1024)"

# tests/test_config.py
def test_la_config_de_prod_se_charge():
    c = charger_config(Path("config/rag.yaml"))
    assert c.indexation.strategie == "titres" and c.indexation.dimensions == 1024
    assert c.recherche.rrf_k == 60 and c.recherche.rerank.actif is False
    assert c.sources[0].nom == "coffre-maison" and c.sources[0].repos_minutes == 10
    assert c.sources[0].visibilite == "public-maison" and "_versions/**" in c.sources[0].exclure

def test_empreinte_change_avec_la_strategie():
    c = charger_config(Path("config/rag.yaml")).indexation
    assert empreinte_indexation(c) == empreinte_indexation(c.model_copy())
    assert empreinte_indexation(c) != empreinte_indexation(c.model_copy(update={"strategie": "recursif"}))

def test_une_cle_inconnue_est_refusee(tmp_path):   # une faute de frappe ne doit pas devenir un réglage ignoré
    ...  # YAML valide + clé `recherche.rrf_kk: 60` -> pydantic.ValidationError (modèles en extra="forbid")
```

- [ ] **Step 2: Lancer** `docker compose -f compose.dev.yml up -d && uv run pytest tests/test_db.py tests/test_config.py -v` — attendu : ÉCHEC (modules absents).
- [ ] **Step 3: Implémenter** `pyproject.toml` (dépendances : `fastapi`, `uvicorn[standard]`, `pydantic-settings`, `psycopg[binary]>=3.2`, `httpx`, `pyyaml`, `boto3` ; groupe `dev` : `pytest`, `ruff` ; `[project.scripts] rag = "rag.cli:main"` ; `[tool.pytest.ini_options] markers = ["vivant: appelle le vrai LiteLLM"]` ; ruff `line-length = 100`), `compose.dev.yml` (un service `postgres`, image `pgvector/pgvector:0.8.6-pg18-bookworm`, `POSTGRES_USER/PASSWORD/DB = rag`, port `5434:5432`, volume nommé), `.gitignore` (`.venv/`, `donnees/`, `banc/resultats/`, `.env`, `__pycache__/`), puis `rag/config.py`, `rag/db.py`, la migration et `tests/conftest.py` selon les interfaces ci-dessus. `README.md` : 15 lignes, comment lancer la base de dev et les tests.
- [ ] **Step 4: Relancer** la même commande — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git add -A && git commit -m "feat: ossature, config versionnée et schéma de base"`

---

### Task 2: Preuve graphify en conteneur (spike, décide le plan A ou B du plan 2)

Aucun code produit. Le livrable est un compte rendu et un fichier d'exemple que le plan 2 utilise.

**Files:**
- Create: `spike/graphify/Dockerfile`, `docs/superpowers/spikes/2026-11-graphify.md`, `tests/fixtures/graphify/graph.json`

**Interfaces:**
- Produces: `tests/fixtures/graphify/graph.json` (sortie réelle de graphify sur ce dépôt, réduite à ≤ 200 nœuds) ; dans le compte rendu, une ligne `Décision : A` ou `Décision : B`, la forme exacte du JSON (clés des nœuds et des liens, valeurs possibles de la confiance), et la réponse à « l'extraction sémantique peut-elle viser LiteLLM ? ».

- [ ] **Step 1: Image.** `spike/graphify/Dockerfile` : `FROM python:3.13-slim`, `RUN apt-get update && apt-get install -y --no-install-recommends git && pip install --no-cache-dir graphifyy`, `ENTRYPOINT ["graphify"]`. Construire : `docker build -t rag-spike-graphify spike/graphify`. Noter la taille de l'image et la version installée (`pip show graphifyy`).
- [ ] **Step 2: Extraction structurelle sans réseau ni LLM.** `docker run --rm --network none -v "$PWD":/src:ro -v /tmp/gout:/out -w /out rag-spike-graphify update /src`. Attendu : code de sortie 0 et un `graph.json` écrit. Noter où il est écrit (un volume `:ro` peut l'empêcher d'écrire `graphify-out/` dans la source : si oui, noter l'option ou la variable qui déplace la sortie, sinon monter une copie). Refaire sur un dépôt Rust et un dépôt TypeScript (`~/Code/Moria`, `~/Code/AI`, montés en lecture seule) : noter la durée et le nombre de nœuds.
- [ ] **Step 3: Forme du JSON.** `python3 -c "import json;g=json.load(open('graph.json'));print(g.keys());print(g['nodes'][0]);print((g.get('links') or g.get('edges'))[0])"`. Consigner : nom de la clé des liens, clé d'identité des nœuds, champs fichier/ligne/communauté, valeurs de confiance (`EXTRACTED`/`INFERRED`/`AMBIGUOUS`). Copier un extrait de ≤ 200 nœuds et leurs liens dans `tests/fixtures/graphify/graph.json`.
- [ ] **Step 4: Incrémental.** Modifier un fichier dans une copie, relancer `update` : noter si seuls les fichiers changés sont retraités et si `--force` est nécessaire après une suppression.
- [ ] **Step 5: Sémantique par modèle local.** `docker run --rm --entrypoint python rag-spike-graphify -c "import graphify.llm as l, inspect; print(inspect.signature(l.extract_corpus_parallel)); print([n for n in dir(l) if 'backend' in n.lower()])"`. Chercher un backend `ollama` ou compatible OpenAI avec URL de base réglable. Si oui, essayer sur 3 notes du coffre copiées dans `/tmp`, contre LiteLLM (`https://litellm.infra.famillelallier.net/v1`, clé `rag`, modèle `gemma4:26b-a4b-it-qat`). Vérifier dans le code du backend qu'aucune URL infonuagique n'est appelée par défaut (Gemini est le défaut de graphify quand `GEMINI_API_KEY` existe : cette variable ne doit jamais être posée).
- [ ] **Step 6: Décider et écrire le compte rendu.** `A` si les steps 2 à 4 passent (graphify sert d'extracteur structurel). `B` sinon (tree-sitter + networkx, même schéma). Pour la sémantique : noter `sémantique : graphify` si le step 5 passe, sinon `sémantique : maison` (le plan 2, tâche 7, a les deux branches).
- [ ] **Step 7: Commit** `git add spike docs/superpowers/spikes tests/fixtures/graphify && git commit -m "docs: preuve graphify en conteneur et décision A/B"`

---

### Task 3: Lecteurs de source et exclusions

**Files:**
- Create: `rag/sources.py`, `rag/exclusions.py`
- Test: `tests/test_sources.py`, `tests/test_exclusions.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) Entree: chemin: str; signature: str; modifie_le: datetime; taille: int` (`chemin` relatif à la racine de la source, NFC, séparateur `/`).
  - `class LecteurDossier(racine: Path)` et `class LecteurS3(client, bucket: str, prefixe: str = "")`, même interface : `lister() -> list[Entree]`, `lire(chemin: str) -> bytes`, `revision() -> str`.
    - `LecteurDossier` : signature `f"{st_mtime_ns}-{st_size}"`, ignore `.git/`, `revision()` = horodatage ISO du listage.
    - `LecteurS3` : `list_objects_v2` paginé (suivre `NextContinuationToken` tant que `IsTruncated`), préfixe retiré des chemins, signature = `ETag` sans guillemets, `modifie_le` = `LastModified`. **Lève `ListagePartielError`** si une page dit `IsTruncated` sans jeton de suite. `lire` retrouve la clé S3 d'origine (qui peut être NFD) par une table chemin NFC → clé construite au listage.
  - `client_s3(reglages: Reglages)` : `boto3.client("s3", endpoint_url=…, aws_access_key_id=…, aws_secret_access_key=…, region_name="us-east-1", config=Config(s3={"addressing_style": "path"}))` (même forme que `agents/agent-musique/server.py` de l'Infra).
  - `rag.exclusions.motif_exclusion(chemin: str, debut: bytes, exclure: list[str]) -> str | None` : la raison de l'exclusion, ou `None`. Règles, dans l'ordre : glob de `exclure` (`fnmatch` sur le chemin entier) → `"config"` ; nom qui ressemble à un secret (`.env`, `.env.*`, `*.pem`, `*.key`, `*.p12`, `id_rsa*`, `id_ed25519*`, `seal.key`, `.runner.env`, `.openbao.env`, `.portainer.env`) → `"secret"` ; contenu qui ressemble à un secret (`-----BEGIN [A-Z ]*PRIVATE KEY-----`, `sk-[A-Za-z0-9_-]{20,}`, `ghp_[A-Za-z0-9]{36}`, `github_pat_[A-Za-z0-9_]{20,}`, `AKIA[0-9A-Z]{16}`) → `"secret"` ; fichier généré ou verrou (`uv.lock`, `Cargo.lock`, `package-lock.json`, `pnpm-lock.yaml`, `*.min.js`, `*.map`, dossiers `node_modules/`, `dist/`, `target/`, `graphify-out/`, `.venv/`) → `"genere"` ; binaire (octet nul dans `debut`, ou `debut` non décodable en UTF-8) → `"binaire"`.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
# tests/test_sources.py
def test_dossier_liste_en_nfc_et_lit(tmp_path):
    nfd = unicodedata.normalize("NFD", "Modèles/é.md")
    (tmp_path / nfd).parent.mkdir(); (tmp_path / nfd).write_text("x")
    e = LecteurDossier(tmp_path).lister()
    assert [x.chemin for x in e] == [unicodedata.normalize("NFC", "Modèles/é.md")]
    assert LecteurDossier(tmp_path).lire(e[0].chemin) == b"x"

def test_dossier_ignore_git(tmp_path): ...            # .git/HEAD absent du listage
def test_s3_pagine_et_retire_le_prefixe():            # faux client : 2 pages, Prefix "coffre/"
    assert [e.chemin for e in LecteurS3(faux, "obsidian", "coffre/").lister()] == ["a.md", "b/c.md"]
    assert faux.appels_list == 2
def test_s3_signature_est_l_etag_sans_guillemets(): ...   # ETag '"abc"' -> "abc"
def test_s3_listage_tronque_sans_jeton_leve():
    with pytest.raises(ListagePartielError): LecteurS3(faux_tronque, "obsidian").lister()
def test_s3_lit_une_cle_nfd_par_son_chemin_nfc(): ...
def test_aucune_ecriture_s3_dans_le_depot():
    src = "".join(p.read_text() for p in Path("rag").rglob("*.py"))
    assert not re.search(r"put_object|delete_object|copy_object|upload_file", src)

# tests/test_exclusions.py
@pytest.mark.parametrize("chemin,debut,raison", [
    (".env", b"A=1", "secret"), ("deploy/.env.example", b"A=change-me", "secret"),
    ("cle.pem", b"x", "secret"), ("notes/api.md", b"cle: sk-abcdefghijklmnopqrstuvwx", "secret"),
    ("uv.lock", b"x", "genere"), ("front/node_modules/a/b.js", b"x", "genere"),
    ("img.png", b"\x89PNG\x00", "binaire"), (".obsidian/app.json", b"{}", "config"),
    ("_versions/2026-11-20/Poulet.md", b"# Poulet", "config"),
    ("02 - Domaines/Cuisine/Poulet.md", b"# Poulet", None), ("rag/api.py", b"import os", None),
])
def test_motif_exclusion(chemin, debut, raison):
    assert motif_exclusion(chemin, debut, [".obsidian/**", "_versions/**"]) == raison
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_sources.py tests/test_exclusions.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter** selon les interfaces. Le faux client S3 des tests vit dans `tests/conftest.py` (`FauxS3(pages: list[dict], objets: dict[str, bytes])`).
- [ ] **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "feat: lecteurs de source (dossier, S3 lecture seule) et exclusions"`

---

### Task 4: Analyse d'une note Obsidian

**Files:**
- Create: `rag/notes.py`
- Test: `tests/test_notes.py`

**Interfaces:**
- Produces: `@dataclass Note: titre: str; alias: list[str]; tags: list[str]; type: str | None; origine: str | None; date: datetime.date | None; frontmatter: dict; corps: str; liens: list[str]` et `analyser_note(chemin: str, texte: str) -> Note`.
  - `titre` = nom du fichier sans `.md`. `alias` = `aliases` ou `alias` du frontmatter (chaîne ou liste). `tags` = ceux du frontmatter (chaîne ou liste, `#` retiré) **plus** les `#tag` du corps, sans doublon. `origine` = champ `source`. `frontmatter` = dictionnaire complet, valeurs rendues sérialisables en JSON (dates en ISO).
  - `corps` = texte sans le frontmatter, sans les commentaires `%%…%%` (sur une ou plusieurs lignes), octets nuls retirés. Le frontmatter n'est jamais dans `corps`.
  - `liens` = cibles des `[[cible]]`, `[[cible|texte]]`, `[[cible#section]]` (la cible seule, sans `|…` ni `#…`), dans l'ordre, sans doublon ; les liens dans un bloc de code ne comptent pas.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
RECETTE = """---
type: recette
tags: [recette, cuisine, poulet]
source: claude
date: 2026-11-08
aliases: Poulet au cidre
---

# Poulet cidre et Boursin

Souper pour [[Lena Hope Capp]]. Retour sur [[Cuisine|la cuisine]]. %%note privée%%
#souper
"""

def test_frontmatter_devient_metadonnees():
    n = analyser_note("02 - Domaines/Cuisine/Recettes/Poulet cidre et Boursin.md", RECETTE)
    assert (n.titre, n.type, n.origine) == ("Poulet cidre et Boursin", "recette", "claude")
    assert n.tags == ["recette", "cuisine", "poulet", "souper"]
    assert n.alias == ["Poulet au cidre"] and n.date == date(2026, 11, 8)
    assert n.frontmatter["date"] == "2026-11-08"
    assert "type: recette" not in n.corps and "note privée" not in n.corps
    assert n.liens == ["Lena Hope Capp", "Cuisine"]

def test_note_sans_frontmatter():
    n = analyser_note("Accueil.md", "# Accueil\n\nBonjour")
    assert (n.titre, n.type, n.tags, n.frontmatter) == ("Accueil", None, [], {})

@pytest.mark.parametrize("fm", ["---\ntags: [a, b\n---\n", "---\n: : :\n---\n", "---\njuste du texte\n---\n", "---\nsans fin"])
def test_frontmatter_invalide_n_arrete_rien(fm):
    n = analyser_note("x.md", fm + "corps")
    assert n.titre == "x" and n.frontmatter == {} and "corps" in n.corps

def test_commentaire_sur_plusieurs_lignes_retire():
    assert "secret" not in analyser_note("x.md", "a\n%%\nsecret\n%%\nb").corps

def test_lien_dans_un_bloc_de_code_ignore():
    assert analyser_note("x.md", "```\n[[pas un lien]]\n```\n[[vrai]]").liens == ["vrai"]
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_notes.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter** `analyser_note` (frontmatter par `yaml.safe_load`, toute erreur ou tout résultat qui n'est pas un dictionnaire donne `{}` et le bloc reste dans le corps).
- [ ] **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "feat: analyse des notes (frontmatter, commentaires, liens)"`

---

### Task 5: Découpage des notes (3 stratégies)

**Files:**
- Create: `rag/decoupage.py`
- Test: `tests/test_decoupage.py`

**Interfaces:**
- Produces:
  - `@dataclass Morceau: ordre: int; fil: str; texte: str; parent: int | None = None; cherchable: bool = True; symbole: str | None = None; ligne: int | None = None` (`parent` = `ordre` du morceau parent dans la même liste).
  - `compter_tokens(texte: str) -> int` = `ceil(len(texte) / 4)`. Marquer `# ponytail: approximation 4 caractères par token ; brancher le tokenizer de Qwen3 si le banc montre des morceaux tronqués`.
  - `decouper_recursif(fil: str, texte: str, cible: int, recouvrement: float) -> list[Morceau]` : coupe sur `\n\n`, puis `\n`, puis `. `, puis par mots ; un bloc de code clôturé (```` ``` ````) n'est jamais coupé, même s'il dépasse la cible.
  - `decouper_note(titre: str, corps: str, conf: IndexationConf) -> list[Morceau]` selon `conf.strategie` :
    - `titres` : une section par titre Markdown (hors blocs de code) ; `fil` = `"Titre de la note > H1 > H2"` ; sections de moins de `fusion_sous_tokens` fusionnées avec la suivante de même niveau parent ; section de plus de `plafond_tokens` redécoupée par `decouper_recursif(fil, …, cible_tokens, 0)`.
    - `recursif` : `decouper_recursif(titre, corps, recursif_tokens, recouvrement)`.
    - `parent_enfant` : les sections de `titres` deviennent des parents (`cherchable=False`), chacun suivi de ses enfants de `enfant_tokens` (`parent=<ordre du parent>`).
  - Le texte d'un morceau ne contient pas le fil ; l'indexeur préfixe `fil + "\n\n"` avant l'embedding et le met en poids A du tsvector.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
CONF = charger_config(Path("config/rag.yaml")).indexation
LONG = "mot " * 400                                   # ~500 tokens

def test_titres_fil_d_ariane():
    m = decouper_note("Recette", f"# A\n\n{LONG}\n\n## B\n\n{LONG}", CONF)
    assert [x.fil for x in m] == ["Recette > A", "Recette > A > B"]

def test_titres_fusionne_les_petites_sections():
    m = decouper_note("N", f"# A\n\ncourt\n\n# B\n\n{LONG}", CONF)
    assert len(m) == 1 and "court" in m[0].texte and LONG.strip()[:20] in m[0].texte

def test_titres_redecoupe_au_dessus_du_plafond():
    m = decouper_note("N", "# A\n\n" + "\n\n".join(["mot " * 150] * 8), CONF)   # ~1500 tokens
    assert len(m) >= 3 and all(compter_tokens(x.texte) <= CONF.plafond_tokens for x in m)
    assert {x.fil for x in m} == {"N > A"}

def test_bloc_de_code_jamais_coupe():
    code = "```python\n" + "x = 1\n" * 900 + "```"
    m = decouper_note("N", f"# A\n\n{code}", CONF)
    assert any(code in x.texte for x in m)

def test_diese_dans_un_bloc_de_code_n_est_pas_un_titre():
    assert len(decouper_note("N", f"# A\n\n```sh\n# commentaire\n```\n\n{LONG}", CONF)) == 1

def test_recursif_recouvre():
    m = decouper_note("N", "\n\n".join(f"p{i} " + "mot " * 100 for i in range(12)), CONF.model_copy(update={"strategie": "recursif"}))
    assert len(m) >= 3 and all(x.fil == "N" for x in m)
    assert " ".join(m[0].texte.split()[-5:]) in m[1].texte        # la fin du 1er est reprise au début du 2e

def test_parent_enfant():
    m = decouper_note("N", f"# A\n\n{LONG}\n\n{LONG}", CONF.model_copy(update={"strategie": "parent_enfant"}))
    parents = [x for x in m if not x.cherchable]
    assert len(parents) == 1 and all(x.parent == parents[0].ordre for x in m if x.cherchable)
    assert all(compter_tokens(x.texte) <= 260 for x in m if x.cherchable)

def test_note_vide_ou_sans_titre():
    assert decouper_note("N", "", CONF) == []
    assert [x.fil for x in decouper_note("N", "juste du texte", CONF)] == ["N"]

def test_ordres_uniques_et_croissants(): ...          # pour chaque stratégie : [x.ordre] == list(range(len(m)))
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_decoupage.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter** `rag/decoupage.py`.
- [ ] **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "feat: découpage des notes (titres, récursif, parent-enfant)"`

---

### Task 6: Client LiteLLM et instruction de requête Qwen3

Prérequis du test vivant : tâche I1 du plan Infra faite (clé `rag`), `RAG_LITELLM_URL=https://litellm.infra.famillelallier.net`, `RAG_LITELLM_CLE=sk-…`, `RAG_LITELLM_CA=~/Code/Infra/certs/infra-ca.crt` dans un `.env` local non commité.

**Files:**
- Create: `rag/modeles.py`
- Test: `tests/test_modeles.py`

**Interfaces:**
- Produces: `class Modeles(url: str, cle: str, ca: Path | None = None, client: httpx.Client | None = None)` :
  - `embed_documents(textes: list[str], modele: str) -> list[list[float]]` : `POST {url}/v1/embeddings`, lots de 32, réponses remises dans l'ordre par `index`, lève `ErreurModele` si le nombre ou la dimension (1024) ne correspond pas. Texte envoyé tel quel (pas d'instruction).
  - `embed_requete(question: str, modele: str, instruction: str) -> list[float]` : envoie `f"Instruct: {instruction}\nQuery: {question}"`.
  - `generer(modele: str, messages: list[dict], temperature: float, json_mode: bool = False) -> str` : `POST {url}/v1/chat/completions`, non diffusé ; `json_mode` pose `response_format={"type": "json_object"}`.
  - Délai 300 s ; toute erreur HTTP ou réseau devient `ErreurModele` (message sans la clé).
  - `modeles_depuis(reglages: Reglages) -> Modeles`.

- [ ] **Step 1: Écrire les tests qui échouent** (transport simulé par `httpx.MockTransport`)

```python
def test_documents_sans_instruction_requete_avec():
    m.embed_documents(["poulet"], "qwen3-embedding:0.6b")
    assert vu[-1]["input"] == ["poulet"]
    m.embed_requete("poulet ?", "qwen3-embedding:0.6b", "Trouve les passages")
    assert vu[-1]["input"] == ["Instruct: Trouve les passages\nQuery: poulet ?"]

def test_lots_de_32_et_ordre_conserve(): ...        # 70 textes -> 3 appels, résultat dans l'ordre d'entrée même si `data` revient mélangé
def test_mauvaise_dimension_leve(): ...             # le serveur rend 768 dims -> ErreurModele
def test_erreur_http_sans_fuite_de_cle():
    with pytest.raises(ErreurModele) as e: m_500.embed_documents(["x"], "q")
    assert "sk-test" not in str(e.value)

@pytest.mark.vivant
def test_qwen3_bout_en_bout(modeles_vivants):       # la vérification « dès le premier jour » du spec
    inst = charger_config(Path("config/rag.yaml")).recherche.instruction_requete
    doc_p, doc_g = modeles_vivants.embed_documents(
        ["Poulet cidre et Boursin : poitrines de poulet, carottes, cidre.", "Tableaux de bord Grafana du homelab."], M)
    assert len(doc_p) == 1024
    q = modeles_vivants.embed_requete("Quelle recette contient du poulet ?", M, inst)
    brut = modeles_vivants.embed_documents(["Quelle recette contient du poulet ?"], M)[0]
    assert cosinus(q, brut) < 0.999                 # l'instruction atteint bien le modèle
    assert cosinus(q, doc_p) > cosinus(q, doc_g)
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_modeles.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter** `rag/modeles.py`.
- [ ] **Step 4: Relancer** — attendu : SUCCÈS ; puis `uv run pytest -m vivant tests/test_modeles.py -v` avec le `.env` chargé — attendu : SUCCÈS. Si `cosinus(q, brut) >= 0.999`, l'instruction est perdue en route (LiteLLM ou Ollama) : arrêter et le signaler, c'est un risque nommé du spec.
- [ ] **Step 5: Commit** `git commit -am "feat: client LiteLLM, instruction de requête Qwen3 vérifiée"`

---

### Task 7: Indexeur incrémental

**Files:**
- Create: `rag/indexeur.py`
- Test: `tests/test_indexeur.py`

**Interfaces:**
- Consumes: lecteurs (tâche 3), `motif_exclusion`, `analyser_note`, `decouper_note`, `Modeles.embed_documents`, `empreinte_indexation`.
- Produces:
  - `@dataclass Bilan: nouveaux: int = 0; modifies: int = 0; supprimes: int = 0; inchanges: int = 0; reportes: int = 0; exclus: int = 0; morceaux: int = 0`.
  - `version_courante(conn, conf: IndexationConf) -> int` : crée la ligne `index_versions` si l'empreinte est neuve, rend son `id`.
  - `indexer_source(conn, source: SourceConf, lecteur, conf: IndexationConf, modeles, *, forcer: bool = False, sans_repos: bool = False, maintenant: datetime | None = None) -> Bilan`.
  - `class CoffrePartielError(Exception)`.

Algorithme (c'est lui qui fait l'incrémental, il est fixé ici) :

1. Créer ou retrouver la ligne `sources` par `nom`, et y recopier `emplacement`, `branche` et `visibilite` de la config (la config fait foi à chaque passe) ; `v = version_courante(conn, conf)`.
2. `entrees = lecteur.lister()` ; retirer celles dont `motif_exclusion(chemin, b"", source.exclure)` répond (tri par nom, sans lire).
3. Garde : si la source a déjà des documents et que plus de la moitié sont absents du listage, ou si le listage est vide, lever `CoffrePartielError`, sauf `forcer=True`.
4. Pour chaque entrée, dans une transaction par document (`conn.commit()` après chacun) :
   - même `signature` qu'en base **et** des morceaux existent pour `v` → `inchanges` ;
   - sinon, si `modifie_le > maintenant - repos_minutes` et pas `sans_repos` → `reportes` (on n'y touche pas) ;
   - sinon lire le contenu ; `motif_exclusion(chemin, contenu[:8192], …)` → s'il répond, supprimer le document s'il existait, `exclus` ;
   - calculer le sha256 ; s'il est égal à celui en base et que des morceaux existent pour `v` → mettre à jour `signature`, `inchanges` ;
   - sinon : upsert du document (pour `obsidian` : champs de `analyser_note`, `liens_sortants = note.liens` ; `titre_norm` = titre et alias en minuscules sans accents, joints par un espace), **si le sha a changé, supprimer ses morceaux de toutes les versions**, découper, calculer les embeddings de `fil + "\n\n" + texte` pour les morceaux `cherchable`, insérer les morceaux de `v` avec `tsv = setweight(to_tsvector('fr', fil), 'A') || setweight(to_tsvector('fr', <tags joints par un espace>), 'B') || setweight(to_tsvector('fr', texte), 'C')` ; un parent a `embedding` et `tsv` à `NULL`. Compter `nouveaux` ou `modifies`.
   - une exception pendant un document : `conn.rollback()`, puis relancer l'exception (la passe s'arrête, ce qui est fait reste).
5. Supprimer les documents de la source absents du listage (`supprimes`) ; les morceaux partent en cascade. Un renommage est une suppression plus une création.
6. `UPDATE sources SET revision = lecteur.revision(), indexe_le = now()`.

Un fichier de plus de `taille_max_octets` : un seul morceau, `fil` normal, texte = les 2000 premiers caractères.

- [ ] **Step 1: Écrire les tests qui échouent** (coffre de test = `tmp_path` avec 4 notes, lu par `LecteurDossier` ; `faux_modeles`)

```python
def test_premiere_passe_puis_rien_a_faire(bd, coffre, faux_modeles):
    b1 = indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    assert (b1.nouveaux, b1.modifies, b1.supprimes) == (4, 0, 0) and b1.morceaux >= 4
    n = faux_modeles.appels
    b2 = indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    assert b2.inchanges == 4 and faux_modeles.appels == n      # aucun embedding recalculé

def test_note_modifiee_seule_retraitee(bd, coffre, faux_modeles): ...   # réécrire a.md -> modifies == 1, les morceaux des 3 autres gardent leur id
def test_touch_sans_changement_ne_recalcule_rien(bd, coffre, faux_modeles): ...  # os.utime -> signature change, sha égal -> inchanges == 4, appels inchangés
def test_suppression_et_renommage(bd, coffre, faux_modeles):
    (coffre / "a.md").rename(coffre / "z.md"); (coffre / "b.md").unlink()
    b = indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    assert (b.nouveaux, b.supprimes) == (1, 2)
    assert chemins(bd) == {"c.md", "d.md", "z.md"}
    assert bd.execute("SELECT count(*) n FROM morceaux m JOIN documents d ON d.id=m.document_id WHERE d.chemin IN ('a.md','b.md')").fetchone()["n"] == 0

def test_coffre_partiel_refuse(bd, coffre, faux_modeles):
    for f in ("a.md", "b.md", "c.md"): (coffre / f).unlink()
    with pytest.raises(CoffrePartielError): indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    assert len(chemins(bd)) == 4
    assert indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles, forcer=True).supprimes == 3

def test_listage_vide_refuse(bd, coffre, faux_modeles): ...             # dossier vidé -> CoffrePartielError, base intacte

def test_note_recente_reportee(bd, coffre, faux_modeles):
    src = SRC.model_copy(update={"repos_minutes": 10})
    b = indexer_source(bd, src, LecteurDossier(coffre), CONF, faux_modeles, maintenant=datetime.now(UTC))
    assert b.reportes == 4 and b.nouveaux == 0
    assert indexer_source(bd, src, LecteurDossier(coffre), CONF, faux_modeles, sans_repos=True).nouveaux == 4

def test_chemin_nfd_puis_nfc_meme_document(bd, tmp_path, faux_modeles): ...   # même note listée en NFD puis NFC -> 1 document, inchanges == 1

def test_panne_du_modele_ne_laisse_pas_de_demi_document(bd, coffre):
    m = FauxModelesQuiTombe(apres=2)                   # lève ErreurModele au 3e appel
    with pytest.raises(ErreurModele): indexer_source(bd, SRC, LecteurDossier(coffre), CONF, m)
    assert bd.execute("SELECT count(*) n FROM documents d WHERE NOT EXISTS (SELECT 1 FROM morceaux WHERE document_id=d.id)").fetchone()["n"] == 0
    assert len(chemins(bd)) == 2
    assert indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles).nouveaux == 2   # reprise

def test_deux_versions_cote_a_cote(bd, coffre, faux_modeles):
    indexer_source(bd, SRC, LecteurDossier(coffre), CONF, faux_modeles)
    indexer_source(bd, SRC, LecteurDossier(coffre), CONF.model_copy(update={"strategie": "recursif"}), faux_modeles)
    assert bd.execute("SELECT count(DISTINCT version_id) n FROM morceaux").fetchone()["n"] == 2
    assert len(chemins(bd)) == 4

def test_visibilite_suit_la_config(bd, coffre, faux_modeles): ...          # réindexer avec visibilite="prive" -> sources.visibilite == 'prive'
def test_liens_sortants_stockes(bd, coffre, faux_modeles): ...          # a.md contient [[b]] -> documents.liens_sortants == ['b']
def test_secret_dans_une_note_exclu(bd, coffre, faux_modeles): ...      # note contenant « sk-… » -> exclus == 1, absente de documents
def test_tsv_poids_a_sur_le_fil(bd, coffre, faux_modeles): ...          # le titre de la note est trouvé avec un rang > au même mot dans le corps
def test_parent_sans_embedding(bd, coffre, faux_modeles): ...           # strategie parent_enfant : parents embedding IS NULL, enfants parent_id renseigné
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_indexeur.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter** `rag/indexeur.py` selon l'algorithme. Le vecteur est passé à Postgres comme littéral texte `'[…]'::halfvec` (pas de dépendance `pgvector` Python).
- [ ] **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "feat: indexeur incrémental (signature, sha256, garde coffre partiel, versions)"`

---

### Task 8: Recherche hybride, flou et filtres

**Files:**
- Create: `rag/recherche.py`
- Test: `tests/test_recherche.py`

**Interfaces:**
- Consumes: `Modeles.embed_requete`, `RechercheConf`, schéma.
- Produces:
  - `@dataclass Filtres: sources: list[str] | None = None; dossier: str | None = None; tags: list[str] | None = None; type: str | None = None; depuis: date | None = None; jusqua: date | None = None` (`sources` = noms de source, donc aussi « dépôt » ; `dossier` = préfixe de chemin ; `tags` = tous requis ; dates sur `date_doc`).
  - `@dataclass Resultat: morceau_id: int; document_id: int; cle: str; source: str; type_source: str; chemin: str; titre: str; fil: str; texte: str; symbole: str | None; ligne: int | None; origine: str | None; lien: str; score: float; cosinus: float | None; rerank: float | None = None; via: str = "hybride"` (`cle` = `"<source>:<chemin>"`).
  - `chercher(conn, question: str, conf: RechercheConf, modeles, version_id: int, modele_embedding: str, *, niveaux: frozenset[str], filtres: Filtres | None = None, limite: int = 10) -> list[Resultat]`. `niveaux` est obligatoire ; un ensemble vide rend `[]`.
  - `lien_source(type_source: str, source: str, chemin: str, ligne: int | None) -> str` : `obsidian://open?vault=Maison&file=<chemin sans .md, encodé par urllib.parse.quote>` pour `obsidian` ; `"<source>/<chemin>#L<ligne>"` pour `code` (sans `#L…` si `ligne` est `None`).
  - `normaliser(texte: str) -> str` (minuscules, sans accents ; la même fonction sert à `titre_norm` dans l'indexeur — la déplacer ici ou l'importer, une seule définition).

Requête (une seule instruction SQL, dans une transaction qui fait d'abord `SET LOCAL hnsw.iterative_scan = relaxed_order`) :

```sql
WITH q AS (SELECT %(vec)s::halfvec AS v,
                  NULLIF(replace(plainto_tsquery('fr', %(question)s)::text, '&', '|'), '')::tsquery AS ts),
base AS (SELECT m.id, m.embedding, m.tsv, m.symbole, d.titre_norm
           FROM morceaux m JOIN documents d ON d.id = m.document_id JOIN sources s ON s.id = d.source_id
          WHERE m.version_id = %(version)s AND m.embedding IS NOT NULL
            AND s.visibilite = ANY(%(niveaux)s) /* + filtres */),
vecteur AS (SELECT id, row_number() OVER (ORDER BY embedding <=> (SELECT v FROM q)) AS r
              FROM base ORDER BY embedding <=> (SELECT v FROM q) LIMIT %(top_vecteur)s),
texte AS (SELECT id, row_number() OVER (ORDER BY ts_rank_cd(tsv, (SELECT ts FROM q)) DESC) AS r
            FROM base WHERE tsv @@ (SELECT ts FROM q) LIMIT %(top_texte)s),
flou AS (SELECT id, row_number() OVER (ORDER BY sim DESC) AS r FROM (
           SELECT id, greatest(word_similarity(%(qnorm)s, titre_norm),
                               word_similarity(%(qnorm)s, lower(coalesce(symbole, '')))) AS sim FROM base) x
          WHERE sim >= %(seuil_flou)s ORDER BY sim DESC LIMIT 20),
fusion AS (SELECT id, sum(poids / (%(rrf_k)s + r)) AS score FROM (
             SELECT id, r, %(p_vecteur)s::float AS poids FROM vecteur
   UNION ALL SELECT id, r, %(p_texte)s FROM texte
   UNION ALL SELECT id, r, %(p_flou)s FROM flou) u GROUP BY id)
SELECT …, 1 - (m.embedding <=> (SELECT v FROM q)) AS cosinus, coalesce(p.texte, m.texte) AS texte
  FROM fusion f JOIN morceaux m ON m.id = f.id LEFT JOIN morceaux p ON p.id = m.parent_id …
 ORDER BY f.score DESC LIMIT %(limite)s
```

Le `OR` entre les termes (remplacement de `&` par `|`) est voulu : une question en langue naturelle ne contient jamais tous ses mots dans un passage. La branche `flou` est omise quand `conf.flou` est faux. Avec `parent_enfant`, deux enfants du même parent ne donnent qu'un résultat (le mieux classé).

- [ ] **Step 1: Écrire les tests qui échouent** (corpus : 6 notes indexées par la fixture `index`, dont une recette de poulet `type: recette, tags: [poulet]`, une note « Grafana », une note « Kubernetes » datée 2025)

```python
def test_trouve_par_le_sens_et_par_les_mots(bd, index, faux_modeles):
    r = chercher(bd, "recette de poulet au cidre", CONF.recherche, faux_modeles, index.version, M)
    assert r[0].chemin.endswith("Poulet cidre et Boursin.md") and r[0].score > r[1].score
    assert r[0].cle == "coffre-maison:" + r[0].chemin and r[0].lien.startswith("obsidian://open?vault=Maison&file=")
    assert r[0].origine == "claude" and 0 < r[0].cosinus <= 1

def test_sans_accents(bd, index, faux_modeles): ...       # « pate chinois » trouve « Pâté chinois »
def test_flou_sur_un_titre_mal_tape(bd, index, faux_modeles):
    assert chercher(bd, "grafanna", CONF.recherche, faux_modeles, index.version, M)[0].titre == "Grafana"
    sans = CONF.recherche.model_copy(update={"flou": False, "poids": Poids(vecteur=0, texte=1, flou=0)})
    assert all(x.titre != "Grafana" for x in chercher(bd, "grafanna", sans, faux_modeles, index.version, M))

@pytest.mark.parametrize("q", ["?", "c'est quoi ça", "a & b | c : d", "", "   ", "l'", "\\ ' \" ;"])
def test_question_sans_mot_utile_ne_plante_pas(bd, index, faux_modeles, q):
    assert isinstance(chercher(bd, q, CONF.recherche, faux_modeles, index.version, M), list)

def test_filtres(bd, index, faux_modeles):
    f = lambda **k: {x.chemin for x in chercher(bd, "note", CONF.recherche, faux_modeles, index.version, M, Filtres(**k), 50)}
    assert all(c.startswith("02 - Domaines/") for c in f(dossier="02 - Domaines/"))
    assert f(type="recette") == {"02 - Domaines/Cuisine/Recettes/Poulet cidre et Boursin.md"}
    assert f(tags=["poulet", "recette"]) == f(type="recette") and f(tags=["poulet", "absent"]) == set()
    assert f(depuis=date(2026, 1, 1)).isdisjoint({"03 - Ressources/Kubernetes.md"})
    assert f(sources=["inconnue"]) == set()

def test_source_privee_invisible_sans_le_niveau(bd, index, faux_modeles):
    bd.execute("UPDATE sources SET visibilite = 'prive'")
    assert chercher(bd, "recette de poulet", CONF.recherche, faux_modeles, index.version, M, niveaux=PUBLIC) == []
    assert chercher(bd, "recette de poulet", CONF.recherche, faux_modeles, index.version, M, niveaux=TOUT)
    assert chercher(bd, "recette de poulet", CONF.recherche, faux_modeles, index.version, M, niveaux=frozenset()) == []
def test_niveaux_est_obligatoire(bd, index, faux_modeles):
    with pytest.raises(TypeError): chercher(bd, "x", CONF.recherche, faux_modeles, index.version, M)

def test_poids_des_branches_reglables(bd, index, faux_modeles): ...   # poids.texte=0 : l'ordre égale celui de la distance vecteur seule
def test_version_isolee(bd, index, faux_modeles): ...                 # une 2e version indexée ne change pas les résultats de la 1re
def test_parent_enfant_rend_la_section_entiere(bd, index_pe, faux_modeles): ...  # texte du résultat == texte du parent, un seul résultat par parent
def test_lien_code():
    assert lien_source("code", "Infra", "litellm/config.yaml", 12) == "Infra/litellm/config.yaml#L12"
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_recherche.py -v` — attendu : ÉCHEC.
Dans les tests ci-dessus, tout appel à `chercher` sans `niveaux` écrit s'entend avec `niveaux=PUBLIC`.

- [ ] **Step 3: Implémenter** `rag/recherche.py`. Une question vide ou blanche rend `[]` sans appeler le modèle.
- [ ] **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "feat: recherche hybride RRF, flou pg_trgm et filtres"`

---

### Task 9: CLI et API HTTP (chercher)

**Files:**
- Create: `rag/cli.py`, `rag/api.py`
- Modify: `rag/sources.py` (`lecteur_pour`)
- Test: `tests/test_api.py`, `tests/test_cli.py`

**Interfaces:**
- Consumes: tout ce qui précède.
- Produces:
  - `rag.api.create_app(reglages: Reglages | None = None, modeles=None) -> FastAPI`. Routes : `GET /sante` (sans jeton ; `{"ok": true, "documents": n, "version": empreinte}`, 503 si la base ne répond pas) ; `POST /chercher` corps `{"question": str, "filtres": {...}, "limite": int = 10}` → `{"resultats": [Resultat…]}`. Toute route sauf `/sante` exige `Authorization: Bearer <jeton>` : une dépendance `niveaux_du_jeton(...) -> frozenset[str]` compare par `hmac.compare_digest` à `RAG_JETON` (→ `PUBLIC`) puis à `RAG_JETON_PRIVE` s'il n'est pas vide (→ `TOUT`), 401 sinon ; chaque route passe ces niveaux aux fonctions de lecture ; **si `RAG_JETON` est vide, l'application refuse de démarrer**. `limite` borné à 50, `question` à 2000 caractères (422 au-delà).
  - `lecteur_pour(source: SourceConf, reglages: Reglages)` dans `rag/sources.py` : `emplacement == "s3"` → `LecteurS3(client_s3(reglages), reglages.coffre_bucket, reglages.coffre_prefixe)` ; sinon, pour `obsidian`, `LecteurDossier(Path(emplacement).expanduser())`. (`code` → `LecteurGit`, plan 2.)
  - CLI `rag` (argparse, sous-commandes) : `rag migrer` ; `rag indexer [--source NOM] [--complet] [--forcer]` (`--complet` = `sans_repos=True` et toutes les sources ; affiche le `Bilan` de chaque source ; code de sortie 2 sur `CoffrePartielError`) ; `rag chercher "question" [--type T] [--tag T…] [--source S…] [--dossier D] [--limite N]` (une ligne par résultat : rang, score, cosinus, clé, fil ; niveaux `PUBLIC`, ou `TOUT` avec `--prive`) ; `rag servir` (uvicorn sur `0.0.0.0:8000`). Toutes lisent `Reglages()` et `charger_config(reglages.config)`.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_sante_sans_jeton(client): assert client.get("/sante").json()["ok"] is True
def test_chercher_exige_le_jeton(client):
    assert client.post("/chercher", json={"question": "poulet"}).status_code == 401
    assert client.post("/chercher", json={"question": "poulet"}, headers={"Authorization": "Bearer mauvais"}).status_code == 401
def test_chercher(client, auth):
    r = client.post("/chercher", json={"question": "poulet", "filtres": {"type": "recette"}, "limite": 3}, headers=auth).json()
    assert r["resultats"][0]["cle"].startswith("coffre-maison:") and "lien" in r["resultats"][0]
def test_le_jeton_public_ne_voit_pas_une_source_privee(client, auth, auth_prive, bd):
    bd.execute("UPDATE sources SET visibilite = 'prive'"); bd.commit()
    assert client.post("/chercher", json={"question": "poulet"}, headers=auth).json()["resultats"] == []
    assert client.post("/chercher", json={"question": "poulet"}, headers=auth_prive).json()["resultats"]
def test_jeton_prive_vide_n_ouvre_rien(...): ...     # RAG_JETON_PRIVE="" : « Bearer  » (vide) -> 401
def test_bornes(client, auth):
    assert client.post("/chercher", json={"question": "x" * 2001}, headers=auth).status_code == 422
    assert client.post("/chercher", json={"question": "x", "limite": 51}, headers=auth).status_code == 422
def test_refuse_de_demarrer_sans_jeton():
    with pytest.raises(RuntimeError): create_app(Reglages(database_url=URL, litellm_url="http://x", jeton=""))
def test_cli_indexer_puis_chercher(bd, coffre, monkeypatch, capsys): ...   # RAG_CONFIG -> yaml de test ; `rag indexer` affiche « 4 nouveaux », `rag chercher poulet` affiche la recette en ligne 1
def test_cli_coffre_partiel_sort_en_2(...): ...
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_api.py tests/test_cli.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter** `rag/api.py` et `rag/cli.py`.
- [ ] **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "feat: CLI rag et API HTTP /chercher avec jeton"`

---

### Task 10: Banc de mesure (retrieval)

**Files:**
- Create: `rag/banc/__init__.py`, `rag/banc/metriques.py`, `rag/banc/execution.py`, `banc/README.md`
- Modify: `rag/cli.py` (sous-commande `banc`)
- Test: `tests/test_banc.py`

**Interfaces:**
- Produces:
  - `rappel(attendus: set[str], obtenus: list[str], k: int) -> float` ; `mrr(attendus, obtenus) -> float` ; `ndcg(attendus, obtenus, k: int = 10) -> float` (pertinence binaire). `obtenus` = clés de document, dédoublonnées en gardant le meilleur rang.
  - Format du jeu (`banc/etalon.yaml`, liste de cas) : `id`, `question`, `nature` (`lister | synthetiser | factuel | sans_reponse`), `attendus: [cle…]` (vide pour `sans_reponse`), `mode_attendu` (`lister | repondre`, utilisé au plan 3), `statut` (`propose | valide`), `note` (libre). Un `.jsonl` avec les mêmes champs est accepté (jeu synthétique).
  - `charger_jeu(chemin: Path) -> list[Cas]` (refuse un `id` en double, une clé `attendus` sans `:`).
  - `executer(conn, cas: list[Cas], config: Config, modeles, version_id: int) -> dict` : pour chaque cas, `chercher(..., niveaux=PUBLIC, limite=10)` ; rapport `{"etiquette", "empreinte_indexation", "config": {...recherche}, "cas": [{id, obtenus, rappel5, rappel10, mrr, ndcg10, latence_ms, meilleur_cosinus, meilleur_rerank}], "resume": {rappel5, rappel10, mrr, ndcg10, latence_p50_ms, latence_p95_ms, n}, "par_nature": {...}}`. Les cas `sans_reponse` sont exclus des moyennes de retrieval ; leur `meilleur_cosinus` est gardé (calibration du refus au plan 3).
  - `comparer(rapport: dict, reference: dict) -> list[str]` : une ligne par métrique du résumé, `nom  réf → nouveau  (±delta)`, puis la liste des `id` de cas dont `rappel10` a baissé.
  - CLI : `rag banc [--jeu banc/etalon.yaml] [--etiquette NOM] [--sans-indexer] [--reference]`. Sans `--sans-indexer`, lance d'abord `indexer_source` sur chaque source (ne fait rien si rien n'a changé ; indexe tout si la config d'indexation a une empreinte neuve). Écrit `banc/resultats/<AAAAMMJJ-HHMMSS>-<etiquette>.json`, affiche le résumé et `comparer(...)` contre `banc/reference.json` s'il existe. `--reference` copie le rapport vers `banc/reference.json` et **refuse** (code 2) si un cas du jeu a `statut: propose`.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_metriques():
    assert rappel({"a", "b"}, ["x", "a", "y"], 5) == 0.5
    assert rappel({"a"}, ["x"] * 5 + ["a"], 5) == 0.0
    assert mrr({"a"}, ["x", "a"]) == 0.5 and mrr({"a"}, ["x"]) == 0.0
    assert ndcg({"a"}, ["a", "x"]) == 1.0 and 0 < ndcg({"a"}, ["x", "a"]) < 1
    assert ndcg({"a", "b"}, ["a", "b"]) == 1.0

def test_obtenus_dedoublonnes_par_document(bd, index, faux_modeles): ...   # 3 morceaux de la même note -> une seule clé dans `obtenus`
def test_executer_et_resume(bd, index, faux_modeles):
    r = executer(bd, [Cas(id="p", question="recette de poulet", nature="lister", attendus=[CLE_POULET]),
                      Cas(id="n", question="quel est le prix du bitcoin", nature="sans_reponse", attendus=[])],
                 CONFIG, faux_modeles, index.version)
    assert r["resume"]["n"] == 1 and r["resume"]["rappel5"] == 1.0
    assert r["cas"][1]["meilleur_cosinus"] is not None and "latence_p95_ms" in r["resume"]

def test_comparer_signale_la_baisse():
    lignes = comparer(rapport(rappel10=0.6, cas={"p": 0.0}), rapport(rappel10=0.8, cas={"p": 1.0}))
    assert any("rappel10" in l and "-0.20" in l for l in lignes) and any("p" in l for l in lignes[-2:])

def test_reference_refusee_si_cas_proposes(...): ...     # `rag banc --reference` avec un cas statut propose -> code 2, reference.json absent
def test_jeu_invalide(tmp_path): ...                     # id en double -> ValueError
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_banc.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter.** `banc/README.md` : le format du jeu, la règle « un paramètre à la fois », comment poser une référence.
- [ ] **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "feat: banc de mesure retrieval (rappel, MRR, nDCG, latence, comparaison)"`

---

### Task 11: Indexer le vrai coffre et proposer le jeu étalon

Tâche de données, pas de code. **Elle se termine par une attente : Nicolas corrige le jeu.** Prérequis : `.env` de la tâche 6, base de dev lancée.

**Files:**
- Create: `banc/etalon.yaml`

- [ ] **Step 1: Indexer.** `RAG_CONFIG=config/dev.yaml uv run rag migrer && RAG_CONFIG=config/dev.yaml uv run rag indexer --complet`. Attendu : autant de `nouveaux` que de `.md` du coffre hors `.obsidian/` et `.trash/` (≈ 91 le 2026-11-21, vérifier avec `find ~/Obsidian/Maison -name '*.md' -not -path '*/.obsidian/*' -not -path '*/.trash/*' | wc -l`), `exclus` expliqués un par un. Relancer : `0 nouveaux`, aucun appel d'embedding.
- [ ] **Step 2: Vérifier à la main** : `uv run rag chercher "recette de poulet"` rend `Poulet cidre et Boursin, légumes rôtis.md` en tête ; `uv run rag chercher "pate chinois"` et une recherche avec faute de frappe sur un nom propre du coffre rendent la bonne note.
- [ ] **Step 3: Rédiger `banc/etalon.yaml`.** Lire le coffre (lecture seule) et écrire **30 à 35 questions avec réponse** et **15 sans réponse**, toutes `statut: propose`. Règles :
  - obligatoire, mot pour mot : `id: recettes-poulet`, « Quelles recettes j'ai pour le poulet, avec les sources ? », `nature: lister`, `mode_attendu: lister`, `attendus` = toutes les notes `type: recette` qui ont le tag ou l'ingrédient poulet ;
  - couvrir chaque dossier (`00` à `05`, `_Claude`, racine) et chaque `type` de frontmatter présent (`ressource`, `veille`, `projet`, `journal`, `domaine`, `personne`, `plan`, `note-techno`, `recette`…) ;
  - au moins 8 `lister`, 8 `synthetiser` (plusieurs notes attendues), le reste `factuel` ;
  - formuler comme Nicolas parle (français québécois, questions courtes), jamais en recopiant une phrase de la note ; au moins 5 questions avec un nom propre, 3 avec une faute de frappe ou sans accents ;
  - `sans_reponse` : des questions plausibles pour ce coffre mais dont la réponse n'y est pas (vérifier par `grep -ril` dans le coffre), pas des questions absurdes ; 5 d'entre elles proches d'un sujet présent (le piège utile) ;
  - `attendus` : chemins exacts, vérifiés par `ls` ; `note` : pourquoi cette question.
  Les questions sur le code (dont `architecture-ai` et `dashboards-grafana`) s'ajoutent au plan 2, tâche 8.
- [ ] **Step 4: Valider le fichier** : `uv run python -c "from rag.banc.execution import charger_jeu; from pathlib import Path; c=charger_jeu(Path('banc/etalon.yaml')); print(len(c))"` puis `uv run rag banc --etiquette brouillon --sans-indexer` (mesure indicative, pas de référence).
- [ ] **Step 5: Commit** `git add banc/etalon.yaml && git commit -m "banc: jeu étalon proposé (coffre), à corriger par Nicolas"`
- [ ] **Step 6: ARRÊT.** Remettre à Nicolas : le fichier, le résumé du banc « brouillon », la liste des cas à `rappel10 = 0` (question mal posée, ou vrai trou de recherche ?). Nicolas corrige questions et `attendus` et passe chaque cas à `statut: valide`. La tâche 13 ne commence qu'après.

---

### Task 12: Jeu synthétique

**Files:**
- Create: `rag/banc/synthetique.py`, `banc/synthetique.jsonl`
- Modify: `rag/cli.py` (sous-commande `banc-synthetique`)
- Test: `tests/test_synthetique.py`

**Interfaces:**
- Consumes: `Modeles.generer`, schéma.
- Produces: `generer_jeu(conn, modeles, version_id: int, conf: BancConf, n: int = 250, graine: int = 1, sources: list[str] | None = None) -> list[Cas]`.
  - Tire des morceaux au hasard (graine fixe, `ORDER BY md5(id::text || graine)`), au plus 2 par document, texte d'au moins 80 tokens.
  - `conf.modele_synthetique` (gemma) écrit une question en français à laquelle le passage répond, sans reprendre plus de 3 mots consécutifs du passage, compréhensible sans voir le passage (`json_mode`, réponse `{"question": "…"}`).
  - Filtre : rejeter si la réponse n'est pas le JSON attendu, si la question fait moins de 20 caractères ou ne finit pas par `?`, si elle partage une suite de 4 mots avec le passage, si elle est un doublon (après `normaliser`), ou si `conf.modele_juge` (qwen3.8) répond `{"ok": false}` à « cette question est-elle claire seule, et ce passage y répond-il ? ».
  - S'arrête à `n` cas gardés ou après `3 * n` essais. Chaque cas : `id = "syn-<morceau_id>"`, `nature: factuel`, `attendus = [cle du document]`, `statut: valide`, `note = "morceau <id>"`.
  - CLI : `rag banc-synthetique [--n 250] [--graine 1] [--source S…]` écrit `banc/synthetique.jsonl` et affiche gardés / rejetés par raison.

- [ ] **Step 1: Écrire les tests qui échouent** (modèle simulé qui rend des réponses scriptées)

```python
def test_filtre_et_forme(bd, index):
    m = FauxGenerateur(["pas du json", '{"question": "court ?"}', '{"question": "Quels légumes accompagnent le poulet au cidre ?"}'], juge='{"ok": true}')
    cas = generer_jeu(bd, m, index.version, CONFIG.banc, n=1)
    assert len(cas) == 1 and cas[0].id.startswith("syn-") and cas[0].attendus[0].startswith("coffre-maison:")
def test_rejette_la_recopie(bd, index): ...          # question qui reprend 4 mots de suite du passage -> rejetée
def test_rejette_si_le_juge_dit_non(bd, index): ...
def test_s_arrete_apres_3n_essais(bd, index): ...    # générateur qui ne rend que du bruit -> liste vide, 3 appels pour n=1
def test_graine_reproductible(bd, index): ...        # mêmes morceaux tirés pour la même graine
def test_generateur_et_juge_sont_deux_modeles(bd, index):
    assert {a.modele for a in m.appels} == {"gemma4:26b-a4b-it-qat", "qwen3.8:27b-mlx"}
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_synthetique.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter.**
- [ ] **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Produire le jeu réel** : `RAG_CONFIG=config/dev.yaml uv run rag banc-synthetique --n 250`. Attendu : 200 à 300 cas. Relire 20 cas au hasard ; si plus de 4 sont mauvais, resserrer la consigne et recommencer.
- [ ] **Step 6: Commit** `git add -A && git commit -m "feat: jeu synthétique généré et filtré par modèles locaux"`

---

### Task 13: Référence de base et première mesure des découpages

Après la correction de Nicolas (tâche 11, step 6). Pas de code.

**Files:**
- Create: `banc/reference.json`, `docs/mesures.md`

- [ ] **Step 1: Référence.** `RAG_CONFIG=config/dev.yaml uv run rag banc --etiquette base --reference`. Attendu : `banc/reference.json` écrit (refus si un cas est encore `propose`).
- [ ] **Step 2: Synthétique.** `uv run rag banc --jeu banc/synthetique.jsonl --etiquette base-synthetique --sans-indexer`.
- [ ] **Step 3: Comparer les découpages, un paramètre à la fois.** Copier `config/dev.yaml` vers `/tmp/recursif.yaml` (`strategie: recursif`) et `/tmp/parent_enfant.yaml` ; pour chacun : `RAG_CONFIG=/tmp/<x>.yaml uv run rag banc --etiquette <x>` (indexe la nouvelle version à côté de l'ancienne, puis compare à la référence). Même chose sur le jeu synthétique.
- [ ] **Step 4: Consigner dans `docs/mesures.md`** : un tableau (étiquette, empreinte, rappel@5, rappel@10, MRR, nDCG@10, p95) pour les deux jeux, la stratégie retenue comme défaut et pourquoi, les cas étalon encore à `rappel10 = 0`. Si une autre stratégie que `titres` gagne nettement sur l'étalon (rappel@10 +0,05 ou plus), le proposer à Nicolas ; ne changer `config/rag.yaml` qu'avec son accord, puis reposer la référence.
- [ ] **Step 5: Commit** `git add banc/reference.json docs/mesures.md && git commit -m "banc: référence de base et comparaison des trois découpages"`

**Fin du plan 1 :** le coffre est indexé et interrogeable par CLI et HTTP, le banc tourne, une référence existe. Suite : plan 2.
