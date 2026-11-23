# Sources git gérées depuis l'interface — plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal :** les sources git vivent en base et se gèrent (ajouter, modifier, supprimer, exporter, importer) depuis `/ui/`, avec indexation immédiate et effacement complet.

**Architecture :** un nouveau module `rag/sources_gerees.py` porte les données (validation, chiffrement des jetons, CRUD, export/import, amorçage). Le travailleur et la CLI ne lisent plus `config.sources` directement mais `sources_actives(conn, config, coffret)`, qui renvoie le coffre du YAML plus les sources `geree` de la base. `LecteurGit` est généralisé à tout hôte https. L'interface ajoute des écrans sous `/ui/sources-git/…` dans le style htmx des exclusions.

**Tech Stack :** Python 3.13, FastAPI, Jinja2 + htmx, psycopg 3, PostgreSQL (pgvector), `cryptography` (Fernet), pytest sur vrai Postgres, ruff, uv.

**Spec :** `docs/superpowers/specs/2026-11-23-sources-git-interface-design.md`

## Global Constraints

- Nom de source : `^[\w.-]+$`, ni `.` ni `..`, unique, coffre compris ; jamais renommé.
- URL : `https://` seulement, avec hôte, sans identifiants (`user@`) dans l'URL ; adresses du réseau local permises. Git lancé avec `GIT_ALLOW_PROTOCOL=https` en production.
- Branche : non vide, ne commence pas par `-` ; défaut `main`.
- Utilisateur : défaut `x-access-token`.
- Jeton : chiffré (Fernet, clé `RAG_CLE_SECRETS`) ; jamais réaffiché, exporté, journalisé ni dans une URL ; sans `RAG_CLE_SECRETS`, l'enregistrement d'un jeton est refusé. Sans jeton propre, `RAG_GITHUB_JETON` sert pour l'hôte `github.com` seulement.
- Vérification `git ls-remote --heads <url> <branche>` avant d'enregistrer un ajout ou une modification de l'URL, de la branche, de l'utilisateur ou du jeton ; délai 30 s ; pas de vérification à l'import ni à l'amorçage.
- Écriture dans l'interface : dépendance `ecrivain` existante (jeton privé, `HX-Request`, origine locale).
- Le coffre (source non `geree`) n'est ni modifiable ni supprimable depuis l'interface.
- Pas de nouvelle dépendance au-delà de `cryptography` (déjà dans `uv.lock`), qui devient directe.
- Tests : `uv run pytest` avec `RAG_TEST_DATABASE_URL` ; `uv run ruff check .`.
- Textes de l'interface et messages en français.

## Review Focus

1. **Suppression ou changement d'URL pendant une passe en cours** : aucune ligne de la source supprimée ne doit revenir, et la passe suivante après un changement d'URL ne doit pas buter sur `CoffrePartielError`. Tests : tâche 3 (`test_source_supprimee_n_est_pas_recreee`, `test_sans_garde_apres_changement_d_url`).
2. **`RAG_CLE_SECRETS` absente ou changée alors que des jetons sont en base** : la source se clone sans jeton (ou échoue à sa passe) avec un avertissement qui nomme la source, sans faire planter le tour ni afficher de secret. Test : tâche 1 (`test_jeton_illisible_ignore_avec_avertissement`).
3. **Formulaire de modification envoyé avec le champ jeton vide** : le jeton enregistré est gardé, pas effacé. Tests : tâche 2 (`test_modifier_sans_jeton_garde_l_ancien`) et tâche 6.
4. **Nom réservé ou déjà pris** (nom du coffre YAML, source existante, `..`, nom avec `/`) : refusé avec un message, rien n'est écrit. Test : tâche 2 (`test_nom_refuse`).
5. **Import d'un YAML qui contient un jeton, une URL `file://` ou une clé inconnue** : tout le fichier est refusé, rien n'est écrit. Test : tâche 4 (`test_import_refuse_tout_ou_rien`).

---

### Task 1 : Migration, réglages, chiffrement et `sources_actives`

**Files :**
- Create : `rag/migrations/006_sources_gerees.sql`, `rag/sources_gerees.py`, `tests/test_sources_gerees.py`
- Modify : `rag/config.py` (`Reglages`), `pyproject.toml` (`cryptography` en dépendance directe), `tests/conftest.py` (`bd` vide aussi `instance`)

**Interfaces :**
- Produces :
  - Migration : `sources` + `exclure text[] NOT NULL DEFAULT '{}'`, `jeton_chiffre bytea`, `utilisateur text NOT NULL DEFAULT 'x-access-token'`, `geree boolean NOT NULL DEFAULT false`, `sans_garde boolean NOT NULL DEFAULT false`, `cree_le timestamptz NOT NULL DEFAULT now()` ; table `instance (cle text PRIMARY KEY, valeur text NOT NULL)`.
  - `Reglages.cle_secrets: str = ""`, `Reglages.sources_amorce: Path | None = None`.
  - `class Coffret: __init__(self, cle: str)`, `chiffrer(self, jeton: str) -> bytes`, `dechiffrer(self, brut: bytes) -> str` (lève `JetonIllisible`).
  - `coffret_depuis(reglages) -> Coffret | None` (`None` si `cle_secrets` vide ; `ErreurConfig("RAG_CLE_SECRETS invalide")` si la clé n'est pas une clé Fernet).
  - `@dataclass(frozen=True) class SourceActive: conf: SourceConf; jeton: str | None = None; utilisateur: str = "x-access-token"`.
  - `sources_actives(conn, config: Config, coffret: Coffret | None) -> list[SourceActive]` : les sources `obsidian` du YAML d'abord, puis les lignes `geree` triées par nom, converties en `SourceConf(type="code", …)` avec `exclure` de la base. Les sources `code` du YAML sont ignorées.

- [ ] **Step 1 : tests qui échouent** dans `tests/test_sources_gerees.py`

```python
def test_coffret_aller_retour():
    c = Coffret(Fernet.generate_key().decode())
    assert c.dechiffrer(c.chiffrer("ghp_x")) == "ghp_x" and b"ghp_x" not in c.chiffrer("ghp_x")

def test_coffret_depuis_cle_invalide():
    with pytest.raises(ErreurConfig, match="RAG_CLE_SECRETS"):
        coffret_depuis(Reglages(database_url=URL, litellm_url="x", cle_secrets="court"))

def test_sources_actives_coffre_puis_gerees(bd):
    # YAML : coffre-maison (obsidian) + « vieux » (code, ignoré) ; base : « b-depot », « a-depot » gérées, « hors » non gérée
    noms = [s.conf.nom for s in sources_actives(bd, config, None)]
    assert noms == ["coffre-maison", "a-depot", "b-depot"]
    assert next(s for s in sources_actives(bd, config, None) if s.conf.nom == "a-depot").conf.exclure == ["banc/**"]

def test_jeton_illisible_ignore_avec_avertissement(bd, caplog):
    # jeton chiffré avec une clé A, lu avec une clé B
    [s] = [s for s in sources_actives(bd, config, Coffret(cle_b)) if s.conf.nom == "a-depot"]
    assert s.jeton is None and "a-depot" in caplog.text and "ghp_" not in caplog.text
```

- [ ] **Step 2 : lancer** `uv run pytest tests/test_sources_gerees.py -v` → FAIL (module absent).
- [ ] **Step 3 : écrire** la migration, les réglages, `Coffret`, `coffret_depuis`, `SourceActive` et `sources_actives` ; ajouter `"cryptography"` aux dépendances et `uv lock`. Dans `bd`, ajouter `instance` au `TRUNCATE`.
- [ ] **Step 4 : lancer** `uv run pytest tests/test_sources_gerees.py tests/test_db.py -v` → PASS.
- [ ] **Step 5 : commit** `feat: sources gérées en base (migration 006, coffret des jetons)`.

### Task 2 : Validation, ajout, modification, suppression

**Files :**
- Modify : `rag/sources_gerees.py`, `tests/test_sources_gerees.py`

**Interfaces :**
- Consumes : `Coffret`, `gestion.verrouiller_source`, `gestion.valider_motif`, `gestion.demander_reindexation`.
- Produces :
  - `class SourceInvalide(ValueError)` ; `class JetonImpossible(SourceInvalide)` (jeton fourni sans coffret).
  - `valider(nom: str, emplacement: str, branche: str, visibilite: str, exclure: list[str], utilisateur: str, *, noms_reserves: set[str]) -> SourceConf`. `noms_reserves` = noms des sources du YAML.
  - `ajouter(conn, conf: SourceConf, *, utilisateur: str, jeton: str | None, coffret: Coffret | None) -> None` : `INSERT … geree = true, demande_le = now()`, `NOTIFY rag_reindexer`, commit ; `SourceInvalide("nom déjà pris")` si le nom existe.
  - `modifier(conn, nom: str, conf: SourceConf, *, utilisateur: str, jeton: str | None, retirer_jeton: bool, coffret: Coffret | None) -> set[str]` : rend les champs changés parmi `{"emplacement", "branche", "visibilite", "exclure", "utilisateur", "jeton"}`. Sous `verrouiller_source`. URL ou branche changée → `DELETE FROM documents`, `noeuds`, `exclus` de la source, `revision = NULL`, `graphe_revision = NULL`. URL, branche ou `exclure` changé → `sans_garde = true`. Seule la visibilité changée → pas de réindexation ; tout autre changement → `demander_reindexation`. `jeton=None` et `retirer_jeton=False` → jeton gardé. `SourceInconnue` si la source n'existe pas ou n'est pas `geree`.
  - `bilan_suppression(conn, nom: str) -> dict` : `{"nom", "emplacement", "documents", "exclus", "exclusions_manuelles"}` ; `SourceInconnue` si elle n'existe pas ou n'est pas `geree`.
  - `supprimer(conn, nom: str) -> None` : sous `verrouiller_source`, `DELETE FROM exclusions_manuelles WHERE source = nom` puis `DELETE FROM sources WHERE nom = nom AND geree` ; `SourceInconnue` sinon.

- [ ] **Step 1 : tests qui échouent**

```python
@pytest.mark.parametrize("nom", ["", ".", "..", "a/b", "coffre-maison", "a b"])
def test_nom_refuse(nom): ...  # SourceInvalide

@pytest.mark.parametrize("url", ["http://h/x.git", "file:///etc", "ext::sh", "/srv/x", "https://u:p@h/x.git", "https:///x"])
def test_url_refusee(url): ...

def test_url_du_reseau_local_acceptee():
    assert valider("x", "https://192.168.2.10/x.git", "main", "public-maison", [], "x-access-token", noms_reserves=set())

def test_branche_refusee(): ...  # "" et "-x"

def test_ajouter_demande_l_indexation(bd):  # demande_le non nul, geree vrai
def test_ajouter_jeton_sans_coffret_refuse(bd):  # JetonImpossible, aucune ligne
def test_modifier_sans_jeton_garde_l_ancien(bd):  # jeton_chiffre inchangé
def test_modifier_retirer_jeton(bd):  # jeton_chiffre NULL
def test_modifier_visibilite_seule_ne_reindexe_pas(bd):  # demande_le reste NULL, rend {"visibilite"}
def test_modifier_url_efface_les_documents(bd, index_code):  # documents, noeuds, exclus à 0 ; revision NULL ; sans_garde vrai
def test_modifier_exclure_pose_sans_garde(bd): ...
def test_modifier_le_coffre_refuse(bd): ...  # SourceInconnue
def test_supprimer_efface_tout(bd, index_code):
    # après une exclusion manuelle sur la source : documents, morceaux, noeuds, liens, exclus, exclusions_manuelles de la source à 0
def test_bilan_suppression_compte(bd, index_code): ...
```

- [ ] **Step 2 : lancer** `uv run pytest tests/test_sources_gerees.py -v` → FAIL.
- [ ] **Step 3 : écrire** `valider` (règles des Global Constraints, `urllib.parse.urlsplit`), `ajouter`, `modifier`, `bilan_suppression`, `supprimer`. `exclure` passe par `valider_motif`.
- [ ] **Step 4 : lancer** → PASS.
- [ ] **Step 5 : commit** `feat: ajouter, modifier et supprimer une source git gérée`.

### Task 3 : Lecteur git généralisé, indexeur, travailleur et CLI sur `sources_actives`

**Files :**
- Modify : `rag/sources.py`, `rag/indexeur.py:107-120`, `rag/travailleur.py`, `rag/cli.py:20-55,285-295`, `tests/conftest.py` (`indexer_depot`, `index_code`), `tests/test_sources_git.py`, `tests/test_indexeur_code.py`, `tests/test_travailleur.py`

**Interfaces :**
- Consumes : `SourceActive`, `sources_actives`, `coffret_depuis`.
- Produces :
  - `LecteurGit(url, dest, branche="main", jeton="", utilisateur="x-access-token", protocoles: str | None = None)`. L'en-tête est posé sur `http.<scheme>://<hôte>/.extraheader` de l'URL. `protocoles` non `None` → `GIT_ALLOW_PROTOCOL`. Dans `synchroniser`, si `git remote get-url origin` ≠ `url`, le clone est refait.
  - `verifier_depot(url: str, branche: str, jeton: str = "", utilisateur: str = "x-access-token", *, protocoles: str | None = "https", delai_s: int = 30) -> None` : lève `ErreurSource` (secrets masqués) si `ls-remote` échoue ou ne trouve pas la branche.
  - `lecteur_pour(source: SourceConf, reglages, jeton: str | None = None, utilisateur: str = "x-access-token")` : jeton propre, sinon `reglages.github_jeton` si l'hôte est `github.com`, sinon `""` ; `protocoles="https"`.
  - `class SourceSupprimee(Exception)` dans `rag/indexeur.py` : `indexer_source` ne crée plus de source `code`. Il lit son `id` (`SELECT id FROM sources WHERE nom = %s AND type = 'code'`) et lève `SourceSupprimee` si elle est absente. L'upsert reste pour `obsidian`.
  - `menage_dossiers(donnees_dir: Path, noms: set[str]) -> list[str]` dans `rag/travailleur.py` : efface `clones/<x>` et `graphe/<x>` dont `x ∉ noms`, et rend les noms effacés. Appelé dans `passe`, verrou tenu, après `fermer_interrompues`.
  - `passe(conn, genre, config, reglages, modeles, sources=None, sans_garde: frozenset[str] = frozenset())` itère `sources_actives` et passe `forcer=src.conf.nom in sans_garde`. `SourceSupprimee` → `bilan[nom] = {"supprimee": True}`, sans erreur. `tour` lit `sans_garde` avec les demandes, puis remet `sans_garde = false` dans le même `UPDATE` que `demande_le = NULL`.
  - `rag indexer` et `rag graphe` itèrent `sources_actives`.
  - `tests/conftest.py` : `inscrire_source(bd, nom, visibilite="public-maison", emplacement="x")` insère une ligne `type='code', geree=true` ; `indexer_depot` et `index_code` l'appellent avant `indexer_source`.

- [ ] **Step 1 : tests qui échouent**

```python
def test_jeton_pour_l_hote_de_l_url(tmp_path):
    lec = LecteurGit("https://gitea.maison/a/b.git", tmp_path / "clones" / "c", jeton="j", utilisateur="nico")
    assert lec._env["GIT_CONFIG_KEY_0"] == "http.https://gitea.maison/.extraheader"
    assert base64.b64encode(b"nico:j").decode() in lec._env["GIT_CONFIG_VALUE_0"]

def test_url_changee_reclone(depot, tmp_path): ...  # 2e dépôt, même dest : lire() rend le contenu du 2e
def test_protocole_https_seul(depot, tmp_path):  # LecteurGit(str(depot), …, protocoles="https").synchroniser() → ErreurSource
def test_verifier_depot_branche_absente(depot):  # verifier_depot(str(depot), "nexiste", protocoles=None) → ErreurSource
def test_verifier_depot_ok(depot): ...
def test_lecteur_pour_jeton_github_seulement(tmp_path): ...  # gitea sans jeton propre : pas d'en-tête
def test_source_supprimee_n_est_pas_recreee(bd, depot, tmp_path, faux_modeles):
    # indexer_source sur une source code absente → SourceSupprimee ; SELECT count(*) FROM sources = 0
def test_sans_garde_apres_changement_d_url(bd, ...):
    # source indexée (3 fichiers), modifier() vers un dépôt à 1 fichier, tour() → pas d'erreur, 1 document, sans_garde faux
def test_menage_dossiers(tmp_path):  # clones/a, clones/b, graphe/b ; noms={"a"} → rend ["b"], a reste
def test_passe_source_supprimee_sans_erreur(bd, ...): ...
```

Remplacer `test_jeton_limite_a_github` par `test_jeton_pour_l_hote_de_l_url`.

- [ ] **Step 2 : lancer** `uv run pytest tests/test_sources_git.py tests/test_travailleur.py tests/test_indexeur_code.py -v` → FAIL.
- [ ] **Step 3 : écrire** le code des Interfaces. Factoriser la construction de l'environnement git (`_env_git(url, jeton, utilisateur, protocoles) -> tuple[dict, list[str]]`) partagée par `LecteurGit` et `verifier_depot`.
- [ ] **Step 4 : lancer** `uv run pytest -q` (toute la suite) → PASS.
- [ ] **Step 5 : commit** `feat: le travailleur et la CLI lisent les sources gérées ; lecteur git pour tout hôte https`.

### Task 4 : Export, import et CLI `rag sources`

**Files :**
- Modify : `rag/sources_gerees.py`, `rag/cli.py`, `tests/test_sources_gerees.py`, `tests/test_cli.py`

**Interfaces :**
- Consumes : `valider`, `ajouter`, `modifier`.
- Produces :
  - `exporter(conn) -> str` : YAML, liste triée par nom de `{nom, emplacement, branche, visibilite, exclure, utilisateur}` des sources `geree`. Jamais de jeton.
  - `lire_export(texte: str, *, noms_reserves: set[str] = frozenset()) -> list[dict]` : valide tout (clés exactement celles-là, `nom` et `emplacement` obligatoires, les autres facultatives avec leurs défauts) ou lève `SourceInvalide("entrée i : …")`.
  - `importer(conn, texte: str, *, noms_reserves: set[str]) -> dict` : `{"ajoutees": int, "modifiees": int, "inchangees": int}`, en une transaction. Une source inconnue passe par la logique d'`ajouter`, une source connue par celle de `modifier` (jeton gardé). Rien n'est supprimé.
  - CLI : `rag sources lister` (nom, emplacement, branche, visibilité, « jeton » ou « — »), `rag sources exporter` (stdout), `rag sources importer <fichier>`.

- [ ] **Step 1 : tests qui échouent**

```python
def test_export_sans_jeton(bd): ...  # source avec jeton : "jeton" absent du YAML, aller-retour lire_export(exporter()) identique
@pytest.mark.parametrize("texte", ["- {nom: a, emplacement: https://h/a.git, jeton: x}",
                                   "- {nom: a, emplacement: file:///etc}",
                                   "- {nom: a, emplacement: https://h/a.git}\n- {nom: '..', emplacement: https://h/b.git}",
                                   "pas une liste"])
def test_import_refuse_tout_ou_rien(bd, texte):  # SourceInvalide, SELECT count(*) FROM sources = 0
def test_import_ajoute_et_modifie(bd): ...  # bilan {"ajoutees": 1, "modifiees": 1, "inchangees": 0}
def test_cli_sources_exporter(...): ...
```

- [ ] **Step 2 : lancer** → FAIL.
- [ ] **Step 3 : écrire.**
- [ ] **Step 4 : lancer** → PASS.
- [ ] **Step 5 : commit** `feat: export, import et CLI des sources git`.

### Task 5 : Amorçage, config, démarrage et déploiement

**Files :**
- Create : `config/sources-maison.yaml` (les sources `type: code` de `config/rag.yaml`, au format de l'export, `banc/**` pour `RAG`, `branche: master` gardée là où elle est).
- Modify : `config/rag.yaml` et `config/dev.yaml` (sources de code retirées, commentaire qui renvoie à l'interface et à `sources-maison.yaml`), `rag/sources_gerees.py`, `rag/cli.py` (`migrer`), `rag/demarrage.py`, `deploy/portainer-stack.yml`, `scripts/vault-render-env.sh`, `tests/test_demarrage.py`, `tests/test_scripts.py`, `tests/test_sources_gerees.py`

**Interfaces :**
- Consumes : `lire_export(texte: str, *, noms_reserves) -> list[dict]` (tâche 4).
- Produces :
  - `amorcer(conn, chemin: Path) -> int` : sous `pg_advisory_xact_lock`, si `instance.sources_amorcees` est absent, upsert par nom de chaque entrée (`type='code'`, `geree=true`, `exclure`, `branche`, `visibilite`, `emplacement`, `utilisateur`). Ne touche ni `id`, ni `revision`, ni les documents, et ne pose ni `demande_le` ni `sans_garde`. Pose ensuite `sources_amorcees` et rend le nombre de sources. Journalise les lignes `type='code'` non gérées absentes du fichier. Un second appel rend `0`.
  - `rag migrer` appelle `amorcer` après `migrer` si `reglages.sources_amorce` est défini.
  - `verifier_reglages` : la règle `RAG_GITHUB_JETON` disparaît ; `coffret_depuis(r)` est appelé (une clé invalide arrête le démarrage) ; avertissement sur stderr si le YAML contient encore des sources `code`.
  - Pile : `RAG_GITHUB_JETON: ${RAG_GITHUB_JETON:-}`, `RAG_CLE_SECRETS: ${RAG_CLE_SECRETS:-}`, `RAG_SOURCES_AMORCE: /app/config/sources-maison.yaml`. `vault-render-env.sh` : `RAG_CLE_SECRETS` ajoutée à `CLES` et, avec `RAG_GITHUB_JETON`, à `FACULTATIVES`.

- [ ] **Step 1 : tests qui échouent**

```python
def test_amorcer_garde_les_lignes(bd, index_code):
    # « depot-test » non gérée, indexée ; fichier d'amorce qui la nomme avec exclure ["banc/**"]
    rev, docs = ...
    assert amorcer(bd, f) == 1 and (revision, documents, id) inchangés and geree and exclure == ["banc/**"]
    assert bd.execute("SELECT demande_le FROM sources WHERE nom='depot-test'").fetchone()["demande_le"] is None
    assert amorcer(bd, f) == 0

def test_amorcer_instance_neuve(bd, tmp_path):  # fichier vide → 0, sources vide, marqueur posé
def test_config_principale_sans_source_de_code():
    assert all(s.type != "code" for s in charger_config(Path("config/rag.yaml")).sources)
def test_sources_maison_valides():
    assert len(lire_export(Path("config/sources-maison.yaml").read_text())) == 50  # sources code de rag.yaml au 2026-11-23
```

Dans `test_demarrage.py`, la règle GitHub est remplacée par `RAG_CLE_SECRETS invalide`. Dans `test_scripts.py`, `RAG_GITHUB_JETON=""` n'est plus une erreur.

- [ ] **Step 2 : lancer** → FAIL.
- [ ] **Step 3 : écrire** le code, la config et la pile.
- [ ] **Step 4 : lancer** `uv run pytest -q` → PASS.
- [ ] **Step 5 : commit** `feat: amorçage unique des sources depuis sources-maison.yaml ; jeton GitHub facultatif`.

### Task 6 : Écrans de l'interface

**Files :**
- Create : `rag/ui/gabarits/source_git.html` (formulaire ajouter et modifier), `rag/ui/gabarits/supprimer_source.html`
- Modify : `rag/ui/__init__.py`, `rag/api.py` (passer `coffret` et `noms_reserves` à `routeur_ui`), `rag/ui/gabarits/sources.html`, `rag/ui/gabarits/_macros.html` (colonne Actions), `rag/ui/statique/rag.css` si besoin, `tests/test_ui.py`

**Interfaces :**
- Consumes : tâches 1, 2, 3 (`verifier_depot`) et 4.
- Produces (routes, toutes `ecrivain` pour les POST ; les GET demandent `peut_ecrire`, sinon 403) :
  - `GET /ui/sources-git/nouvelle` → formulaire vide.
  - `POST /ui/sources-git` (champs `nom, emplacement, branche, visibilite, exclure` (un motif par ligne), `utilisateur, jeton`) → `valider`, `verifier_depot`, `ajouter` ; succès : `HX-Redirect: /ui/` ; erreur : message 400 dans le formulaire.
  - `GET /ui/sources-git/{nom}/modifier` → formulaire prérempli. Le champ jeton est vide, avec « jeton enregistré » s'il y en a un, et une case `retirer_jeton`. Le nom n'est pas modifiable.
  - `POST /ui/sources-git/{nom}/modifier` → `verifier_depot` seulement si l'URL, la branche, l'utilisateur ou le jeton change, puis `modifier` ; `HX-Redirect: /ui/`.
  - `GET /ui/sources-git/{nom}/supprimer` → `bilan_suppression` affiché, bouton « Supprimer définitivement ».
  - `POST /ui/sources-git/{nom}/supprimer` → `supprimer` ; `HX-Redirect: /ui/`.
  - `GET /ui/sources-git/export` → `exporter`, `Content-Disposition: attachment; filename="sources-rag.yaml"`.
  - `POST /ui/sources-git/import` → même garde de taille et de lecture que l'import d'exclusions (`IMPORT_MAX`), puis `importer`.
  - `sources.html` : boutons « Ajouter une source git », « Exporter », « Importer » (écrivain seulement). `ligne_source` : « Modifier » et « Supprimer » pour une source gérée, « (config) » pour le coffre. `gestion.sources` ajoute la colonne `geree`.
  - Journal : `log.info("source git ajoutée : %s", nom)`, idem modifiée et supprimée ; jamais l'URL avec le jeton ni le jeton lui-même.

- [ ] **Step 1 : tests qui échouent** (`verifier_depot` remplacé par `monkeypatch`)

```python
def test_ajouter_source_par_l_interface(client, bd, monkeypatch): ...  # 200 + HX-Redirect, ligne en base, demande_le non nul
def test_ajouter_depot_injoignable_n_ecrit_rien(client, bd, monkeypatch):
    # verifier_depot lève ErreurSource("git ls-remote : *** refusé") → 400, message affiché, 0 source
def test_formulaire_ne_reaffiche_jamais_le_jeton(client, bd):  # "ghp_secret" absent de la page modifier, "jeton enregistré" présent
def test_modifier_sans_jeton_ne_verifie_pas_si_rien_de_reseau(client, bd, monkeypatch):
    # seule la visibilité change : verifier_depot jamais appelé, jeton gardé
def test_supprimer_affiche_le_bilan_puis_efface(client, bd, index_code): ...
def test_coffre_sans_actions(client, bd, index): ...  # pas de lien modifier/supprimer pour coffre-maison ; GET …/coffre-maison/supprimer → 404
def test_ecriture_refusee_au_jeton_public(client, bd): ...  # GET nouvelle → 403 ; POST → 403
def test_ecriture_refusee_sans_hx_ou_origine_etrangere(client, bd): ...
def test_jeton_absent_des_journaux(client, bd, monkeypatch, caplog): ...
def test_export_sources_reserve_a_l_ecrivain(client, bd): ...
```

- [ ] **Step 2 : lancer** `uv run pytest tests/test_ui.py -v` → FAIL.
- [ ] **Step 3 : écrire** les routes et les gabarits dans le style des écrans d'exclusions (`message`, `page_ui`, `_sans_cache`). `verifier_depot` est appelé dans `run_in_threadpool` (30 s au plus).
- [ ] **Step 4 : lancer** `uv run pytest -q && uv run ruff check .` → PASS.
- [ ] **Step 5 : commit** `feat(ui): ajouter, modifier, supprimer, exporter et importer les sources git`.

### Task 7 : README et vérification de bout en bout

**Files :**
- Modify : `README.md` (sources gérées dans l'interface, `RAG_CLE_SECRETS` à créer dans OpenBao, `RAG_SOURCES_AMORCE`, « exporter les sources après chaque changement » à côté de l'export des exclusions, commande `rag sources exporter`)

- [ ] **Step 1 : écrire** le README.
- [ ] **Step 2 : vérifier en local** avec `compose.dev.yml` : `RAG_CONFIG=config/dev.yaml RAG_SOURCES_AMORCE=config/sources-maison.yaml uv run rag migrer` → « N sources amorcées » ; second appel → 0. Lancer `uv run rag servir`, ajouter un dépôt public (`https://github.com/nicolaslallier/Glee.git` s'il n'est pas déjà là, sinon un dépôt public tiers) depuis `/ui/`, et vérifier qu'il passe à « en attente ». Puis lancer `uv run rag travailleur` et vérifier qu'il s'indexe. Modifier la branche vers une branche absente → message d'erreur, rien de changé. Supprimer → la ligne disparaît et `donnees/clones/<nom>` est effacé au tour suivant.
- [ ] **Step 3 : lancer** `uv run pytest -q && uv run ruff check .` → PASS.
- [ ] **Step 4 : commit** `docs: sources git gérées depuis l'interface (README)`.
