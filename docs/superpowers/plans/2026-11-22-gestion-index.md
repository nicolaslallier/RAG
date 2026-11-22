# Gestion de l'index (interface web et outils MCP) — plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal :** voir ce que le RAG a indexé ou exclu, exclure ou réintégrer des fichiers depuis une interface web servie par l'API, et exposer la consultation à des agents par deux outils MCP.

**Architecture :** une table `exclusions_manuelles` et une table `exclus` élargie en registre de toutes les exclusions ; l'indexeur applique les motifs manuels comme `exclure:`. Toute la logique vit dans `rag/gestion.py`, appelé par trois portes : les pages `/ui/` (Jinja2 + htmx, cookie), le MCP (lecture) et la CLI (export/import).

**Tech Stack :** Python 3.13, FastAPI, psycopg 3, Jinja2, python-multipart, htmx 2.0.4 (copié dans le dépôt), pytest sur Postgres 18 + pgvector, ruff.

**Spec :** `docs/superpowers/specs/2026-11-22-gestion-index-design.md`

## Global Constraints

- Même processus et même image que l'API (`rag servir`, port 8000) ; aucun service ajouté à la pile, aucun CDN.
- Lecture : `RAG_JETON` (voit `public-maison`) ou `RAG_JETON_PRIVE` (voit tout) ; toute requête filtre `s.visibilite = ANY(niveaux)`.
- Écriture (exclure, annuler, importer, réindexer depuis l'interface) : `RAG_JETON_PRIVE` seulement, et seulement sous `/ui/` (cookie) ; jamais par l'API Bearer ni le MCP. Sans `RAG_JETON_PRIVE` défini, l'interface est en lecture seule.
- Cookie `rag_session` : `HttpOnly`, `SameSite=Strict`, `Path=/ui`, sans `Secure` ; il contient le jeton. Le jeton n'apparaît jamais dans une page ni un journal.
- Requêtes d'écriture : en-tête `HX-Request: true` exigé, et `Origin` absent ou égal à l'hôte de la requête ; sinon 403.
- Motif : `fnmatch` (le `*` traverse les `/`), 1 à 1000 caractères ; raison facultative, 500 caractères au plus.
- Aperçu : total et 50 premiers chemins ; seconde confirmation au-delà de 100 documents.
- Pagination : 50 lignes par page.
- Une source absente ou invisible pour le jeton donne la même réponse (404 en page, erreur « source inconnue » en MCP), sans dire si elle existe.
- Le contenu d'un fichier exclu n'est jamais lu ni affiché : seulement le chemin et la raison.
- Textes de l'interface en français.
- `uv run ruff check .` et `uv run pytest -m "not vivant and not lent"` passent à la fin de chaque tâche.

## Review Focus

- Un motif qui couvre toute une source (`*`, `**`) : l'aperçu donne le vrai total, la seconde confirmation est exigée, et la passe suivante ne lève pas `CoffrePartielError` (tests en tâches 2 et 8).
- Une exclusion posée pendant une passe sur la même source : le document n'est pas réécrit (test en tâche 2).
- Un fichier exclu par le nom qui n'est plus exclu (motif retiré de la config, exclusion annulée) : il est réindexé et `deja_exclus` ne le bloque pas (test en tâche 1).
- Un import YAML malformé ou hostile (pas une liste, clés en trop, motif vide ou trop long) : refusé en bloc, sans écriture partielle (test en tâche 4).
- Un chemin de fichier avec des espaces, des accents, `#` ou `?` dans les URL du détail : il s'ouvre correctement (test en tâche 7).

---

### Task 1 : Registre `exclus` élargi et migration 005

**Files :**
- Create : `rag/migrations/005_exclusions_manuelles.sql`
- Modify : `rag/indexeur.py` (`indexer_source`, l. 115-200)
- Test : `tests/test_indexeur_code.py`, `tests/test_indexeur.py`

**Interfaces :**
- Produces : table `exclusions_manuelles (id serial, source text, motif text, raison text NULL, cree_le timestamptz, UNIQUE (source, motif))` (SQL du spec) ; colonne `exclus.lu boolean NOT NULL DEFAULT true` (true = verdict pris après lecture du contenu). Raisons possibles dans `exclus.raison` : `secret`, `binaire`, `genere`, `trop_gros`, `config`, `manuelle`.

- [ ] **Step 1 : Écrire les tests qui échouent**

Dans `tests/test_indexeur_code.py`, remplacer l'assertion l. 28-29 :
```python
raisons = {r["chemin"]: (r["raison"], r["lu"]) for r in bd.execute("SELECT chemin, raison, lu FROM exclus")}
assert raisons == {"logo.png": ("binaire", True), ".env.example": ("secret", False),
                   "uv.lock": ("genere", False), "gros.json": ("trop_gros", False)}
```
(vérifier d'abord que `gros.json` dépasse bien `10 * taille_max_octets` dans la config ; sinon le retirer de l'attendu). Ajouter :
```python
def test_exclu_sur_le_nom_puis_plus_exclu_est_reindexe(bd, depot, tmp_path, faux_modeles):
    # 1re passe avec exclure=["docs/**"] : docs/guide.md a raison "config", lu=False
    # 2e passe sans exclure, même commit : docs/guide.md est dans documents et plus dans exclus
def test_exclus_disparu_du_listage_est_oublie(bd, depot, ...):
    # git rm uv.lock + commit, 2e passe : plus de ligne exclus pour uv.lock
```
Les compteurs `Bilan.exclus` existants ne changent pas (`config` n'est toujours pas compté).

- [ ] **Step 2 : Lancer, constater l'échec**

Run : `uv run pytest tests/test_indexeur_code.py -v`
Expected : FAIL (colonne `lu` absente, raisons manquantes).

- [ ] **Step 3 : Migration et indexeur**

Migration : `CREATE TABLE exclusions_manuelles …` puis `ALTER TABLE exclus ADD COLUMN lu boolean NOT NULL DEFAULT true`.
Indexeur : chaque exclusion décidée au listage (raison non nulle, `trop_gros` compris) est upsertée dans `exclus` avec `signature = e.signature`, `empreinte = emp`, `lu = false`. L'insertion après lecture existante pose `lu = true`. `deja_exclus` ne lit que les lignes `lu`. Le nettoyage final (l. 196) supprime de `exclus` les chemins absents de `entrees ∪ exclus_au_listage`. Un chemin qui n'est plus exclu revient dans `entrees` ; une fois écrit dans `documents`, sa ligne `exclus` est supprimée (l. 182).

- [ ] **Step 4 : Lancer, constater le succès**

Run : `uv run pytest tests/test_indexeur_code.py tests/test_indexeur.py -v`
Expected : PASS.

- [ ] **Step 5 : Commit**

`git commit -m "feat(index): exclus devient le registre de toutes les exclusions ; migration 005"`

---

### Task 2 : Motifs manuels dans l'indexeur et le graphe

**Files :**
- Create : `rag/gestion.py` (deux fonctions seulement dans cette tâche)
- Modify : `rag/indexeur.py`, `rag/graphe.py` (`maj_graphe`, l. 196-215)
- Test : `tests/test_gestion.py` (nouveau)

**Interfaces :**
- Produces :
  - `motifs_manuels(conn, source: str) -> list[str]`
  - `correspond(chemin: str, motifs: Iterable[str]) -> bool` (`fnmatch`, comme `exclure:`)

- [ ] **Step 1 : Écrire les tests qui échouent** (`tests/test_gestion.py`, fixture `index` du conftest = source `coffre-maison`)

```python
def test_motif_manuel_exclu_a_la_passe(bd, index, faux_modeles):
    bd.execute("INSERT INTO exclusions_manuelles (source, motif) VALUES ('coffre-maison', '03 - Ressources/**')")
    bd.commit()
    b = reindexer_coffre(bd, index, faux_modeles)          # helper local : indexer_source sur index.coffre
    assert {"03 - Ressources/Grafana.md", "03 - Ressources/Kubernetes.md"}.isdisjoint(chemins(bd))
    assert raisons(bd)["03 - Ressources/Grafana.md"] == ("manuelle", False)

def test_motif_couvrant_toute_la_source_ne_bloque_pas_la_passe(bd, index, faux_modeles):
    # motif "*" ; les documents ont été supprimés par la logique d'exclusion (simulée ici : DELETE FROM documents)
    # indexer_source ne lève pas CoffrePartielError ; documents vide ; exclus = 6 lignes "manuelle"

def test_exclusion_posee_pendant_la_passe_n_est_pas_ecrite(bd, index, faux_modeles, monkeypatch):
    # nouvelle note ajoutée au coffre ; monkeypatch de lecteur.lire qui, à la lecture de cette note,
    # insère (par une autre connexion, commit) une exclusion manuelle de son chemin
    # après la passe : la note n'est pas dans documents, exclus a raison "manuelle"

def test_graphify_ne_voit_pas_les_fichiers_exclus(bd, index_code, monkeypatch, tmp_path):
    # exclusion manuelle "a.py" sur depot-test ; monkeypatch extraire_structure pour capturer `exclure`
    # maj_graphe(forcer=True) appelle extraire_structure avec exclure contenant "a.py"
```

- [ ] **Step 2 : Lancer, constater l'échec**

Run : `uv run pytest tests/test_gestion.py -v` → FAIL (`exclusions_manuelles` ignorée).

- [ ] **Step 3 : Implémenter**

- `indexer_source` lit `motifs_manuels(conn, source.nom)` une fois au début ; au listage, `correspond(chemin, motifs)` donne la raison `manuelle` (enregistrée comme au Task 1, non comptée dans `entrees`, donc hors `absents`).
- Juste avant `_ecrire`, relire `motifs_manuels` (une requête) ; si le chemin correspond : supprimer le document existant s'il y en a un, upserter `exclus` (`manuelle`, `lu = false`), `commit`, `continue`.
- `maj_graphe` (sources de code) passe `exclure = source.exclure + motifs_manuels(conn, source.nom)` à `extraire_structure`.

- [ ] **Step 4 : Lancer, constater le succès**

Run : `uv run pytest tests/test_gestion.py tests/test_indexeur.py tests/test_indexeur_code.py tests/test_graphe.py -v` → PASS.

- [ ] **Step 5 : Commit**

`git commit -m "feat(index): exclusions manuelles appliquées par l'indexeur et graphify"`

---

### Task 3 : Lecture partagée (`sources`, `fichiers`, `fichier`)

**Files :**
- Modify : `rag/gestion.py`
- Test : `tests/test_gestion.py`

**Interfaces :**
- Consumes : `motifs_manuels`, `correspond` (Task 2).
- Produces :
  - `class SourceInconnue(Exception)`
  - `sources(conn, *, niveaux: frozenset[str]) -> list[dict]` : clés `nom, type, visibilite, revision, indexe_le, documents, exclus_auto, exclus_manuels, demande_en_attente` ; trié par nom.
  - `fichiers(conn, source: str, *, niveaux, etat: Literal["indexe","exclu_auto","exclu_manuel"] | None = None, contient: str | None = None, page: int = 1, par_page: int = 50) -> dict` : `{"source", "total", "page", "pages", "fichiers": [{"chemin", "etat", "raison", "titre", "type", "tags", "morceaux", "modifie_le"}]}` ; tri par chemin ; `contient` = sous-chaîne insensible à la casse sur le chemin ; lève `SourceInconnue`.
  - `fichier(conn, source: str, chemin: str, *, niveaux, version_id: int | None) -> dict` : `{"source", "chemin", "titre", "type", "tags", "alias", "origine", "frontmatter", "liens_sortants", "modifie_le", "morceaux": [{"ordre", "fil", "texte", "symbole", "ligne"}]}` (morceaux de `version_id`, triés par `ordre`, parents compris) ; `SourceInconnue` si la source ou le document est absent ou invisible.

- [ ] **Step 1 : Tests qui échouent**

```python
def test_sources_compte_indexes_et_exclus(bd, index): ...         # coffre-maison : documents == 6, exclus_auto == 0
def test_sources_cache_une_source_privee_au_jeton_public(bd, index):
    bd.execute("UPDATE sources SET visibilite='prive'"); bd.commit()
    assert sources(bd, niveaux=PUBLIC) == [] and len(sources(bd, niveaux=TOUT)) == 1
def test_fichiers_union_indexes_et_exclus_pagine(bd, index_code):
    r = fichiers(bd, "depot-test", niveaux=PUBLIC, par_page=2)
    assert r["total"] == 7 and r["pages"] == 4 and [f["chemin"] for f in r["fichiers"]] == [".env.example", "a.py"]
    assert {f["etat"] for f in fichiers(bd, "depot-test", niveaux=PUBLIC, etat="exclu_auto")["fichiers"]} == {"exclu_auto"}
def test_fichiers_filtre_contient(bd, index): ...                  # contient="poulet" → 1 fichier
def test_fichiers_source_privee_ou_inconnue(bd, index):            # même exception SourceInconnue pour les deux
def test_fichier_detail_avec_morceaux(bd, index): ...              # "Accueil.md" : morceaux non vide, fil commence par "Accueil"
```
(adapter le total 7 au dépôt de test réel : 5 fichiers suivis par git + `docs/guide.md`, compter avant d'écrire l'assertion.)

- [ ] **Step 2 : Lancer** → FAIL (fonctions absentes).
- [ ] **Step 3 : Implémenter** en SQL (`UNION ALL` de `documents` et `exclus` pour `fichiers`, `count(*) OVER ()` pour le total).
- [ ] **Step 4 : Lancer** `uv run pytest tests/test_gestion.py -v` → PASS.
- [ ] **Step 5 : Commit** `feat(gestion): lecture des sources et des fichiers indexés ou exclus`

---

### Task 4 : Écriture partagée, export/import et CLI

**Files :**
- Modify : `rag/gestion.py`, `rag/api.py` (`route_reindexer`, l. 208-216), `rag/cli.py`
- Test : `tests/test_gestion.py`, `tests/test_cli.py`

**Interfaces :**
- Consumes : Task 2 et 3.
- Produces :
  - `class MotifInvalide(ValueError)` ; `valider_motif(motif: str) -> str` (1 à 1000 caractères après `strip`, sinon `MotifInvalide`).
  - `apercu(conn, source: str, motif: str) -> dict` : `{"total": int, "chemins": list[str]}` (50 premiers, triés) — documents indexés touchés.
  - `exclure(conn, source: str, motif: str, raison: str | None = None) -> int` : une transaction ; insère (rien si déjà présent), supprime les documents correspondants (cascade morceaux), supprime `noeuds WHERE document_id = ANY(ids)`, upserte `exclus` (`manuelle`, `lu = false`, signature et empreinte reprises du document) ; retourne le nombre de documents supprimés. `SourceInconnue` si la source n'est pas dans `sources`.
  - `annuler(conn, source: str, motif: str) -> bool` : supprime la ligne, supprime les lignes `exclus` `manuelle` de cette source que plus aucun motif ne couvre, pose `graphe_revision = NULL` si source de code, appelle `demander_reindexation` ; `False` si le motif n'existait pas.
  - `demander_reindexation(conn, source: str | None) -> int` : le `UPDATE sources SET demande_le = now() … ; NOTIFY rag_reindexer` actuel de `route_reindexer`, qui l'appelle désormais.
  - `exclusions(conn) -> list[dict]` : `source, motif, raison, cree_le`, triées.
  - `exporter(conn) -> str` : YAML, liste de `{source, motif, raison}`.
  - `importer(conn, texte: str) -> dict` : `{"ajoutees": int, "ignorees": int}` ; valide tout avant d'écrire (liste de dictionnaires aux clés `source`, `motif`, `raison` facultative, rien d'autre ; `yaml.safe_load`) sinon `MotifInvalide` sans rien écrire ; source inconnue : ligne gardée, appliquée quand la source existera ; source connue : appliquée comme `exclure`.
  - CLI : `rag exclusions exporter` (YAML sur stdout) et `rag exclusions importer FICHIER`.

- [ ] **Step 1 : Tests qui échouent**

```python
def test_apercu_et_exclure_immediat(bd, index):
    assert apercu(bd, "coffre-maison", "02 - Domaines/**")["total"] == 2
    assert exclure(bd, "coffre-maison", "02 - Domaines/**", "essai") == 2
    assert "02 - Domaines/Cuisine/Recettes/Pâté chinois.md" not in chemins(bd)
def test_exclure_retire_les_noeuds_du_graphe(bd, index):  # charger_wikiliens puis exclure "Accueil.md" : aucun nœud cle="Accueil.md"
def test_exclure_source_inconnue(bd, index): pytest.raises(SourceInconnue)
def test_motif_vide_ou_trop_long(bd, index): pytest.raises(MotifInvalide) pour "" , "  ", "x"*1001
def test_annuler_demande_la_reindexation(bd, index, faux_modeles):
    exclure(bd, "coffre-maison", "Accueil.md"); assert annuler(bd, "coffre-maison", "Accueil.md") is True
    # demande_le non nul ; après indexer_source, "Accueil.md" est revenu dans documents
def test_annuler_motif_absent(bd, index): assert annuler(bd, "coffre-maison", "zzz") is False
def test_export_import_aller_retour(bd, index):
    exclure(bd, "coffre-maison", "Accueil.md", "bruit"); t = exporter(bd)
    bd.execute("DELETE FROM exclusions_manuelles"); bd.commit()
    assert importer(bd, t) == {"ajoutees": 1, "ignorees": 0} and importer(bd, t) == {"ajoutees": 0, "ignorees": 1}
@pytest.mark.parametrize("texte", ["pas une liste", "- {source: a}", "- {source: a, motif: '', raison: x}",
                                   "- {source: a, motif: b, autre: 1}", "- {source: a, motif: b}\n- 3"])
def test_import_malforme_n_ecrit_rien(bd, index, texte): MotifInvalide, et exclusions(bd) == []
```
Dans `tests/test_cli.py` : `rag exclusions exporter` imprime le YAML ; `rag exclusions importer f.yaml` l'applique.
Les tests existants de `/reindexer` (`tests/test_api.py`) doivent rester verts.

- [ ] **Step 2 : Lancer** → FAIL.
- [ ] **Step 3 : Implémenter.**
- [ ] **Step 4 : Lancer** `uv run pytest tests/test_gestion.py tests/test_cli.py tests/test_api.py -v` → PASS.
- [ ] **Step 5 : Commit** `feat(gestion): exclure, annuler, exporter et importer les exclusions manuelles`

---

### Task 5 : Outils MCP `rag_sources` et `rag_fichiers`

**Files :**
- Modify : `rag/mcp_serveur.py`, `rag/api.py` (le `SimpleNamespace` passé à `creer_mcp`)
- Test : `tests/test_mcp.py`

**Interfaces :**
- Consumes : `sources`, `fichiers`, `SourceInconnue` (Task 3).
- Produces : l'`etat` du MCP gagne `sources(niveaux)` et `fichiers(source, etat, contient, page, niveaux)`. Outils :
  - `rag_sources() -> str` (JSON de `sources`), description : « Liste les sources indexées (coffre, dépôts) avec leurs nombres de fichiers indexés et exclus. »
  - `rag_fichiers(source: Nom, etat: Literal["indexe","exclu_auto","exclu_manuel"] | None = None, contient: Nom | None = None, page: int ≥ 1 = 1) -> str` (JSON de `fichiers`), description : « Liste les fichiers d'une source, indexés ou exclus (avec la raison), 50 par page. » `SourceInconnue` → erreur d'outil « source inconnue ».
  - Docstring du module : « sept outils de lecture ».

- [ ] **Step 1 : Tests qui échouent** : `tools/list` contient les sept noms ; `rag_sources` renvoie `coffre-maison` ; `rag_fichiers` avec `etat="indexe"` renvoie 6 fichiers ; après `UPDATE sources SET visibilite='prive'`, `rag_sources` est vide pour `pub` et `rag_fichiers` renvoie `isError` pour `pub` mais réussit pour `priv`.
- [ ] **Step 2 : Lancer** `uv run pytest tests/test_mcp.py -v` → FAIL.
- [ ] **Step 3 : Implémenter.**
- [ ] **Step 4 : Lancer** → PASS.
- [ ] **Step 5 : Commit** `feat(mcp): outils rag_sources et rag_fichiers`

---

### Task 6 : Interface — socle, connexion et écran Sources

**Files :**
- Create : `rag/ui/__init__.py`, `rag/ui/gabarits/base.html`, `rag/ui/gabarits/connexion.html`, `rag/ui/gabarits/sources.html`, `rag/ui/statique/htmx.min.js`, `rag/ui/statique/rag.css`
- Modify : `pyproject.toml` (+ `jinja2`, `python-multipart`), `uv.lock`, `rag/api.py` (monter le routeur, `GET /` → 307 vers `/ui/`)
- Test : `tests/test_ui.py` (nouveau)

**Interfaces :**
- Consumes : `sources` (Task 3), `demander_reindexation` (Task 4), `niveaux_de` de `create_app`.
- Produces :
  - `routeur_ui(*, niveaux_de: Callable[[str], frozenset[str] | None], ecriture_possible: bool, base_url: str, version_id: Callable[[], int | None]) -> APIRouter` (préfixe `/ui`), monté par `create_app`.
  - Dépendances internes : `session(request) -> frozenset[str]` (lit le cookie `rag_session`, appelle `niveaux_de("Bearer " + jeton)` ; absent ou invalide → 303 vers `/ui/connexion`) ; `ecrivain(request) -> None` (exige `niveaux == TOUT`, `ecriture_possible`, `HX-Request: true`, `Origin` absent ou égal à `request.base_url` sans la barre finale ; sinon 403).
  - Routes : `GET /ui/connexion`, `POST /ui/connexion` (formulaire `jeton` ; valide → cookie + 303 `/ui/` ; invalide → 401 avec le formulaire et « Jeton refusé. »), `POST /ui/deconnexion`, `GET /ui/` (écran Sources), `POST /ui/sources/{nom}/reindexer` (écrivain ; renvoie le fragment de ligne).
  - Gabarits : `base.html` charge `/ui/statique/htmx.min.js` et `rag.css` ; variable `ecrivain: bool` pour afficher ou non les boutons.

- [ ] **Step 1 : Copier htmx** : `curl -fsSL https://unpkg.com/htmx.org@2.0.4/dist/htmx.min.js -o rag/ui/statique/htmx.min.js`, vérifier que le fichier commence par `var htmx=` ou `(function`, et noter la version en commentaire dans `base.html`.
- [ ] **Step 2 : Dépendances** : `uv add jinja2 python-multipart` (vérifier que `uv.lock` change et que `uv sync --locked` passe).
- [ ] **Step 3 : Tests qui échouent** (`tests/test_ui.py`, client comme `tests/test_api.py` avec `jeton="pub"`, `jeton_prive="priv"`) :
```python
def test_racine_redirige_vers_ui(client): assert client.get("/", follow_redirects=False).headers["location"] == "/ui/"
def test_ui_sans_cookie_redirige_vers_connexion(client): 303 → /ui/connexion
def test_connexion_pose_un_cookie_httponly_strict(client):
    r = client.post("/ui/connexion", data={"jeton": "priv"}, follow_redirects=False)
    c = r.headers["set-cookie"]
    assert r.status_code == 303 and "HttpOnly" in c and "SameSite=strict" in c.replace("Strict", "strict") and "Path=/ui" in c and "Secure" not in c
def test_connexion_refusee(client): 401, "Jeton refusé." dans le texte
def test_le_jeton_n_apparait_jamais_dans_la_page(client): après connexion, "priv" absent de GET /ui/
def test_sources_public_ne_voit_pas_prive(client, bd): source passée en prive ; cookie pub → nom absent ; cookie priv → présent
def test_boutons_absents_pour_le_jeton_public(client): pas de "Réindexer" avec pub, présent avec priv
def test_reindexer_exige_l_ecrivain(client):
    # pub + HX-Request → 403 ; priv sans HX-Request → 403 ; priv + HX-Request + Origin étranger → 403 ; priv + HX-Request → 200 et demande_le posé
def test_sans_jeton_prive_l_interface_est_en_lecture_seule(cfg, faux_modeles): client sans jeton_prive, cookie pub : pas de bouton, POST → 403
def test_jeton_absent_des_journaux(client, caplog): connexion réussie et ratée : "priv" absent de caplog.text
```
- [ ] **Step 4 : Lancer** `uv run pytest tests/test_ui.py -v` → FAIL.
- [ ] **Step 5 : Implémenter** (`Jinja2Templates`, `StaticFiles` sur `rag/ui/statique` monté à `/ui/statique`, autoescape actif ; CSS sobre avec `prefers-color-scheme`).
- [ ] **Step 6 : Lancer** → PASS, puis `docker build -t rag:local --build-arg GROUPES= .` et vérifier que `/app/rag/ui/gabarits/base.html` est dans l'image (`docker run --rm rag:local ls /app/rag/ui/gabarits`).
- [ ] **Step 7 : Commit** `feat(ui): connexion par jeton et écran des sources`

---

### Task 7 : Interface — fichiers d'une source et détail

**Files :**
- Create : `rag/ui/gabarits/fichiers.html`, `rag/ui/gabarits/_lignes_fichiers.html`, `rag/ui/gabarits/fichier.html`
- Modify : `rag/ui/__init__.py`
- Test : `tests/test_ui.py`

**Interfaces :**
- Consumes : `fichiers`, `fichier`, `SourceInconnue` (Task 3) ; `session` (Task 6).
- Produces : `GET /ui/sources/{nom}?etat=&contient=&page=` (page complète ; avec `HX-Request`, seulement le fragment `_lignes_fichiers.html` pour le filtre en direct) ; `GET /ui/sources/{nom}/fichier?chemin=…` (le chemin en paramètre de requête, encodé par `urlencode`) ; `SourceInconnue` → 404 « Source ou fichier introuvable. ». Chaque ligne exclue montre sa raison ; chaque ligne indexée mène au détail ; avec `ecrivain`, chaque ligne a un bouton « Exclure » qui ouvre l'aperçu de la Task 8 avec ce chemin comme motif.

- [ ] **Step 1 : Tests qui échouent** : liste paginée (`page=2` montre la suite) ; filtre `etat=exclu_auto` sur `depot-test` ; filtre `contient` en fragment htmx (pas de `<html>` dans la réponse) ; détail de « 02 - Domaines/Cuisine/Recettes/Pâté chinois.md » (espaces, accent) atteint par l'URL générée dans la liste, et un chemin ajouté au coffre de test « Notes/C# et ? test.md » s'ouvre aussi ; source privée avec cookie pub → 404 ; aucun texte de fichier exclu dans la page (`CLE=xyz` de `.env.example` absent).
- [ ] **Step 2 : Lancer** → FAIL.
- [ ] **Step 3 : Implémenter.**
- [ ] **Step 4 : Lancer** `uv run pytest tests/test_ui.py -v` → PASS.
- [ ] **Step 5 : Commit** `feat(ui): fichiers d'une source et détail des morceaux`

---

### Task 8 : Interface — exclusions

**Files :**
- Create : `rag/ui/gabarits/exclusions.html`, `rag/ui/gabarits/_apercu.html`
- Modify : `rag/ui/__init__.py`, `rag/ui/gabarits/base.html` (menu : Sources, Exclusions, Déconnexion)
- Test : `tests/test_ui.py`

**Interfaces :**
- Consumes : `apercu`, `exclure`, `annuler`, `exclusions`, `exporter`, `importer`, `MotifInvalide`, `SourceInconnue` (Task 4) ; `session`, `ecrivain` (Task 6).
- Produces :
  - `GET /ui/exclusions` : liste (source, motif, raison, date) ; formulaire d'ajout (source en liste déroulante des sources visibles, motif, raison) et boutons « Exporter », « Importer » si `ecrivain`.
  - `POST /ui/exclusions/apercu` (écrivain ; formulaire `source, motif, raison`) → fragment `_apercu.html` : « N documents seront retirés de l'index », 50 chemins, bouton « Confirmer » ; si N > 100, une case « Je confirme retirer N documents » à cocher, exigée par la route suivante.
  - `POST /ui/exclusions` (écrivain ; `source, motif, raison, confirme_gros`) → si `apercu` > 100 et `confirme_gros` absent : 400 ; sinon `exclure`, puis fragment de liste à jour et message « N documents retirés. ».
  - `POST /ui/exclusions/annuler` (écrivain ; `source, motif`) → fragment à jour, message « Exclusion annulée : le fichier reviendra à la prochaine passe. ».
  - `GET /ui/exclusions/export` → `text/yaml`, `Content-Disposition: attachment; filename="exclusions-rag.yaml"`.
  - `POST /ui/exclusions/import` (écrivain ; fichier `fichier`, 1 Mo au plus) → message « N ajoutées, M déjà présentes. » ou l'erreur de `MotifInvalide`.
  - `MotifInvalide` → 400 avec le message ; `SourceInconnue` → 404.

- [ ] **Step 1 : Tests qui échouent** :
```python
def test_apercu_puis_exclusion(client_priv): aperçu "02 - Domaines/**" → "2 documents" ; POST → documents retirés, ligne listée
def test_gros_motif_exige_la_seconde_confirmation(client_priv, bd):
    # 101 documents factices insérés dans coffre-maison ; motif "*" : aperçu montre la case ; POST sans confirme_gros → 400 ; avec → 200
def test_annuler(client_priv): ligne disparue, demande_le posé
def test_ecritures_refusees_au_jeton_public(client_pub): apercu, exclure, annuler, import → 403
def test_export_import_par_l_interface(client_priv): export → attachment YAML ; import du même fichier → "0 ajoutées, 1 déjà présentes."
def test_import_malforme(client_priv): "pas une liste" → 400, aucune exclusion créée
def test_import_trop_gros(client_priv): 1 Mo + 1 octet → 413
```
- [ ] **Step 2 : Lancer** → FAIL.
- [ ] **Step 3 : Implémenter.**
- [ ] **Step 4 : Lancer** `uv run pytest tests/test_ui.py -v` → PASS.
- [ ] **Step 5 : Commit** `feat(ui): exclusions (aperçu, exclure, annuler, export, import)`

---

### Task 9 : README, contrat et vérification de bout en bout

**Files :**
- Modify : `README.md`

- [ ] **Step 1 : README**
  - Section « Gestion de l'index » : adresse `http://192.168.2.40:8000/ui/` (depuis le MacBook), connexion par jeton, ce que voit `RAG_JETON` et ce que permet `RAG_JETON_PRIVE`, outils MCP `rag_sources` et `rag_fichiers`.
  - Section « Sauvegarde » : les exclusions manuelles sont la seule donnée irremplaçable de la base ; exporter après chaque changement (bouton ou `docker --context colima exec rag-rag-1 rag exclusions exporter > exclusions-rag.yaml`), réimporter après un `colima delete`.
  - Tableau de contrat, ligne « Niveaux de visibilité » marquée **(changé)** : `RAG_JETON_PRIVE` → `public-maison` + `prive` **et gestion de l'index par l'interface `/ui/`** ; ligne MCP : sept outils.
- [ ] **Step 2 : Vérifier**
  Run : `uv run ruff check . && uv run pytest -m "not vivant and not lent"`
  Expected : tout passe.
  Puis lancer l'app en local (`uv run rag servir` avec `RAG_CONFIG=config/dev.yaml` sur la base de dev indexée), ouvrir `http://localhost:8000/ui/` dans le navigateur, se connecter, parcourir Sources → une source → un fichier, exclure puis annuler un fichier, exporter.
- [ ] **Step 3 : Commit** `docs: gestion de l'index (interface, MCP, sauvegarde des exclusions, contrat)`
