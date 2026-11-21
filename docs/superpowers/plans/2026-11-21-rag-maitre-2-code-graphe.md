# RAG maître — plan 2 : code des dépôts et graphe de connaissances

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ajouter le code des dépôts `nicolaslallier/*` (`origin/main`) à l'index, charger un graphe (graphify + `[[liens]]`) dans le même Postgres, offrir `chemin` et `expliquer`, et prouver au banc que le coffre ne régresse pas.

**Architecture:** Un troisième lecteur (`LecteurGit`) garde un clone par dépôt ; le code est découpé par symbole avec tree-sitter. Le graphe vit dans deux tables (`noeuds`, `liens`) reliées aux documents ; graphify en est l'extracteur structurel (plan A) ou tree-sitter (plan B), selon la décision du spike. L'extraction sémantique tourne à part, par modèle local.

**Tech Stack:** celle du plan 1, plus `tree-sitter-language-pack`, `git` en sous-processus, `graphifyy` (plan A).

**Spec:** `docs/superpowers/specs/2026-11-21-rag-maitre-design.md`. Prérequis : plan 1 terminé ; lire `docs/superpowers/spikes/2026-11-graphify.md` (décision A ou B, forme du JSON) avant la tâche 3.

## Global Constraints

- Toutes celles du plan 1 (rien ne sort du homelab, NFC, config versionnée, dépôt privé, lecture seule du coffre).
- Code : **`origin/main` seulement**, jamais d'autre branche ni de worktree. Les clones appartiennent au service (`<RAG_DONNEES_DIR>/clones/<nom>`), jamais les copies de travail de Nicolas dans `~/Code`.
- Tout est indexé sauf : fichiers générés, verrous, binaires, fichiers qui ressemblent à des secrets (`rag/exclusions.py`), et `banc/**` du dépôt RAG.
- **Une seule base** : le graphe est dans Postgres. Pas de Neo4j, pas de FalkorDB, pas de `graph.json` servi comme source de vérité (c'est un fichier intermédiaire).
- **Visibilité** : toute lecture du graphe (`trouver_noeuds`, `chemin`, `expliquer`, expansion, doublons, orphelines, liens entrants) prend `niveaux: frozenset[str]` obligatoire et ne voit que les nœuds dont la source a un de ces niveaux ; un chemin ne traverse jamais un nœud invisible pour l'appelant.
- Dans les tests de ce plan, un appel écrit sans `niveaux` s'entend avec `niveaux=PUBLIC` ; le code, lui, n'a pas de valeur par défaut.
- Confiance des liens : `EXTRAIT`, `INFERE`, `AMBIGU` (traduction de `EXTRACTED`, `INFERRED`, `AMBIGUOUS`).
- graphify ne doit jamais voir `GEMINI_API_KEY` ni `GOOGLE_API_KEY` : ces variables sont retirées de l'environnement du sous-processus.
- Le jeton GitHub ne passe jamais par un argument de commande ni par une URL ; il n'apparaît dans aucun message d'erreur ni journal.
- L'expansion par le graphe reste **désactivée par défaut** ; elle ne s'active dans `config/rag.yaml` que si le banc la valide.

## Review Focus

1. **Fichier de code illisible pour l'analyseur** (erreur de syntaxe, langage non pris en charge comme `.vue`, fichier vide) : découpé en récursif, jamais d'exception. → test tâche 1.
2. **Le dépôt RAG s'indexe lui-même** : sans exclusion, `banc/etalon.yaml` serait indexé et chaque question étalon se retrouverait elle-même. → test tâche 2 sur `config/rag.yaml`.
3. **Fichier secret dans un dépôt** (`.env.example`, clé dans un YAML, `seal.key`) : jamais en base, compté dans `exclus`. → test tâche 2.
4. **graphify échoue ou dépasse le délai sur un dépôt** : l'index texte de ce dépôt est quand même à jour, l'ancien graphe reste, l'erreur est rapportée, les autres dépôts continuent. → test tâche 3.
5. **Nom ambigu ou inconnu pour `chemin` / `expliquer`** (« config » correspond à 40 nœuds) : la réponse liste les candidats, elle ne choisit pas au hasard et ne rend pas une erreur 500. → test tâche 5.

---

### Task 1: Découpage du code par symbole

**Files:**
- Create: `rag/decoupage_code.py`
- Modify: `pyproject.toml` (dépendance `tree-sitter-language-pack`)
- Test: `tests/test_decoupage_code.py`

**Interfaces:**
- Consumes: `Morceau`, `decouper_recursif`, `compter_tokens`, `IndexationConf` (plan 1).
- Produces:
  - `langage(chemin: str) -> str` : `python` (`.py`), `rust` (`.rs`), `typescript` (`.ts`, `.tsx`), `javascript` (`.js`, `.mjs`, `.jsx`), `vue`, `yaml`, `markdown`, `shell`, `dockerfile` (nom `Dockerfile*`), `json`, `toml`, `sql`, sinon `autre`.
  - `decouper_code(depot: str, chemin: str, texte: str, conf: IndexationConf) -> list[Morceau]`.
    - `python`, `rust`, `typescript`, `javascript` : un morceau par symbole de premier niveau — Python : `function_definition`, `class_definition` (avec ses décorateurs) ; Rust : `function_item`, `struct_item`, `enum_item`, `trait_item`, `impl_item` ; TS/JS : `function_declaration`, `class_declaration`, et `lexical_declaration` exportée dont la valeur est une fonction. `symbole` = nom (`impl Foo for Bar` → `Bar`), `ligne` = première ligne (base 1), `fil` = `"<depot> > <chemin> > <symbole>"`.
    - Une classe ou un `impl` de plus de `plafond_tokens` est éclaté : un morceau d'en-tête (jusqu'à la première méthode, symbole = la classe) puis un morceau par méthode, `symbole = "Classe.methode"`. Un symbole encore trop long passe par `decouper_recursif` en gardant `fil`, `symbole`, `ligne`.
    - Le reste du fichier (imports, code de module), s'il fait au moins 20 tokens : un morceau `fil = "<depot> > <chemin>"`, `symbole = None`, `ligne = 1`.
    - Tout autre langage, une erreur d'analyse (`root_node.has_error`), ou aucun symbole trouvé : `decouper_recursif("<depot> > <chemin>", texte, conf.recursif_tokens, conf.recouvrement)`.
    - Marquer `# ponytail: .vue en récursif ; analyser le bloc <script> si le banc le demande`.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
PY = '''import os

def aide():
    return 1

class Indexeur:
    """Doc."""
    def lancer(self):
        return aide()
'''

def test_python_par_symbole():
    m = decouper_code("RAG", "rag/indexeur.py", PY, CONF)
    assert [(x.symbole, x.ligne) for x in m if x.symbole] == [("aide", 3), ("Indexeur", 6)]
    assert m[0].fil == "RAG > rag/indexeur.py > aide" and "def aide" in m[0].texte

def test_grosse_classe_eclatee_en_methodes():
    src = "class C:\n    x = 1\n" + "".join(f"    def m{i}(self):\n" + "        a = 1\n" * 150 for i in range(4))
    s = [x.symbole for x in decouper_code("R", "c.py", src, CONF)]
    assert s[0] == "C" and {"C.m0", "C.m3"} <= set(s)

def test_rust_et_typescript():
    rs = decouper_code("Moria", "src/lib.rs", "pub struct Nain { pv: u32 }\nimpl Nain { pub fn creuser(&self) {} }\npub fn main() {}", CONF)
    assert {"Nain", "main"} <= {x.symbole for x in rs}
    ts = decouper_code("AI", "a.ts", "export const charger = async () => 1;\nexport function f() {}\nclass K {}", CONF)
    assert {"charger", "f", "K"} <= {x.symbole for x in ts}

@pytest.mark.parametrize("chemin,texte", [("a.py", "def cassé(:\n  ???"), ("App.vue", "<template><p/></template>\n" * 30),
                                          ("x.yaml", "a: 1\n" * 50), ("Dockerfile", "FROM x\nRUN y\n"), ("vide.py", ""), ("LISEZ.md", "# T\n\ntexte " * 40)])
def test_repli_recursif_sans_exception(chemin, texte):
    m = decouper_code("R", chemin, texte, CONF)
    assert all(x.symbole is None and x.fil == f"R > {chemin}" for x in m)
    assert (m == []) == (texte.strip() == "")

def test_langage():
    assert [langage(c) for c in ("a.py", "b.rs", "c.tsx", "Dockerfile.dev", "d.vue", "e.bin")] == ["python", "rust", "typescript", "dockerfile", "vue", "autre"]
```

- [ ] **Step 2: Lancer** `uv add tree-sitter-language-pack && uv run pytest tests/test_decoupage_code.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter** `rag/decoupage_code.py` (`tree_sitter_language_pack.get_parser(nom)`).
- [ ] **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "feat: découpage du code par symbole (tree-sitter), repli récursif"`

---

### Task 2: Lecteur git et dépôts de code dans l'indexeur

**Files:**
- Modify: `rag/sources.py` (`LecteurGit`, `lecteur_pour`), `rag/indexeur.py` (branche `code`), `rag/config.py` (`Reglages.github_jeton: str = ""`), `config/rag.yaml`, `config/dev.yaml`
- Test: `tests/test_sources_git.py`, `tests/test_indexeur_code.py`

**Interfaces:**
- Consumes: `decouper_code`, `langage`, `motif_exclusion`.
- Produces:
  - `class LecteurGit(url: str, dest: Path, branche: str = "main", jeton: str = "")`, même interface que les autres lecteurs, plus `synchroniser() -> str` :
    - `synchroniser()` : `git clone --single-branch --branch <branche> <url> <dest>` si absent ; sinon `git fetch origin <branche>` puis `git reset --hard origin/<branche>` (une réécriture d'historique en amont est donc suivie). Rend le sha de `HEAD`. Appelé par `lister()` s'il ne l'a pas été.
    - Jeton : passé par l'environnement (`GIT_CONFIG_COUNT=1`, `GIT_CONFIG_KEY_0=http.extraheader`, `GIT_CONFIG_VALUE_0="AUTHORIZATION: basic <base64 de 'x-access-token:JETON'>"`), `GIT_TERMINAL_PROMPT=0`. Une erreur git devient `ErreurSource` dont le message a le jeton et sa forme base64 remplacés par `***`.
    - `lister()` : `git ls-tree -r -z --long HEAD` → `Entree(chemin NFC, signature=<sha du blob>, modifie_le=datetime.min UTC, taille)` ; les sous-modules (mode `160000`) sont ignorés. `lire(chemin)` lit le fichier de la copie de travail. `revision()` = sha de `HEAD`.
  - `lecteur_pour` : `type == "code"` → `LecteurGit(source.emplacement, reglages.donnees_dir / "clones" / source.nom, source.branche, reglages.github_jeton)`.
  - `indexer_source`, pour une source `code` : `titre = chemin`, `titre_norm = normaliser(chemin)`, `type = langage(chemin)`, `tags = []`, `frontmatter = {}`, `origine = None`, morceaux par `decouper_code(source.nom, chemin, texte, conf)` ; `symbole` et `ligne` stockés ; poids A du tsvector = `fil` (qui contient chemin et symbole). Le reste de l'algorithme du plan 1 est inchangé (la signature étant le sha du blob, un fichier inchangé n'est jamais relu).
- `config/rag.yaml`, à ajouter sous `sources` (le coffre git `obsidian-maison` n'y est pas : le coffre est lu dans S3) :

```yaml
  - {nom: AI,           type: code, emplacement: "https://github.com/nicolaslallier/AI.git"}
  - {nom: Infra,        type: code, emplacement: "https://github.com/nicolaslallier/Infra.git"}
  - {nom: Jarvis,       type: code, emplacement: "https://github.com/nicolaslallier/Jarvis.git"}
  - {nom: EA,           type: code, emplacement: "https://github.com/nicolaslallier/EA.git"}
  - {nom: DarkAngel,    type: code, emplacement: "https://github.com/nicolaslallier/DarkAngel.git"}
  - {nom: Final,        type: code, emplacement: "https://github.com/nicolaslallier/Final.git"}
  - {nom: Glee,         type: code, emplacement: "https://github.com/nicolaslallier/Glee.git"}
  - {nom: Heaven,       type: code, emplacement: "https://github.com/nicolaslallier/Heaven.git"}
  - {nom: Midas,        type: code, emplacement: "https://github.com/nicolaslallier/Midas.git"}
  - {nom: Moria,        type: code, emplacement: "https://github.com/nicolaslallier/Moria.git"}
  - {nom: orchestre,    type: code, emplacement: "https://github.com/nicolaslallier/orchestre.git"}
  - {nom: mcp-fichiers, type: code, emplacement: "https://github.com/nicolaslallier/mcp-fichiers.git"}
  - {nom: RAG,          type: code, emplacement: "https://github.com/nicolaslallier/RAG.git", exclure: ["banc/**"]}
```

Vérifier la liste avant de l'écrire : `gh repo list nicolaslallier --limit 100 --json name,isArchived,defaultBranchRef` ; tout dépôt non archivé absent de la liste est ajouté (sauf `obsidian-maison`), une branche par défaut autre que `main` va dans `branche:`. Le dépôt RAG n'existe sur GitHub qu'après le plan 4 : sa ligne reste commentée jusque-là. `config/dev.yaml` reçoit les mêmes lignes.

- [ ] **Step 1: Écrire les tests qui échouent** (dépôt git local créé dans `tmp_path` par une fixture `depot` : `main` avec `a.py`, `.env.example`, `uv.lock`, `logo.png`, `docs/guide.md`, `gros.json` de 400 ko)

```python
def test_clone_puis_suit_origin_main(depot, tmp_path):
    l = LecteurGit(str(depot), tmp_path / "clone")
    sha1 = l.synchroniser(); assert {e.chemin for e in l.lister()} >= {"a.py", "docs/guide.md"}
    commit(depot, "b.py", "def b(): pass")
    l2 = LecteurGit(str(depot), tmp_path / "clone"); assert l2.synchroniser() != sha1 and "b.py" in {e.chemin for e in l2.lister()}

def test_suit_une_reecriture_d_historique(depot, tmp_path): ...     # git commit --amend en amont -> le clone suit, pas d'erreur
def test_une_autre_branche_n_est_jamais_lue(depot, tmp_path): ...   # fichier présent seulement sur `travail` -> absent du listage
def test_signature_est_le_sha_du_blob(depot, tmp_path): ...         # == `git rev-parse HEAD:a.py`
def test_le_jeton_ne_fuit_pas(tmp_path):
    with pytest.raises(ErreurSource) as e: LecteurGit("https://invalide.invalid/x.git", tmp_path / "c", jeton="ghp_secret123").synchroniser()
    assert "ghp_secret123" not in str(e.value) and base64.b64encode(b"x-access-token:ghp_secret123").decode() not in str(e.value)

def test_indexe_le_code_par_symbole(bd, depot, tmp_path, faux_modeles):
    b = indexer_source(bd, SRC_CODE, LecteurGit(str(depot), tmp_path / "c"), CONF, faux_modeles)
    assert b.exclus == 3                                             # .env.example, uv.lock, logo.png
    assert chemins(bd) == {"a.py", "docs/guide.md", "gros.json"}
    r = bd.execute("SELECT m.symbole, m.ligne, m.fil, d.type FROM morceaux m JOIN documents d ON d.id=m.document_id WHERE d.chemin='a.py' AND m.symbole IS NOT NULL").fetchone()
    assert r["fil"].startswith("depot-test > a.py > ") and r["type"] == "python" and r["ligne"] >= 1

def test_gros_fichier_un_seul_morceau(bd, depot, tmp_path, faux_modeles): ...   # gros.json : 1 morceau de 2000 caractères, trouvable par son chemin (flou)
def test_fichier_inchange_jamais_relu(bd, depot, tmp_path, faux_modeles): ...   # 2e passe : lecteur.lire n'est appelé pour aucun fichier
def test_recherche_filtre_par_depot(bd, ...): ...                               # Filtres(sources=["depot-test"]) ne rend que du code ; lien == "depot-test/a.py#L<n>"

def test_la_config_de_prod_exclut_le_banc_du_depot_rag():
    rag = next(s for s in charger_config(Path("config/rag.yaml")).sources if s.nom == "RAG")
    assert motif_exclusion("banc/etalon.yaml", b"- id: x", rag.exclure) == "config"
def test_le_coffre_git_n_est_pas_une_source():
    assert all("obsidian-maison" not in s.emplacement for s in charger_config(Path("config/rag.yaml")).sources)
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_sources_git.py tests/test_indexeur_code.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter.**
- [ ] **Step 4: Relancer** — attendu : SUCCÈS, et toute la suite du plan 1 reste verte.
- [ ] **Step 5: Indexer pour vrai** : `RAG_GITHUB_JETON=$(gh auth token) RAG_CONFIG=config/dev.yaml uv run rag indexer`. Noter par dépôt : fichiers, morceaux, exclus, durée. Passer en revue les `exclus` de raison `secret` (aucun faux négatif évident : `grep -rIl 'PRIVATE KEY\|sk-' donnees/clones | head` ne doit rien montrer qui soit en base).
- [ ] **Step 6: Commit** `git commit -am "feat: dépôts de code (clones origin/main) dans l'index"`

---

### Task 3: Tables du graphe et chargement de la structure

**Files:**
- Create: `rag/migrations/002_graphe.sql`, `rag/graphe.py`
- Modify: `rag/cli.py` (`rag indexer` met le graphe à jour ; `rag graphe [--source S]`), `pyproject.toml` (plan A : dépendance `graphifyy`)
- Test: `tests/test_graphe.py`

**Interfaces:**
- Consumes: `tests/fixtures/graphify/graph.json` et la forme consignée par le spike.
- Produces:

```sql
-- rag/migrations/002_graphe.sql
CREATE TABLE noeuds (
  id bigserial PRIMARY KEY,
  source_id int NOT NULL REFERENCES sources ON DELETE CASCADE,
  cle text NOT NULL,                 -- identifiant de graphify, chemin de la note, ou « concept:<nom> »
  libelle text NOT NULL,
  libelle_norm text NOT NULL,
  type text,                         -- note | fichier | fonction | classe | concept | …
  communaute int,
  document_id bigint REFERENCES documents ON DELETE SET NULL,
  ligne int,
  UNIQUE (source_id, cle)
);
CREATE TABLE liens (
  id bigserial PRIMARY KEY,
  de bigint NOT NULL REFERENCES noeuds ON DELETE CASCADE,
  vers bigint NOT NULL REFERENCES noeuds ON DELETE CASCADE,
  type text NOT NULL,
  confiance text NOT NULL CHECK (confiance IN ('EXTRAIT', 'INFERE', 'AMBIGU')),
  origine text NOT NULL CHECK (origine IN ('structure', 'wikilien', 'semantique')),
  UNIQUE (de, vers, type, origine)
);
CREATE INDEX ON liens (de);
CREATE INDEX ON liens (vers);
CREATE INDEX ON noeuds (document_id);
CREATE INDEX ON noeuds USING gin (libelle_norm gin_trgm_ops);
ALTER TABLE documents ADD COLUMN semantique_sha text;   -- sha256 du contenu au dernier passage sémantique
```

  - Forme intermédiaire unique (celle que rendent tous les extracteurs) : `{"noeuds": [{"cle", "libelle", "type", "fichier": str | None, "ligne": int | None, "communaute": int | None}], "liens": [{"de": cle, "vers": cle, "type", "confiance"}]}`.
  - `depuis_graphify(graphe: dict) -> dict` : traduit le `graph.json` de graphify vers la forme intermédiaire (correspondance des clés selon le compte rendu du spike ; confiance `EXTRACTED→EXTRAIT`, `INFERRED→INFERE`, `AMBIGUOUS→AMBIGU`, absente → `EXTRAIT` ; `ligne` tirée de la position `L42` ; un lien dont une extrémité manque est ignoré).
  - `extraire_structure(clone: Path, travail: Path, delai_s: int = 600) -> dict` (forme intermédiaire) :
    - **Plan A** : sous-processus `graphify update <clone>` avec la sortie sous `travail` (option ou répertoire courant notés par le spike), environnement sans `GEMINI_API_KEY` ni `GOOGLE_API_KEY`, délai `delai_s` ; lit le `graph.json`, passe par `depuis_graphify`. Code de sortie non nul, délai dépassé ou JSON absent → `ErreurGraphe`.
    - **Plan B** (si le spike a décidé B) : même signature, avec tree-sitter : un nœud par fichier et par symbole (ceux de `decouper_code`), liens `contient` (fichier → symbole, `EXTRAIT`), `importe` (fichier → fichier, quand l'import se résout dans le dépôt, `EXTRAIT`), `appelle` (symbole → symbole de même nom unique dans le dépôt, `INFERE` ; nom porté par plusieurs symboles → `AMBIGU`). Pas de communautés (`communaute = None`).
  - `charger_graphe(conn, source_nom: str, graphe: dict, origine: str = "structure") -> tuple[int, int]` (nœuds, liens), dans une transaction : supprime les liens de cette `origine` dont le nœud `de` est de la source ; upsert des nœuds (`document_id` par `fichier` = `documents.chemin` de la source) ; insère les liens ; supprime les nœuds de la source sans aucun lien et absents du nouveau graphe.
  - `maj_graphe(conn, source: SourceConf, lecteur, reglages) -> str` : pour `code`, si `sources.revision` a changé depuis le dernier chargement, `extraire_structure` puis `charger_graphe` ; pour `obsidian`, `charger_wikiliens` (tâche 4). Rend une ligne de bilan. **Une `ErreurGraphe` est attrapée ici** : journalisée, rendue dans le bilan (`"graphe: ÉCHEC <raison>"`), l'ancien graphe reste. Appelée par `rag indexer` après `indexer_source` de chaque source.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_depuis_graphify_sur_la_sortie_reelle():
    g = depuis_graphify(json.loads(Path("tests/fixtures/graphify/graph.json").read_text()))
    assert g["noeuds"] and g["liens"]
    assert {l["confiance"] for l in g["liens"]} <= {"EXTRAIT", "INFERE", "AMBIGU"}
    cles = {n["cle"] for n in g["noeuds"]}; assert all(l["de"] in cles and l["vers"] in cles for l in g["liens"])
    assert any(n["fichier"] for n in g["noeuds"])

def test_charger_relie_aux_documents(bd, index_code):
    n, l = charger_graphe(bd, "depot-test", G)                      # G : 3 nœuds dont un sur a.py, 2 liens
    assert (n, l) == (3, 2)
    assert bd.execute("SELECT d.chemin FROM noeuds n JOIN documents d ON d.id=n.document_id WHERE n.cle='a.py::aide'").fetchone()["chemin"] == "a.py"

def test_recharger_remplace_sans_doublon(bd, index_code):
    charger_graphe(bd, "depot-test", G); charger_graphe(bd, "depot-test", G_SANS_UN_NOEUD)
    assert compte(bd, "noeuds") == 2 and compte(bd, "liens") == 1

def test_recharger_la_structure_garde_les_autres_origines(bd, index_code): ...   # un lien origine 'wikilien' survit à charger_graphe(origine='structure')
def test_document_supprime_garde_le_noeud_sans_lien_mort(bd, index_code): ...    # DELETE document -> noeuds.document_id NULL, pas d'erreur

def test_echec_de_l_extracteur_garde_l_ancien_graphe(bd, index_code, monkeypatch):
    charger_graphe(bd, "depot-test", G)
    monkeypatch.setattr("rag.graphe.extraire_structure", lambda *a, **k: (_ for _ in ()).throw(ErreurGraphe("délai dépassé")))
    bilan = maj_graphe(bd, SRC_CODE, lecteur, REGLAGES)
    assert "ÉCHEC" in bilan and "délai dépassé" in bilan and compte(bd, "noeuds") == 3

def test_rag_indexer_continue_apres_un_echec_de_graphe(...): ...    # 2 dépôts, l'extracteur tombe sur le 1er : les deux sont indexés (texte), code de sortie 0, l'échec est affiché

def test_extraire_structure_sans_cle_infonuagique(monkeypatch, tmp_path):   # plan A
    monkeypatch.setenv("GEMINI_API_KEY", "x"); vu = {}
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: vu.update(k) or FauxProc(tmp_path))
    extraire_structure(tmp_path / "clone", tmp_path / "t")
    assert "GEMINI_API_KEY" not in vu["env"] and "GOOGLE_API_KEY" not in vu["env"] and vu["timeout"] == 600
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_graphe.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter** la migration et `rag/graphe.py` (branche A ou B selon le spike ; ne pas écrire les deux).
- [ ] **Step 4: Relancer** — attendu : SUCCÈS. Puis pour vrai : `RAG_GITHUB_JETON=$(gh auth token) RAG_CONFIG=config/dev.yaml uv run rag migrer && uv run rag graphe` ; noter nœuds et liens par dépôt, et la part des nœuds avec `document_id` (attendu : plus de 80 % ; sinon la correspondance des chemins est fausse).
- [ ] **Step 5: Commit** `git commit -am "feat: graphe dans Postgres, structure du code chargée à chaque commit"`

---

### Task 4: Liens `[[…]]` du coffre

**Files:**
- Modify: `rag/graphe.py`
- Test: `tests/test_wikiliens.py`

**Interfaces:**
- Consumes: `documents.liens_sortants`, `titre_norm`, `alias` (plan 1).
- Produces: `charger_wikiliens(conn, source_nom: str) -> tuple[int, int, int]` (nœuds, liens, cibles non résolues). Un nœud par note (`cle = chemin`, `libelle = titre`, `type = 'note'`, `document_id`). Résolution d'une cible, dans l'ordre : chemin exact (avec ou sans `.md`) ; titre égal après `normaliser` ; alias égal ; plusieurs notes du même titre → celle du même dossier que la note d'origine, sinon la cible est comptée non résolue. Lien `type = 'lien'`, `EXTRAIT`, `origine = 'wikilien'`. Passe par `charger_graphe(..., origine="wikilien")`.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_liens_resolus_par_titre_alias_et_chemin(bd, coffre_lie, faux_modeles):
    # a.md: [[B]] [[Mon alias]] [[dossier/c]] [[Absente]]
    n, l, non = charger_wikiliens(bd, "coffre-maison")
    assert (n, l, non) == (4, 3, 1)
def test_sans_accents_ni_casse(bd, ...): ...            # [[pate chinois]] -> « Pâté chinois.md »
def test_homonymes_meme_dossier_d_abord(bd, ...): ...
def test_note_supprimee_retire_son_noeud(bd, ...): ...  # réindexer sans b.md puis recharger -> 3 nœuds, plus de lien vers B
def test_idempotent(bd, ...): ...                       # deux appels -> mêmes comptes
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_wikiliens.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter.**
- [ ] **Step 4: Relancer** — attendu : SUCCÈS. Pour vrai : `uv run rag graphe --source coffre-maison` ; relire la liste des cibles non résolues (affichée) avec le coffre sous les yeux : des liens vers des notes pas encore créées sont normaux, un lien vers une note existante est un bogue.
- [ ] **Step 5: Commit** `git commit -am "feat: liens [[…]] du coffre dans le graphe"`

---

### Task 5: `chemin(A, B)` et `expliquer(X)`

**Files:**
- Modify: `rag/graphe.py`, `rag/api.py`, `rag/cli.py`
- Test: `tests/test_chemin_expliquer.py`

**Interfaces:**
- Produces:
  - Toutes les fonctions ci-dessous prennent en plus `*, niveaux: frozenset[str]` (obligatoire), appliqué par `JOIN sources s … WHERE s.visibilite = ANY(%(niveaux)s)` sur chaque nœud lu, y compris les nœuds intermédiaires d'un chemin.
  - `trouver_noeuds(conn, nom: str, limite: int = 8) -> list[dict]` (`id, libelle, type, source, chemin`) : égalité sur `libelle_norm` ou sur `cle` d'abord ; sinon `similarity(libelle_norm, normaliser(nom)) >= 0.5`, meilleurs d'abord.
  - `chemin(conn, a: str, b: str, max_sauts: int = 6) -> dict` : `{"etat": "ok" | "ambigu" | "introuvable" | "aucun_chemin", "candidats": {"a": [...], "b": [...]}, "etapes": [{"noeud": {...}, "lien": {"type", "confiance", "sens"} | None}]}`. `ambigu` dès qu'un des deux noms a plus d'un nœud exact, ou aucun exact et plusieurs approchés ; `introuvable` si un nom n'a aucun candidat. Parcours en largeur **non orienté**, niveau par niveau (une requête par niveau sur `liens WHERE de = ANY(%s) OR vers = ANY(%s)`, ensemble des visités en mémoire), arrêt à `max_sauts`. Un appelant peut lever l'ambiguïté en passant `"id:<n>"`.
  - `expliquer(conn, x: str, limite: int = 20) -> dict` : `{"etat", "candidats", "noeud": {..., "lien": <lien_source du document>}, "communaute": {"id", "taille", "membres": [10 libellés les plus liés]} | None, "voisins": [{"libelle", "type", "lien_type", "confiance", "sens": "sortant" | "entrant", "lien"}], "n_voisins": int}` ; voisins triés `EXTRAIT` avant `INFERE` avant `AMBIGU`.
  - API : `POST /chemin {"de": str, "vers": str}` et `POST /expliquer {"nom": str}` (jeton requis, 200 dans tous les états). CLI : `rag chemin A B`, `rag expliquer X`.

- [ ] **Step 1: Écrire les tests qui échouent** (graphe de test : `A—B—C—D`, `E` isolé, deux nœuds libellés `config`)

```python
def test_plus_court_chemin_non_oriente(bd, graphe):
    r = chemin(bd, "A", "D"); assert r["etat"] == "ok" and [e["noeud"]["libelle"] for e in r["etapes"]] == ["A", "B", "C", "D"]
    assert chemin(bd, "D", "A")["etat"] == "ok" and r["etapes"][1]["lien"]["confiance"] == "EXTRAIT"
def test_aucun_chemin_et_limite_de_sauts(bd, graphe):
    assert chemin(bd, "A", "E")["etat"] == "aucun_chemin" and chemin(bd, "A", "D", max_sauts=2)["etat"] == "aucun_chemin"
def test_nom_ambigu_rend_les_candidats(bd, graphe):
    r = chemin(bd, "config", "A"); assert r["etat"] == "ambigu" and len(r["candidats"]["a"]) == 2 and r["etapes"] == []
    assert chemin(bd, f"id:{r['candidats']['a'][0]['id']}", "A")["etat"] in ("ok", "aucun_chemin")
def test_nom_inconnu(bd, graphe): assert chemin(bd, "zzzqqq", "A")["etat"] == "introuvable"
def test_faute_de_frappe_unique_acceptee(bd, graphe): ...     # un seul candidat approché -> utilisé, etat ok
def test_expliquer(bd, graphe):
    r = expliquer(bd, "B"); assert r["etat"] == "ok" and {v["libelle"] for v in r["voisins"]} == {"A", "C"} and r["n_voisins"] == 2
def test_expliquer_ambigu_et_inconnu(bd, graphe): ...         # "config" -> ambigu + candidats ; "zzz" -> introuvable
def test_un_chemin_ne_traverse_pas_une_source_privee(bd, graphe):      # B appartient à une source 'prive'
    assert chemin(bd, "A", "D", niveaux=PUBLIC)["etat"] == "aucun_chemin" and chemin(bd, "A", "D", niveaux=TOUT)["etat"] == "ok"
    assert chemin(bd, "B", "A", niveaux=PUBLIC)["etat"] == "introuvable"
    assert "B" not in {v["libelle"] for v in expliquer(bd, "A", niveaux=PUBLIC)["voisins"]}
def test_api_ne_rend_jamais_500(client, auth):
    for corps in ({"de": "config", "vers": "A"}, {"de": "zzz", "vers": "A"}, {"de": "", "vers": ""}):
        assert client.post("/chemin", json=corps, headers=auth).status_code in (200, 422)
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_chemin_expliquer.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter.**
- [ ] **Step 4: Relancer** — attendu : SUCCÈS. Pour vrai : `uv run rag expliquer "indexer_source"` et `uv run rag chemin "Portail" "LiteLLM"` rendent quelque chose de sensé.
- [ ] **Step 5: Commit** `git commit -am "feat: chemin et expliquer sur le graphe (API et CLI)"`

---

### Task 6: Expansion de la recherche par le graphe (interrupteur)

**Files:**
- Modify: `rag/recherche.py`
- Test: `tests/test_expansion.py`

**Interfaces:**
- Consumes: `RechercheConf.graphe` (`expansion`, `depuis`, `attenuation`, `plafond`), `noeuds`, `liens`.
- Produces: `etendre_par_graphe(conn, resultats: list[Resultat], vec: list[float], conf, version_id: int, *, niveaux: frozenset[str]) -> list[Resultat]`, appelée à la fin de `chercher` si `conf.graphe.expansion`. Pour les `depuis` premiers résultats : nœuds de leur document → voisins à 1 saut → documents de ces voisins absents des résultats → pour chacun, son morceau le plus proche de `vec` (cosinus), ajouté avec `score = score du résultat d'origine × attenuation`, `via = "graphe"`. Au plus `plafond` ajouts en tout ; liste finale triée par score. Les filtres de la requête s'appliquent aussi aux documents ajoutés.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_desactivee_par_defaut(bd, index_lie, faux_modeles):
    assert all(r.via == "hybride" for r in chercher(bd, "poulet", CONF.recherche, faux_modeles, V, M))
def test_ajoute_le_voisin_a_un_saut(bd, index_lie, faux_modeles):          # Poulet.md -[[lien]]-> Cuisine.md, qui ne contient pas « poulet »
    r = chercher(bd, "recette de poulet au cidre", AVEC_EXPANSION, faux_modeles, V, M, limite=20)
    v = next(x for x in r if x.chemin.endswith("Cuisine.md"))
    assert v.via == "graphe" and v.score == pytest.approx(r[0].score * 0.5)
def test_plafond_et_pas_de_doublon(bd, index_lie, faux_modeles): ...       # 9 voisins, plafond 5 -> 5 ajouts ; un voisin déjà trouvé n'est pas ajouté deux fois
def test_deux_sauts_non_suivis(bd, index_lie, faux_modeles): ...
def test_respecte_les_filtres(bd, index_lie, faux_modeles): ...            # Filtres(type="recette") -> aucun ajout d'un autre type
def test_n_ajoute_rien_d_une_source_privee(bd, index_lie, faux_modeles): ... # le voisin est dans une source 'prive', niveaux=PUBLIC -> pas d'ajout
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_expansion.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter.**
- [ ] **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "feat: expansion à un saut par le graphe, désactivée par défaut"`

---

### Task 6b: Doublons, notes orphelines, liens entrants (pour agent-doc, projet 2)

**Files:**
- Create: `rag/entretien.py`
- Modify: `rag/api.py`, `rag/cli.py`
- Test: `tests/test_entretien.py`

**Interfaces:**
- Consumes: `morceaux.embedding`, `noeuds`, `liens` d'origine `wikilien`.
- Produces (toutes avec `*, niveaux: frozenset[str]` obligatoire, sources `obsidian` seulement) :
  - `doublons(conn, version_id: int, seuil: float = 0.92, limite: int = 50, *, niveaux) -> list[dict]` : paires `{"a": {cle, titre, lien}, "b": {...}, "similarite": float}` de notes dont la similarité cosinus des vecteurs moyens (`avg(embedding)` des morceaux de la version) est ≥ `seuil`, plus toute paire de même `sha256` (similarité 1.0), triées par similarité décroissante ; chaque paire une seule fois (`a.id < b.id`). Marquer `-- ponytail: jointure n², correcte jusqu'à quelques milliers de notes ; passer par l'index HNSW au-delà`.
  - `orphelines(conn, *, niveaux) -> list[dict]` (`cle, titre, lien`) : notes sans aucun lien `wikilien`, ni entrant ni sortant.
  - `liens_entrants(conn, cle: str, *, niveaux) -> dict` : `{"etat": "ok" | "introuvable", "note": {...}, "entrants": [{cle, titre, lien}]}` pour la note `"<source>:<chemin>"`.
  - API (jeton requis) : `GET /doublons?seuil=0.92&limite=50` (`seuil` borné à [0.5, 1], `limite` à 200), `GET /orphelines`, `GET /liens-entrants?cle=…`. CLI : `rag doublons [--seuil S]`, `rag orphelines`, `rag liens-entrants CLE`.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_doublons(bd, index_doublons, faux_modeles):      # a.md et a-copie.md au même texte, b.md proche, c.md sans rapport
    d = doublons(bd, V, niveaux=PUBLIC)
    assert {frozenset((x["a"]["cle"], x["b"]["cle"])) for x in d if x["similarite"] == 1.0} == {frozenset(("coffre-maison:a.md", "coffre-maison:a-copie.md"))}
    assert all(x["similarite"] >= 0.92 for x in d) and all("c.md" not in (x["a"]["cle"] + x["b"]["cle"]) for x in d)
    assert len(doublons(bd, V, seuil=0.5, niveaux=PUBLIC)) > len(d)
def test_doublons_ignore_le_code(bd, index_mixte): ...     # deux fichiers de code identiques ne sont pas des doublons de notes
def test_orphelines_et_liens_entrants(bd, coffre_lie):     # a -> b, c seule
    assert [o["cle"] for o in orphelines(bd, niveaux=PUBLIC)] == ["coffre-maison:c.md"]
    r = liens_entrants(bd, "coffre-maison:b.md", niveaux=PUBLIC); assert r["etat"] == "ok" and [e["cle"] for e in r["entrants"]] == ["coffre-maison:a.md"]
    assert liens_entrants(bd, "coffre-maison:absente.md", niveaux=PUBLIC)["etat"] == "introuvable"
def test_source_privee_invisible(bd, coffre_lie):
    bd.execute("UPDATE sources SET visibilite='prive'")
    assert doublons(bd, V, niveaux=PUBLIC) == [] and orphelines(bd, niveaux=PUBLIC) == []
    assert liens_entrants(bd, "coffre-maison:b.md", niveaux=PUBLIC)["etat"] == "introuvable"
def test_api_bornes(client, auth):
    assert client.get("/doublons?seuil=0.1", headers=auth).status_code == 422 and client.get("/orphelines").status_code == 401
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_entretien.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter.**
- [ ] **Step 4: Relancer** — attendu : SUCCÈS. Pour vrai : `uv run rag doublons` et `uv run rag orphelines` sur le coffre, relire les 10 premières paires.
- [ ] **Step 5: Commit** `git commit -am "feat: doublons, notes orphelines et liens entrants (API et CLI)"`

---

### Task 7: Extraction sémantique (passe de nuit)

**Files:**
- Create: `rag/semantique.py`
- Modify: `rag/cli.py` (`rag graphe-semantique [--source S] [--max N]`)
- Test: `tests/test_semantique.py`

**Interfaces:**
- Consumes: `charger_graphe`, `Modeles.generer`, `documents.semantique_sha`.
- Produces:
  - `mentions_de_code(conn, document_id: int, texte: str) -> list[dict]` (sans modèle) : liens `mentionne`, `EXTRAIT`, du nœud de la note vers un nœud de code quand le texte contient, en mot entier, le nom d'une source `code`, un chemin de fichier indexé, ou le libellé d'un symbole d'au moins 6 caractères porté par un seul nœud. Libellé porté par plusieurs nœuds → un lien `AMBIGU` vers chacun, au plus 3.
  - `concepts_de_note(modeles, modele: str, titre: str, texte: str) -> list[dict]` :
    - **si le spike dit `sémantique : graphify`** : appelle `graphify.llm.extract_corpus_parallel` avec le backend et l'URL LiteLLM notés par le spike, puis `depuis_graphify` ;
    - **sinon (`sémantique : maison`)** : `modeles.generer(modele, …, temperature=0, json_mode=True)` avec la consigne « liste au plus 8 concepts, outils, personnes ou projets dont parle cette note ; réponds `{"concepts": [{"nom": "…", "relation": "concerne" | "utilise" | "decrit"}]}` » ; toute réponse hors forme → `[]` ; un `nom` de moins de 3 ou de plus de 60 caractères est écarté.
    - Dans les deux cas : nœud `cle = "concept:" + normaliser(nom)`, `type = 'concept'`, rattaché à la source du coffre ; lien note → concept, `INFERE`, `origine = 'semantique'`.
  - `passe_semantique(conn, config: Config, modeles, source_nom: str | None = None, maximum: int | None = None) -> dict` (`traites`, `liens`, `echecs`) : pour chaque document `obsidian` où `semantique_sha IS DISTINCT FROM sha256` : remplace ses liens d'origine `semantique` (mentions + concepts), puis pose `semantique_sha = sha256` ; un échec du modèle sur une note est compté et la passe continue (la note sera reprise la nuit suivante). Modèle : `config.reponse.modele` (gemma, local).

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_mentions_exactes_sans_modele(bd, index_mixte):
    l = mentions_de_code(bd, DOC_NOTE, "Le déploiement passe par indexer_source dans le dépôt RAG, voir litellm/config.yaml.")
    assert {(x["type"], x["confiance"]) for x in l} == {("mentionne", "EXTRAIT")} and len(l) == 3
def test_mot_entier_seulement(bd, index_mixte): ...          # « indexer_sources_bis » ne mentionne pas indexer_source ; un symbole de 4 lettres est ignoré
def test_symbole_homonyme_ambigu(bd, index_mixte): ...       # « create_app » existe dans 2 dépôts -> 2 liens AMBIGU
def test_concepts_reponse_invalide_donne_rien():
    assert concepts_de_note(FauxGenerateur(["pas json"]), "g", "T", "x") == []
    assert concepts_de_note(FauxGenerateur(['{"concepts": [{"nom": "x", "relation": "concerne"}, {"nom": "Grafana", "relation": "utilise"}]}']), "g", "T", "x")[0]["nom"] == "Grafana"
def test_passe_incrementale(bd, index_mixte):
    m = FauxGenerateur(repete='{"concepts": [{"nom": "Grafana", "relation": "utilise"}]}')
    assert passe_semantique(bd, CONFIG, m)["traites"] == N_NOTES
    n = len(m.appels); assert passe_semantique(bd, CONFIG, m)["traites"] == 0 and len(m.appels) == n
def test_note_modifiee_remplace_ses_liens(bd, index_mixte): ...
def test_echec_du_modele_n_arrete_pas_la_passe(bd, index_mixte): ...      # 1 note sur 3 lève ErreurModele -> echecs == 1, traites == 2, la note en échec garde semantique_sha NULL
def test_les_liens_de_structure_et_wikiliens_survivent(bd, index_mixte): ...
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_semantique.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter** (une seule des deux branches de `concepts_de_note`).
- [ ] **Step 4: Relancer** — attendu : SUCCÈS. Pour vrai : `uv run rag graphe-semantique --max 10`, relire les concepts des 10 notes ; puis la passe complète, noter la durée (elle doit tenir dans la nuit : moins de 2 h).
- [ ] **Step 5: Commit** `git commit -am "feat: extraction sémantique de nuit par modèle local (mentions de code, concepts)"`

---

### Task 8: Jeu étalon du code, non-régression du coffre, mesure de l'expansion

Tâche de données et de mesure. **Attente : Nicolas corrige les nouvelles questions.**

**Files:**
- Modify: `banc/etalon.yaml`, `docs/mesures.md`, `banc/reference.json`

- [ ] **Step 1: Non-régression d'abord, sans toucher au jeu.** `RAG_CONFIG=config/dev.yaml uv run rag banc --etiquette avec-code`. Le jeu ne contient encore que des questions du coffre : comparer à la référence du plan 1. Attendu : rappel@10 et MRR ne baissent pas de plus de 0,02. Sinon, consigner dans `docs/mesures.md` les cas perdus et leur cause (un fichier de code qui passe devant une note), et ne rien masquer : le remède (un poids par type de source dans la fusion) est un nouveau levier à proposer à Nicolas avant de l'écrire.
- [ ] **Step 2: Ajouter 12 à 15 questions sur le code**, `statut: propose`, mêmes règles que le plan 1 (tâche 11). Obligatoires, mot pour mot :
  - `id: architecture-ai`, « Quelle est l'architecture de mon projet AI ? », `nature: synthetiser`, `mode_attendu: repondre`, `attendus` = les notes du coffre qui décrivent le projet **et** les fichiers clés du dépôt `AI` (`AI:README.md`, `AI:agents.yaml`, `AI:deploy/portainer-stack.yml`, plus ceux que la lecture du dépôt désigne) ;
  - `id: dashboards-grafana`, « Quelles sont mes dashboards Grafana ? », `nature: lister`, `mode_attendu: lister`, `attendus` = chaque `Infra:monitoring/grafana/provisioning/dashboards/json/*.json`, les `AI:deploy/grafana/*.json`, et les notes du coffre qui en parlent.
  Le reste : au moins 3 questions à cheval coffre + code, 3 sur un symbole ou un fichier précis, 3 `sans_reponse` plausibles sur le code.
- [ ] **Step 3: Jeu synthétique du code** : `uv run rag banc-synthetique --n 100 --source AI --source Infra --source Jarvis` ; ajouter les cas à `banc/synthetique.jsonl` (le fichier passe à 300-350 cas) ; relire 15 cas.
- [ ] **Step 4: Commit** `git commit -am "banc: questions sur le code proposées, à corriger par Nicolas"` puis **ARRÊT** : remettre à Nicolas les questions ajoutées et le résultat du step 1.
- [ ] **Step 5: Après correction, reposer la référence** : `uv run rag banc --etiquette base-v2 --reference`.
- [ ] **Step 6: Mesurer l'expansion par le graphe** (un paramètre) : copie de la config avec `recherche.graphe.expansion: true`, `uv run rag banc --etiquette expansion --sans-indexer`, sur l'étalon puis le synthétique. Consigner dans `docs/mesures.md`. L'expansion reste à `false` dans `config/rag.yaml` sauf gain net (rappel@10 +0,03 sans perte de MRR) **et** accord de Nicolas.
- [ ] **Step 7: Commit** `git add -A && git commit -m "banc: référence v2 (coffre + code), mesure de l'expansion par le graphe"`

**Fin du plan 2 :** coffre et code indexés, graphe interrogeable, non-régression mesurée. Suite : plan 3.
