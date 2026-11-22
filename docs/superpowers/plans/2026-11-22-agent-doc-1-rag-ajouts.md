# agent-doc — plan 1 : ce qui manque à l'API du RAG maître

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Donner à agent-doc (et aux projets 3 et 4) les trois choses que le RAG maître n'a pas encore — le champ `origine`, l'état d'indexation d'un document, les liens cassés — et figer par un test tout ce qu'agent-doc consomme.

**Architecture:** Rien de neuf : deux fonctions de lecture dans `rag/entretien.py`, deux routes dans `rag/api.py`, une ligne dans l'analyse du frontmatter, une ligne de config. Aucune migration.

**Tech Stack:** celle du dépôt (uv, Python 3.13, FastAPI, psycopg 3 en SQL direct, pytest contre la base de dev en conteneur).

**Spec:** `docs/superpowers/specs/2026-11-21-agent-doc-design.md` (« Contrat avec le projet 1 »), et les ajouts demandés par `2026-11-21-jarvis-sans-rag-design.md` (état d'indexation par document) et `2026-11-21-redacteur-fine-tune-design.md` (`origine`). Contrat complet : `2026-11-22-agent-doc-0-index.md`.

**Prérequis :** plan 3 du projet 1 terminé (`POST /reindexer`, `GET /etat`, `POST /lister` existent). Déjà livrés, à ne pas refaire : `/doublons`, `/orphelines`, `/liens-entrants`, exclusion de `_versions/**`.

## Global Constraints

- `niveaux` reste un paramètre **obligatoire** (mot-clé seul) de toute fonction qui lit des documents ; une source hors des niveaux du jeton est invisible, sans message qui révèle son existence.
- Toute route exige un jeton de lecture (`RAG_JETON` ou `RAG_JETON_PRIVE`) ; le jeton de réindexation ne lit rien.
- Changer `rag/notes.py` impose d'incrémenter `VERSION_ANALYSEUR` (`rag/config.py`) : toute la base est réanalysée à la passe suivante.
- Clé d'une note : `<nom de source>:<chemin>` (ex. `coffre-maison:03 - Ressources/Poulet au cidre.md`).
- Nom du journal, exact : `Journal du documentaliste.md`, à la racine du coffre.

## Review Focus

1. **Note d'avant, avec `source: https://…` et pas d'`origine`** : l'origine citée reste l'URL (aucune régression des réponses « avec les sources »). → test tâche 1.
2. **Note d'agent-doc, avec `source: agent-musique` et `origine: https://…`** : l'origine citée est l'URL, jamais le nom de l'agent. → test tâche 1.
3. **`GET /document` sur une note d'une source `prive` avec le jeton public** : `absent`, comme une note qui n'existe pas. → test tâche 2.
4. **Note indexée mais sans morceau dans la version courante** (changement de config d'indexation en cours) : `etat: "indexe"`, `morceaux: 0` — l'appelant voit que la recherche ne la trouvera pas encore. → test tâche 2.
5. **Lien `[[Nom|alias]]`, `[[Nom#section]]` ou vers un homonyme d'un autre dossier** : n'est pas compté comme cassé s'il se résout comme dans `charger_wikiliens`. → test tâche 3.

---

### Task 1: Champ `origine` du frontmatter

**Files:**
- Modify: `rag/notes.py` (`analyser_note`), `rag/config.py` (`VERSION_ANALYSEUR`)
- Test: `tests/test_notes.py`

**Interfaces:**
- Produces: `Note.origine` = `str(fm["origine"])` si le frontmatter porte `origine` non vide, sinon `str(fm["source"])` si présent, sinon `None`. Rien d'autre ne change (`documents.origine`, `Resultat.origine`, les `origine` de `/lister` et `/repondre` suivent).

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_origine_prioritaire_sur_source():
    n = analyser_note("a.md", "---\nsource: agent-musique\norigine: https://exemple.org/x\n---\ncorps")
    assert n.origine == "https://exemple.org/x" and n.frontmatter["source"] == "agent-musique"

def test_origine_retombe_sur_source():
    assert analyser_note("a.md", "---\nsource: https://exemple.org/y\n---\nc").origine == "https://exemple.org/y"
    assert analyser_note("a.md", "---\nsource: claude\norigine: ''\n---\nc").origine == "claude"
    assert analyser_note("a.md", "corps sans frontmatter").origine is None
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_notes.py -k origine -v` — attendu : ÉCHEC sur le premier test.
- [ ] **Step 3: Implémenter** dans `analyser_note` ; incrémenter `VERSION_ANALYSEUR` de 1 (lire la valeur courante, le plan 3 du projet 1 a pu la changer).
- [ ] **Step 4: Relancer** `uv run pytest -q` — attendu : tout passe (les tests d'empreinte qui figent `VERSION_ANALYSEUR` s'adaptent s'il y en a).
- [ ] **Step 5: Commit** `git commit -am "feat: origine du frontmatter prioritaire sur source (source = qui a écrit)"`

---

### Task 2: État d'indexation d'un document (`GET /document`)

**Files:**
- Modify: `rag/entretien.py`, `rag/api.py`, `rag/cli.py`
- Test: `tests/test_entretien.py`, `tests/test_api.py`

**Interfaces:**
- Consumes: tables `documents`, `sources` (`visibilite`, `indexe_le`, `demande_le`), `morceaux`, `exclus` ; `version_id(conn, empreinte)` de `rag/api.py`.
- Produces:
  - `etat_document(conn, cle: str, version_id: int | None, *, niveaux: frozenset[str]) -> dict` dans `rag/entretien.py` :
    - `{"etat": "indexe", "cle", "titre", "sha256", "modifie_le": <ISO ou null>, "morceaux": <nombre dans version_id, 0 si None>, "source_indexee_le": <ISO ou null>, "demande_en_attente": bool}` ;
    - `{"etat": "exclu", "cle", "raison": "config" | "secret" | "genere" | "binaire"}` si la table `exclus` a la ligne ;
    - `{"etat": "absent", "cle"}` sinon — y compris source inconnue, source hors `niveaux`, clé sans `:`.
  - `GET /document?cle=<max 1000 car.>` (jeton de lecture) → ce dictionnaire, toujours 200.
  - CLI : `rag document <cle> [--prive]`, sortie JSON.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_etat_document(bd, tmp_path, faux_modeles):
    indexer_coffre(bd, tmp_path, faux_modeles, {"a.md": TEXTE})
    e = etat_document(bd, "coffre-maison:a.md", v(bd), niveaux=PUBLIC)
    assert e["etat"] == "indexe" and e["morceaux"] >= 1 and e["demande_en_attente"] is False
    assert e["sha256"] == hashlib.sha256(TEXTE.encode()).hexdigest()
    assert etat_document(bd, "coffre-maison:a.md", None, niveaux=PUBLIC)["morceaux"] == 0
    for cle in ("coffre-maison:absente.md", "inconnue:a.md", "sans-deux-points"):
        assert etat_document(bd, cle, v(bd), niveaux=PUBLIC) == {"etat": "absent", "cle": cle}

def test_etat_document_exclu_et_demande(bd, tmp_path, faux_modeles):
    indexer_coffre(bd, tmp_path, faux_modeles, {"a.md": TEXTE, "cle.md": "-----BEGIN PRIVATE KEY-----\nabc"})
    assert etat_document(bd, "coffre-maison:cle.md", v(bd), niveaux=PUBLIC)["raison"] == "secret"
    bd.execute("UPDATE sources SET demande_le = now()")
    assert etat_document(bd, "coffre-maison:a.md", v(bd), niveaux=PUBLIC)["demande_en_attente"] is True

def test_etat_document_respecte_les_niveaux(bd, tmp_path, faux_modeles):
    indexer_coffre(bd, tmp_path, faux_modeles, {"a.md": TEXTE})
    bd.execute("UPDATE sources SET visibilite = 'prive'")
    assert etat_document(bd, "coffre-maison:a.md", v(bd), niveaux=PUBLIC)["etat"] == "absent"
    assert etat_document(bd, "coffre-maison:a.md", v(bd), niveaux=TOUT)["etat"] == "indexe"

def test_route_document(client):                      # tests/test_api.py
    assert client.get("/document", params={"cle": "x:y"}).status_code == 401
    r = client.get("/document", params={"cle": "coffre-maison:inexistante.md"}, headers=auth)
    assert r.status_code == 200 and r.json()["etat"] == "absent"
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_entretien.py tests/test_api.py -k document -v` — attendu : ÉCHEC (`ImportError`).
- [ ] **Step 3: Implémenter** `etat_document`, la route et la commande CLI (sur le modèle de `liens_entrants` et de `route_liens_entrants`).
- [ ] **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "feat: état d'indexation par document (API et CLI)"`

---

### Task 3: Liens cassés (`GET /liens-casses`)

**Files:**
- Modify: `rag/graphe.py` (extraire la construction des index de résolution), `rag/entretien.py`, `rag/api.py`, `rag/cli.py`
- Test: `tests/test_entretien.py`, `tests/test_api.py`

**Interfaces:**
- Consumes: `_resoudre(cible, origine, par_chemin, par_titre, par_alias)` et la boucle de `charger_wikiliens` (`rag/graphe.py`).
- Produces:
  - `index_resolution(docs: list[dict]) -> tuple[dict, dict, dict]` dans `rag/graphe.py` : les trois dictionnaires `par_chemin`, `par_titre`, `par_alias` que `charger_wikiliens` construit aujourd'hui en ligne ; `charger_wikiliens` l'appelle (comportement inchangé, ses tests restent verts).
  - `liens_casses(conn, *, niveaux: frozenset[str]) -> list[dict]` dans `rag/entretien.py` : pour chaque source `obsidian` visible, chaque `(note, cible)` de `documents.liens_sortants` que `_resoudre` rend `None` ; éléments `{"note": {"cle", "titre", "lien"}, "cible": "<texte du lien>"}`, triés par clé puis cible. Calculé à la demande depuis `documents` (pas depuis l'attribut `charger_wikiliens.non_resolues`, qui ne survit pas au processus).
  - `GET /liens-casses` (jeton de lecture) → `{"liens": [...]}` ; CLI `rag liens-casses [--prive]`.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_liens_casses(bd, tmp_path, faux_modeles):
    indexer_coffre(bd, tmp_path, faux_modeles, {
        "a.md": "Voir [[b]], [[B|autre nom]], [[b#Section]], [[Fantôme]] et [[dossier/absent]].",
        "b.md": "# b", "x/c.md": "# c", "y/c.md": "# c", "x/d.md": "[[c]]"})
    r = liens_casses(bd, niveaux=PUBLIC)
    assert [(x["note"]["cle"], x["cible"]) for x in r] == [
        ("coffre-maison:a.md", "Fantôme"), ("coffre-maison:a.md", "dossier/absent")]
    bd.execute("UPDATE sources SET visibilite = 'prive'")
    assert liens_casses(bd, niveaux=PUBLIC) == []

def test_route_liens_casses(client):                  # tests/test_api.py
    assert client.get("/liens-casses").status_code == 401
    assert "liens" in client.get("/liens-casses", headers=auth).json()
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_entretien.py -k liens_casses -v` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter** : extraire `index_resolution`, écrire `liens_casses`, la route, la commande.
- [ ] **Step 4: Relancer** `uv run pytest -q` — attendu : SUCCÈS, `tests/test_wikiliens.py` compris.
- [ ] **Step 5: Commit** `git commit -am "feat: liens cassés du coffre (API et CLI)"`

---

### Task 4: Le journal du documentaliste hors de l'index

**Files:**
- Modify: `config/rag.yaml`, `config/dev.yaml` (si la source du coffre y est reprise)
- Test: `tests/test_config.py`

- [ ] **Step 1: Écrire le test qui échoue**

```python
def test_le_coffre_exclut_versions_et_journal():
    coffre = next(s for s in charger_config(Path("config/rag.yaml")).sources if s.type == "obsidian")
    for chemin in ("_versions/01 - Projets/a.md/20261122T030000Z.md", "_versions/_etat/examines.json",
                   "Journal du documentaliste.md"):
        assert motif_exclusion(chemin, b"", coffre.exclure) == "config", chemin
    assert motif_exclusion("01 - Projets/Journal du documentaliste.md", b"", coffre.exclure) is None
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_config.py -k journal -v` — attendu : ÉCHEC sur le journal.
- [ ] **Step 3: Ajouter** `"Journal du documentaliste.md"` à `exclure` de la source `coffre-maison`. Le journal change à chaque écriture d'agent-doc et ne contient que des traces : indexé, il sortirait en tête de toutes les recherches.
- [ ] **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "config: journal du documentaliste exclu de l'index"`

---

### Task 5: Test de contrat d'agent-doc, README, PR

**Files:**
- Create: `tests/test_contrat_agent_doc.py`
- Modify: `README.md`

**Interfaces:**
- Consumes: toutes les routes du tableau « agent-doc → RAG maître » de l'index.
- Produces: un test qui casse si une route ou un champ dont agent-doc dépend change de nom. C'est le seul garde-fou entre les deux dépôts.

- [ ] **Step 1: Écrire le test** (échoue tant qu'un champ manque ; reprendre les fixtures `client`, `auth` de `tests/test_api.py`, et celles du plan 3 pour un jeton de réindexation, `auth_reindexer`)

```python
CLE = "coffre-maison:Poulet au cidre.md"          # prendre une note qui existe dans la fixture `index`

def test_contrat_lecture(client):
    r = client.post("/chercher", json={"question": "poulet", "filtres": {"sources": ["coffre-maison"]}, "limite": 5}, headers=auth)
    assert {"cle", "chemin", "titre", "fil", "texte", "lien", "origine", "score", "cosinus"} <= set(r.json()["resultats"][0])
    e = client.post("/lister", json={"question": "recettes de poulet", "filtres": {}}, headers=auth).json()
    assert "total" in e and all({"cle", "titre", "lien", "origine", "certitude"} <= set(x) for x in e["elements"])
    assert "etat" in client.post("/expliquer", json={"nom": "inconnu-xyz", "limite": 5}, headers=auth).json()
    d = client.get("/doublons", params={"seuil": 0.92, "limite": 50}, headers=auth).json()["doublons"]
    assert all({"a", "b", "similarite"} <= set(x) and "cle" in x["a"] for x in d)
    assert all({"cle", "titre", "lien"} <= set(x) for x in client.get("/orphelines", headers=auth).json()["orphelines"])
    assert {"etat", "entrants"} <= set(client.get("/liens-entrants", params={"cle": CLE}, headers=auth).json())
    assert {"etat", "sha256", "modifie_le", "morceaux", "demande_en_attente"} <= set(
        client.get("/document", params={"cle": CLE}, headers=auth).json())
    assert "liens" in client.get("/liens-casses", headers=auth).json()

def test_contrat_reindexation_et_nuit(client):
    assert client.post("/reindexer", json={"source": "coffre-maison"}, headers=auth).status_code == 403
    r = client.post("/reindexer", json={"source": "coffre-maison"}, headers=auth_reindexer)
    assert r.status_code == 202 and r.json() == {"demande": ["coffre-maison"]}
    e = client.get("/etat", headers=auth).json()
    assert "en_cours" in e and "derniere_nocturne" in e      # null, ou {"debut", "fin", "ok", ...}
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_contrat_agent_doc.py -v` — attendu : SUCCÈS si les tâches 1 à 4 et le plan 3 du projet 1 sont faits ; un échec ici désigne le champ à corriger (dans le RAG, ou dans l'index du projet 2 si c'est le contrat qui a tort).
- [ ] **Step 3: Écrire** dans `README.md` une section « Ce qu'agent-doc consomme » : le tableau « agent-doc → RAG maître » de l'index, et la phrase « changer une de ces routes, c'est changer `agents/agent-doc/clients.py` dans l'Infra ».
- [ ] **Step 4: Vérifier pour vrai**, base de dev et coffre local indexés : `uv run rag document "coffre-maison:Accueil.md"` rend `indexe` avec `morceaux` ≥ 1 ; `uv run rag liens-casses` rend une liste plausible (regarder trois lignes à la main dans le coffre).
- [ ] **Step 5: Commit et PR en brouillon** `git commit -am "test: contrat des routes consommées par agent-doc ; README"` puis `gh pr create --draft`. Ne pas fusionner.
