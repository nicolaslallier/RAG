# Interface pleine largeur et visibilité « travail » — plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal :** l'interface `/ui/` occupe toute la largeur, et un troisième niveau de visibilité `travail`, visible seulement avec `RAG_JETON_PRIVE`, existe partout.

**Architecture :** une règle CSS en moins ; une valeur de plus dans la contrainte SQL de `sources.visibilite` (migration 007), dans le `Literal` et `TOUT` de `rag/config.py` et dans le menu du formulaire. Tout le filtrage passe déjà par `niveaux`, rien d'autre ne change.

**Tech Stack :** Python 3 / FastAPI / Jinja2 / htmx, Postgres (migrations SQL numérotées par `rag/db.py`), pytest, ruff. Lancer les tests avec `uv run pytest` (Postgres de test fourni par `tests/conftest.py`).

**Spec :** `docs/superpowers/specs/2026-11-23-visibilite-travail-design.md`

## Global Constraints

- La valeur s'écrit exactement `travail` (minuscules).
- `RAG_JETON` voit seulement `public-maison` ; `RAG_JETON_PRIVE` voit `public-maison`, `prive` et `travail`.
- La migration ne change la visibilité d'aucune source existante.
- Aucun nouveau jeton, aucun filtre par visibilité (non-buts).
- La carte de connexion (`.connexion`, `max-width: 26rem`) reste étroite.

## Review Focus

1. Passer une source existante de `prive` à `travail` par « Modifier » : enregistré, **aucune** vérification réseau ni réindexation (seuls dépôt/branche en déclenchent) → test en tâche 2.
2. Valeur inconnue (`pro`) postée au formulaire : refusée, rien n'est changé → test en tâche 2.
3. Le cookie public ouvre `/ui/sources/<nom>` d'une source `travail` : introuvable, comme pour `prive` → test en tâche 2.
4. Le jeton privé reste « écrivain » après l'agrandissement de `TOUT` (le test `niveaux == TOUT` de `rag/ui/__init__.py:137`) → test en tâche 2.
5. Export puis import d'une source `travail` : la valeur survit à l'aller-retour → test en tâche 2.

---

### Task 1 : interface pleine largeur

**Files :**
- Modify : `rag/ui/statique/rag.css` (règle `main`)
- Test : `tests/test_ui.py`

**Interfaces :** aucune.

- [ ] **Step 1 : écrire le test qui échoue** (à la fin de `tests/test_ui.py`)

```python
def test_main_occupe_toute_la_largeur():
    css = Path("rag/ui/statique/rag.css").read_text()
    regle_main = re.search(r"^main\s*\{([^}]*)\}", css, re.M).group(1)
    assert "max-width" not in regle_main
    assert re.search(r"^\.connexion\s*\{[^}]*max-width:\s*26rem", css, re.M)
```

(importer `re` et `Path` en tête si absents.)

- [ ] **Step 2 : lancer** `uv run pytest tests/test_ui.py::test_main_occupe_toute_la_largeur -v` → FAIL (`max-width` présent).
- [ ] **Step 3 : implémenter** — la règle devient `main { padding: 1rem; }` (retirer `max-width: 72rem` et `margin: 0 auto`).
- [ ] **Step 4 : lancer** `uv run pytest tests/test_ui.py -q` → tout PASS.
- [ ] **Step 5 : commit**

```bash
git add rag/ui/statique/rag.css tests/test_ui.py
git commit -m "feat(ui): l'interface occupe toute la largeur"
```

### Task 2 : niveau de visibilité `travail`

**Files :**
- Create : `rag/migrations/007_visibilite_travail.sql`
- Modify : `rag/config.py:17` (`TOUT`) et `rag/config.py:71` (`Literal` de `visibilite`)
- Modify : `rag/ui/gabarits/source_git.html:19` (tuple des options)
- Modify : `README.md`, section « Gestion de l'index » (puce `RAG_JETON_PRIVE` : voit aussi les sources `prive` **et `travail`**)
- Test : `tests/test_db.py`, `tests/test_config.py`, `tests/test_api.py`, `tests/test_mcp.py`, `tests/test_ui.py`, `tests/test_sources_gerees.py`

**Interfaces :**
- Produces : `rag.config.TOUT == frozenset({"public-maison", "prive", "travail"})` ; `PUBLIC` inchangé.

- [ ] **Step 1 : écrire les tests qui échouent**, en copiant le style des tests `prive` voisins (fixtures `bd`, `index`, `client`, `gc`, `appels`, helpers `connecter`, `outil`, `ajouter_par_ui`, `modifier_par_ui`, `source_git`, `importer`, `RESERVES`) :

  - `tests/test_db.py::test_visibilite_travail_acceptee_inconnue_refusee` — `inscrire_source(bd, "t", visibilite="travail")` réussit ; un `UPDATE sources SET visibilite = 'pro'` lève `psycopg.errors.CheckViolation`.
  - `tests/test_config.py::test_tout_contient_travail` — `TOUT == frozenset({"public-maison", "prive", "travail"})` et `"travail" not in PUBLIC` ; `SourceConf(nom="a", type="code", emplacement="https://h/a.git", visibilite="travail")` valide.
  - `tests/test_api.py::test_le_jeton_public_ne_voit_pas_une_source_travail` — comme `test_le_jeton_public_ne_voit_pas_une_source_privee` avec `'travail'` : `auth` → `[]`, `auth_prive` → non vide.
  - `tests/test_mcp.py::test_mcp_respecte_le_niveau_travail` — comme `test_mcp_respecte_les_niveaux` avec `'travail'` ; en plus, `rag_sources` sans jeton privé ne liste pas `coffre-maison`, avec jeton privé oui.
  - `tests/test_ui.py::test_sources_public_ne_voit_pas_travail` — comme `test_sources_public_ne_voit_pas_prive` ; en plus, avec le cookie privé, la page contient `"Réindexer"` (Review Focus 4).
  - `tests/test_ui.py::test_source_travail_introuvable_avec_cookie_public` — comme `test_source_privee_introuvable_avec_cookie_public` (Review Focus 3).
  - `tests/test_ui.py::test_formulaire_propose_travail` — `gc.get("/ui/sources-git/nouvelle").text` contient `<option value="travail"`.
  - `tests/test_ui.py::test_modifier_vers_travail_sans_reseau` — `ajouter_par_ui(gc)`, `appels.clear()`, `modifier_par_ui(gc, visibilite="travail")` → 200 et `hx-redirect == "/ui/"`, `appels == []`, `source_git(bd)["visibilite"] == "travail"`, `demande_le` inchangé (Review Focus 1).
  - `tests/test_ui.py::test_visibilite_inconnue_refusee` — après `ajouter_par_ui(gc)`, `modifier_par_ui(gc, visibilite="pro")` ne renvoie pas de `hx-redirect` et la visibilité stockée reste `"public-maison"` (Review Focus 2).
  - `tests/test_sources_gerees.py::test_import_export_garde_travail` — `importer(bd, "- {nom: a, emplacement: https://github.com/a/b.git, visibilite: travail}", noms_reserves=RESERVES)` puis `_ligne(bd, "a")["visibilite"] == "travail"` ; `"visibilite: travail" in exporter(bd)` (`rag.sources_gerees.exporter`) (Review Focus 5).

- [ ] **Step 2 : lancer** `uv run pytest -q -k "travail"` → FAIL (contrainte SQL, `Literal`, menu).
- [ ] **Step 3 : migration** `rag/migrations/007_visibilite_travail.sql` : `ALTER TABLE sources DROP CONSTRAINT sources_visibilite_check;` puis `ALTER TABLE sources ADD CONSTRAINT sources_visibilite_check CHECK (visibilite IN ('public-maison', 'prive', 'travail'));` avec un commentaire d'une ligne. Vérifier d'abord le nom réel : `SELECT conname FROM pg_constraint WHERE conrelid = 'sources'::regclass AND contype = 'c';` sur la base de test.
- [ ] **Step 4 : config** — ajouter `"travail"` à `TOUT` et au `Literal` de `visibilite` dans `rag/config.py`.
- [ ] **Step 5 : gabarit** — le tuple de `source_git.html:19` devient `("public-maison", "prive", "travail")`.
- [ ] **Step 6 : README** — mettre à jour la puce `RAG_JETON_PRIVE`.
- [ ] **Step 7 : lancer** `uv run pytest -q` puis `uv run ruff check .` → tout PASS, aucun avertissement.
- [ ] **Step 8 : commit**

```bash
git add rag/migrations/007_visibilite_travail.sql rag/config.py rag/ui/gabarits/source_git.html README.md tests/
git commit -m "feat: niveau de visibilité travail, visible seulement avec le jeton privé"
```
