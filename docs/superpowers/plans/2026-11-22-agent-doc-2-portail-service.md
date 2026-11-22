# agent-doc — plan 2 : authentification de service du Portail (dépôt AI)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Ce plan s'exécute dans le dépôt `nicolaslallier/AI`**, pas dans RAG. Le recopier dans `AI/docs/superpowers/plans/` au début de la PR. Partir d'`origin/main` à jour, sur une branche, PR en brouillon ; ne jamais pousser sur `main` (un merge redéploie le Portail).

**Goal:** Permettre à agent-doc de créer, sans session de navigateur, une tâche de documentation seule directement « Prête » dans le projet Portail d'un dépôt, et de recevoir son lien ; et faire appeler les agents A2A par le Portail avec une clé limitée aux modèles locaux.

**Architecture:** Une seule route hors de `/api/` (`POST /service/taches-doc`), authentifiée par un jeton de service comparé en temps constant. Elle retrouve le projet par l'URL de son dépôt, réutilise `creer_tache` et `deplacer_tache`, et réveille l'ordonnanceur. Le vhost ferme `/service` au LAN ; seuls les conteneurs d'`infra-net` l'atteignent.

**Tech Stack:** celle du Portail (FastAPI, SQLAlchemy async, pytest-asyncio, `backend/tests/conftest.py`).

**Spec:** `docs/superpowers/specs/2026-11-21-agent-doc-design.md` du dépôt RAG (« Documentation de dépôt »). Contrat : `2026-11-22-agent-doc-0-index.md` du dépôt RAG, section « agent-doc → Portail ».

## Contrat (extrait de l'index)

- Route : `POST /service/taches-doc`, `Authorization: Bearer <PORTAIL_JETON_SERVICE>`.
- Corps : `{"depot": "<nom du dépôt, sans propriétaire>", "demande": "<texte>", "appelant": "<nom>"}`.
- Réponses : 201 `{"tache_id", "projet_id", "lien", "existante": false}` ; 200 avec `existante: true` si une tâche identique est déjà ouverte ; 401 jeton absent ou faux ; 503 jeton de service non configuré ; 404 aucun projet pour ce dépôt ; 409 plusieurs projets pour ce dépôt ; 429 trop de tâches de service ouvertes dans le projet ; 422 corps invalide.
- Secret : clé `PORTAIL_JETON_SERVICE` du secret OpenBao `infra/portail`, **facultative** (absente : route en 503, le reste du Portail inchangé).
- Clé locale : `PORTAIL_LITELLM_CLE_AGENTS` (`infra/portail`, facultative) = clé LiteLLM d'alias `portail-agents`, équipe `agents` ; utilisée pour tout appel du Portail à un agent A2A (agent-musique aujourd'hui).

## Global Constraints

- Le jeton de service ne donne accès qu'à cette route. `/api/` garde son en-tête `X-Auth-Request-Email` ; `/interne/` garde son jeton de mission. Aucune de ces trois portes n'ouvre les deux autres.
- Une requête venue du réseau des agents (`PORTAIL_RESEAU_AGENTS`) ne doit pas atteindre `/service/` : le middleware `_reseau_agents` répond déjà 404 à tout ce qui n'est pas `/interne/` — un test le fige.
- La tâche créée a `doc = True`, `origine = "agent"`, `protegee = False`, colonne `prete`, auteur du journal de tâche `service:<appelant>`.
- Le jeton n'apparaît jamais dans un journal, une réponse ou une ligne d'audit.
- La PR produite par la tâche reste à fusionner par Nicolas : cette route ne touche ni aux protections ni à la boucle de dev.

## Review Focus

1. **gemma appelle l'outil deux fois avec la même demande** (boucle d'outils, nouvelle tentative de LiteLLM) : une seule tâche, la seconde réponse la désigne (`existante: true`). → test tâche 1.
2. **Jeton de service vide en configuration** : la route répond 503 à tout le monde, y compris à `Authorization: Bearer ` (vide) — jamais 201. → test tâche 1.
3. **Dépôt écrit autrement** (`infra`, `Infra.git`, `https://github.com/nicolaslallier/Infra/`) : la comparaison ignore la casse, le suffixe `.git` et la barre finale ; un `depot` avec `/` ou `..` est refusé en 422. → test tâche 1.
4. **Jeton fuité qui inonde le kanban** : au plus 5 tâches de service ouvertes par projet, puis 429. → test tâche 1.
5. **`/service/` joignable depuis le LAN** par le vhost : fermé par `location ^~ /service`. → test tâche 2.

---

### Task 1: Route `POST /service/taches-doc`

**Files:**
- Create: `backend/portail/service.py`
- Modify: `backend/portail/config.py`, `backend/portail/api.py`
- Test: `backend/tests/test_service.py`

**Interfaces:**
- Consumes: `creer_tache(session, projet_id, donnees, auteur, origine, document_id, protegee)`, `deplacer_tache(session, tache, colonne, avant_id, auteur)` (`portail/taches.py`) ; `inscrire` (`portail/audit.py`) ; `Refus` (`portail/moteur.py`) ; `lien_absolu` et `_reveiller` tels qu'utilisés par `ajouter_tache` dans `api.py`.
- Produces:
  - `Reglages.jeton_service: str = ""` (`PORTAIL_JETON_SERVICE`).
  - `class DemandeDoc(BaseModel)` : `depot: str` (`^[A-Za-z0-9._-]{1,100}$`, et ni `.` ni `..`), `demande: str` (1 à 4000 caractères après `strip`), `appelant: str` (`^[a-z0-9-]{1,40}$`) ; `extra="forbid"`.
  - `MAX_OUVERTES = 5`.
  - `async def demander_doc(session, reglages, corps: DemandeDoc) -> tuple[Tache, bool]` dans `service.py` :
    1. projets dont `depot_url`, normalisée (minuscules, sans `.git`, sans `/` final), vaut `https://github.com/<reglages.github_proprietaire>/<depot>` normalisée ; aucun → `Refus(404, "aucun projet pour le dépôt <depot>")` ; plusieurs → `Refus(409, "plusieurs projets pour le dépôt <depot>")` ;
    2. tâche du projet avec `doc`, `origine == "agent"`, colonne dans `("a_faire", "prete", "en_cours")` et `description == corps.demande` → `(tache, True)` sans rien écrire ;
    3. si le projet compte déjà `MAX_OUVERTES` tâches `doc` d'origine `agent` dans ces trois colonnes → `Refus(429, "trop de demandes de documentation ouvertes")` ;
    4. sinon `creer_tache(..., DonneesTache(titre="Documentation : " + <première ligne de la demande, 180 car. au plus>, description=corps.demande, doc=True), auteur=f"service:{appelant}", origine="agent", protegee=False)`, puis `deplacer_tache(..., "prete", None, f"service:{appelant}")` ; rend `(tache, False)`.
  - Route, déclarée dans `create_app` à côté de `ajouter_tache` : vérifie le jeton (`reglages.jeton_service` vide → 503 `{"detail": "service désactivé"}` ; en-tête absent, schéma autre que `Bearer` ou valeur différente par `secrets.compare_digest` → 401 `{"detail": "jeton de service invalide"}`), appelle `demander_doc`, `inscrire(s, f"service:{appelant}", "tache.creer", "tache", t.id)` si créée, `commit`, `_reveiller`, répond 201 (créée) ou 200 (existante) `{"tache_id", "projet_id", "lien": lien_absolu(url_publique, f"/projets/{projet_id}/kanban"), "existante"}`.

- [ ] **Step 1: Écrire les tests qui échouent** (client construit comme la fixture `client`, avec `jeton_service="s3cret"` et `github_proprietaire="nicolaslallier"`)

```python
JETON = {"Authorization": "Bearer s3cret"}
CORPS = {"depot": "Infra", "demande": "Documente la pile mcp.\nDétails…", "appelant": "agent-doc"}

async def _projet(client, depot_url):
    return (await client.post("/api/projets", json={"nom": "P", "depot_url": depot_url})).json()   # adapter aux champs de ProjetIn

async def test_cree_une_tache_doc_prete(client_service, sessions):
    p = await _projet(client_service, "https://github.com/nicolaslallier/Infra.git")
    r = await client_service.post("/service/taches-doc", json=CORPS, headers=JETON)
    assert r.status_code == 201 and r.json()["existante"] is False and r.json()["projet_id"] == p["id"]
    assert r.json()["lien"].endswith(f"/projets/{p['id']}/kanban")
    async with sessions() as s:
        t = await s.get(Tache, r.json()["tache_id"])
        assert (t.doc, t.colonne, t.origine, t.protegee) == (True, "prete", "agent", False)
        assert t.titre == "Documentation : Documente la pile mcp." and t.description == CORPS["demande"]

async def test_meme_demande_une_seule_tache(client_service):
    await _projet(client_service, "https://github.com/NicolasLallier/infra/")
    a = await client_service.post("/service/taches-doc", json=CORPS, headers=JETON)
    b = await client_service.post("/service/taches-doc", json=CORPS | {"depot": "infra"}, headers=JETON)
    assert (b.status_code, b.json()["existante"], b.json()["tache_id"]) == (200, True, a.json()["tache_id"])

async def test_jeton(client_service, client):                    # `client` : jeton_service vide
    assert (await client_service.post("/service/taches-doc", json=CORPS)).status_code == 401
    assert (await client_service.post("/service/taches-doc", json=CORPS, headers={"Authorization": "Bearer faux"})).status_code == 401
    for h in ({}, {"Authorization": "Bearer "}, JETON):
        assert (await client.post("/service/taches-doc", json=CORPS, headers=h)).status_code == 503

async def test_depot_inconnu_ambigu_invalide(client_service):
    assert (await client_service.post("/service/taches-doc", json=CORPS, headers=JETON)).status_code == 404
    for _ in range(2):
        await _projet(client_service, "https://github.com/nicolaslallier/Infra")
    assert (await client_service.post("/service/taches-doc", json=CORPS, headers=JETON)).status_code == 409
    for depot in ("a/b", "..", "", "x" * 101):
        assert (await client_service.post("/service/taches-doc", json=CORPS | {"depot": depot}, headers=JETON)).status_code == 422

async def test_plafond_de_taches_ouvertes(client_service):
    await _projet(client_service, "https://github.com/nicolaslallier/Infra")
    codes = [(await client_service.post("/service/taches-doc", json=CORPS | {"demande": f"d{i}"}, headers=JETON)).status_code for i in range(6)]
    assert codes == [201] * 5 + [429]

async def test_le_jeton_de_service_n_ouvre_pas_api(client_exigeant):   # exiger_utilisateur=True, jeton_service="s3cret"
    assert (await client_exigeant.get("/api/projets", headers=JETON)).status_code == 401

async def test_reseau_agents_ne_voit_pas_service(...):   # sur le modèle du test existant de `_reseau_agents` : pair dans le CIDR → 404
    ...
```

- [ ] **Step 2: Lancer** `cd backend && uv run pytest tests/test_service.py -v` — attendu : ÉCHEC (404 sur la route).
- [ ] **Step 3: Implémenter** `Reglages.jeton_service`, `service.py`, la route. Inclure la route **avant** le repli SPA, comme `ROUTEUR_INTERNE`.
- [ ] **Step 4: Relancer** `uv run pytest -q` et les contrôles de la CI du dépôt (`ruff`, types) — attendu : tout vert.
- [ ] **Step 5: Commit** `git commit -am "feat: route de service /service/taches-doc (tâche doc-only prête, jeton de service)"`

---

### Task 2: Vhost, pile et secret

**Files:**
- Modify: `deploy/nginx/portail.conf`, `deploy/portainer-stack.yml`, `scripts/vault-render-env.sh`
- Test: `backend/tests/test_pile.py`

- [ ] **Step 1: Écrire les tests qui échouent** dans `backend/tests/test_pile.py` (même forme que les contrôles voisins) :

```python
def test_service_ferme_sur_le_vhost_et_jeton_facultatif():
    conf = (RACINE / "deploy" / "nginx" / "portail.conf").read_text()
    assert re.search(r"location \^~ /service \{\s*return 404;\s*\}", conf)
    pile = (RACINE / "deploy" / "portainer-stack.yml").read_text()
    assert "PORTAIL_JETON_SERVICE: ${PORTAIL_JETON_SERVICE:-}" in pile
    rendu = VAULT.read_text()
    assert "PORTAIL_JETON_SERVICE=PORTAIL_JETON_SERVICE" in rendu
    assert " PORTAIL_JETON_SERVICE " in re.search(r'FACULTATIVES="([^"]*)"', rendu).group(1)
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_pile.py -k service -v` — attendu : ÉCHEC.
- [ ] **Step 3: Modifier** : dans `portail.conf`, `location ^~ /service { return 404; }` à côté de `location ^~ /interne` ; dans la pile, la variable sur le service `portail` ; dans `vault-render-env.sh`, la paire dans `MAP` et le nom dans `FACULTATIVES`.
- [ ] **Step 4: Relancer** `uv run pytest tests/test_pile.py -v` — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "deploy: /service fermé sur le vhost, PORTAIL_JETON_SERVICE facultatif"`

---

### Task 3: Clé LiteLLM locale pour les appels aux agents

Décision de Nicolas (2026-11-22) : le Portail n'appelle plus les agents A2A avec sa clé `ai` (équipe `apps`, qui a des modèles infonuagiques) mais avec une clé de l'équipe `agents`, d'alias `portail-agents` (créée côté Infra, plan 3 du projet 2, tâche 1).

**Files:**
- Modify: `backend/portail/config.py`, `backend/portail/api.py` (route `/api/musique`), `deploy/portainer-stack.yml`, `scripts/vault-render-env.sh`
- Test: `backend/tests/test_musique.py`, `backend/tests/test_pile.py`

**Interfaces:**
- Produces: `Reglages.litellm_cle_agents: str = ""` (`PORTAIL_LITELLM_CLE_AGENTS`) ; la route `/api/musique` passe à `demander(...)` `r.litellm_cle_agents or r.litellm_cle`. Aucun autre appel ne change (les modèles restent appelés avec `litellm_cle`). Pile : `PORTAIL_LITELLM_CLE_AGENTS: ${PORTAIL_LITELLM_CLE_AGENTS:-}` ; `vault-render-env.sh` : paire dans `MAP`, nom dans `FACULTATIVES`. Tout futur appel du Portail à agent-doc utilise la même clé.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
async def test_musique_appelle_avec_la_cle_des_agents(...):       # sur le modèle des tests existants de /api/musique (transport httpx simulé)
    # reglages(litellm_cle="cle-apps", litellm_cle_agents="cle-agents") -> la requête vers LiteLLM porte « Bearer cle-agents »
    # reglages(litellm_cle="cle-apps", litellm_cle_agents="")           -> « Bearer cle-apps » (repli, rien ne casse avant l'étape manuelle)

def test_cle_des_agents_facultative_dans_la_pile():               # tests/test_pile.py
    assert "PORTAIL_LITELLM_CLE_AGENTS: ${PORTAIL_LITELLM_CLE_AGENTS:-}" in (RACINE / "deploy" / "portainer-stack.yml").read_text()
    rendu = VAULT.read_text()
    assert "PORTAIL_LITELLM_CLE_AGENTS=PORTAIL_LITELLM_CLE_AGENTS" in rendu
    assert " PORTAIL_LITELLM_CLE_AGENTS " in re.search(r'FACULTATIVES="([^"]*)"', rendu).group(1)
```

- [ ] **Step 2: Lancer** `uv run pytest tests/test_musique.py tests/test_pile.py -v` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter.**
- [ ] **Step 4: Relancer** `uv run pytest -q` — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "feat: clé LiteLLM locale (équipe agents) pour les appels du Portail aux agents A2A"`

---

### Task 4: Documentation, étapes manuelles, PR

**Files:**
- Modify: `README.md`, `deploy/infra/README.md`

- [ ] **Step 1: Écrire** dans `README.md` (section des clés du secret `infra/portail`) : `PORTAIL_LITELLM_CLE_AGENTS` (facultative ; clé de l'équipe `agents`, modèles locaux seulement, utilisée pour les agents A2A) et `PORTAIL_JETON_SERVICE` (facultative), ce qu'elle ouvre (une seule route), qui l'utilise (agent-doc, dépôt Infra), et le tableau des réponses du contrat.
- [ ] **Step 2: Écrire** dans `deploy/infra/README.md` une section « agent-doc (jeton de service) », étapes dans cet ordre, **toutes faites par Nicolas** :
  1. `openssl rand -hex 32` ; ranger la valeur dans OpenBao : `infra/portail` clé `PORTAIL_JETON_SERVICE`, et `infra/env` clé `AGENT_DOC_PORTAIL_JETON` (même valeur) ;
  2. fusionner cette PR (le Portail se redéploie) ;
  3. recopier `deploy/nginx/portail.conf` dans l'Infra (fait par le plan 3 du projet 2, tâche 9) ;
  4. clé locale : après `make litellm-provision` dans l'Infra (plan 3 du projet 2, tâche 1), ranger la clé `portail-agents` dans `infra/portail`, clé `PORTAIL_LITELLM_CLE_AGENTS`, puis redéployer le Portail ; vérifier que la page Musique répond toujours. C'est seulement après cela que l'Infra retire `apps` d'agent-musique (plan 5) ;
  5. vérifier : `curl -sk -o /dev/null -w '%{http_code}' -X POST https://portail.infra.famillelallier.net/service/taches-doc` rend `404` ; depuis un conteneur d'`infra-net`, le même `POST` sur `http://portail:8000/service/taches-doc` sans jeton rend `401`, et avec le jeton et un dépôt qui a un projet, `201` avec un lien — la tâche apparaît dans la colonne « Prête » puis part en exécution.
- [ ] **Step 3: Commit et PR en brouillon** `git commit -am "docs: jeton de service du Portail pour agent-doc"` puis `gh pr create --draft`. Ne pas fusionner.
