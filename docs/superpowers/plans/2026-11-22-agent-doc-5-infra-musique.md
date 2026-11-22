# agent-doc — plan 5 : agent-musique écrit par agent-doc (dépôt Infra)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Ce plan s'exécute dans le dépôt `nicolaslallier/Infra`.** Le recopier dans `Infra/docs/superpowers/plans/`. Branche depuis `origin/main` à jour, PR en brouillon, jamais de push sur `main`. Aucune étape sur l'hôte n'est faite par l'agent.

**Goal:** Qu'agent-doc soit le seul écrivain du coffre côté serveur : agent-musique garde ses lectures directes dans S3, mais ses deux écritures (la note d'un disque, `Discographie.md`) passent par le mode déterministe d'agent-doc, et son identité S3 passe en lecture seule.

**Architecture:** `disques.py` ne reçoit plus un client S3 pour écrire mais une fonction `ecrire(chemin, contenu, pourquoi) -> dict` injectée ; `server.py` la fournit : un appel A2A à agent-doc à travers LiteLLM, avec la clé existante d'agent-musique (équipe `agents`). La règle de calme, la version et le journal deviennent ceux d'agent-doc ; agent-musique n'en garde aucune copie.

**Tech Stack:** celle d'agent-musique (aucune dépendance ajoutée : `httpx` y est déjà).

**Spec:** `docs/superpowers/specs/2026-11-21-agent-doc-design.md` du dépôt RAG (« agent-musique », « Écriture structurée sans modèle »). Contrat : `2026-11-22-agent-doc-0-index.md` (« Accès à agent-doc », « Mode déterministe »). Prérequis : plan 3 déployé et vérifié. Indépendant du plan 4.

## Contrat (extrait de l'index)

- Appel : `POST ${AGENT_DOC_A2A_URL}` (= `http://litellm:4000/a2a/agent-doc/message/send`), `Authorization: Bearer ${AGENT_MUSIQUE_LITELLM_KEY}`, `A2A-Version: 1.0`, JSON-RPC `message/send`, une partie texte = `{"action": "ecrire", "chemin", "contenu", "pourquoi"}`.
- Réponse : le texte de la réponse A2A est un objet JSON — `{"ok": true, "effet": "creee" | "modifiee" | "inchangee", "journal", …}` ou `{"ok": false, "refus": "<code>", "message"}`.
- `02 - Domaines/Loisirs/Musique/Discographie.md` est dans `generes` de la config d'agent-doc : écrit sans version, sans journal, sans règle de calme.
- Identité de l'appelant : la clé `agent-musique` porte l'utilisateur `agent-musique@agents.invalid` ; le journal dit alors `appelant : agent-musique`.

## Global Constraints

- Après ce plan, **aucun** `put_object`, `delete_object` ni `copy_object` dans `agents/agent-musique/` (un test cherche le texte).
- agent-musique reste borné à `02 - Domaines/Loisirs/Musique/` par `cle_ecriture()` **avant** tout appel à agent-doc ; le modèle ne choisit toujours pas le chemin (`nom_note`).
- Ce que l'utilisateur voit ne change pas : mêmes messages (« note créée », « note existante mise à jour », « refusé : la note a été modifiée il y a moins de 10 minutes… »), même ligne `Sources :`, même index régénéré.
- La mise à jour d'une note existante garde le reste de la note octet pour octet (`mise_a_jour`, inchangée).
- La clé d'agent-musique reste dans l'équipe `agents`. Décision de Nicolas : le Portail appelle agent-musique avec sa clé locale `portail-agents` (plan 2, tâche 3) ; ce plan retire donc `apps` des équipes autorisées d'agent-musique — **seulement une fois le Portail déployé avec `PORTAIL_LITELLM_CLE_AGENTS`**, sinon la page Musique répond 403.
- Conventions de l'Infra : commentaires en anglais, `make test` et `make check-env` verts à chaque commit, balise d'image changée au même commit dans `docker-compose.mcp.yml` et `scripts/portainer-images.sh`.

## Review Focus

1. **agent-doc ne répond pas, ou répond autre chose que du JSON** : le disque n'est pas annoncé comme enregistré ; le modèle reçoit « refusé : … » et la réponse ne dit jamais « enregistré ». → tests tâches 1 et 2.
2. **La note est écrite mais l'index échoue** (agent-doc tombe entre les deux appels) : le message dit « note créée ; index NON régénéré », la note figure dans `Sources :`. → test tâche 1.
3. **Refus `calme`** : le message d'agent-doc est rendu tel quel, précédé de « refusé : », et l'index n'est pas régénéré. → test tâche 1.
4 bis. **`apps` retirée d'agent-musique avant que le Portail ait sa clé locale** : la page Musique tombe en 403. L'ordre des étapes manuelles l'interdit. → tâche 4.
4. **Identité S3 passée en lecture seule avant le déploiement de la nouvelle image** : l'ancienne image échouerait à chaque enregistrement. L'ordre des étapes manuelles l'interdit. → tâche 4.
5. **`Discographie.md` écrite à la main** (pas le fichier généré) : toujours jamais écrasée — le contrôle du marqueur `type: index` / `source: claude` reste dans agent-musique, avant l'appel. → test existant `test_a_hand_written_discographie_is_never_overwritten`, à garder vert.

---

### Task 1: `disques.py` — l'écriture devient une fonction injectée

**Files:**
- Modify: `agents/agent-musique/disques.py`
- Test: `scripts/test_agent_musique.py`

**Interfaces:**
- Produces:
  - Type de la fonction injectée : `ecrire(chemin: str, contenu: str, pourquoi: str) -> dict` — `chemin` relatif au coffre ; rend la réponse du mode déterministe d'agent-doc (ou `{"ok": False, "refus": "interne", "message": …}` si l'appel échoue).
  - `_ecrire(s3, …)` disparaît. Nouvelle `_ecrire(ecrire, cle: str, texte: str, vault_prefix: str, pourquoi: str) -> dict` : `cle_ecriture(cle, vault_prefix)` puis `ecrire(<cle sans le préfixe du coffre>, texte, pourquoi)` — toujours la seule fonction du module qui écrit.
  - `regenerer_index(s3, bucket, vault_prefix, ecrire) -> str` : inchangée, sauf l'écriture ; réponse `ok` faux → rend ` ; index NON régénéré : <message>`.
  - `enregistrer(s3, bucket, vault_prefix, now, ecrire, artiste, titre, format, …) -> tuple[str, str]` : `head_object` ne sert plus qu'à choisir entre `note_neuve` et `mise_a_jour` (le contrôle d'âge local et le paramètre `quiet_minutes` sont retirés : la règle de calme est celle d'agent-doc). Réponse `ok` faux → `(rel, "refusé : " + message)`, sans régénérer l'index. `effet == "inchangee"` → message « note déjà à jour ». Sinon « note créée » / « note existante mise à jour », suivi du retour de `regenerer_index`.
  - `repondre(s3, bucket, vault_prefix, llm, question, now, ecrire) -> str` : `quiet_minutes` retiré, `ecrire` transmis.
  - Le docstring du module dit que les écritures passent par agent-doc.

- [ ] **Step 1: Adapter les tests pour qu'ils échouent.** Un `FauxDoc` remplace les écritures du `FakeS3` :

```python
class FauxDoc:
    """agent-doc's deterministic `ecrire`, on the same dict as FakeS3; `refus` forces an answer."""
    def __init__(self, s3, refus=None, tombe_apres=None):
        self.s3, self.refus, self.tombe_apres, self.appels = s3, refus, tombe_apres, []
    def __call__(self, chemin, contenu, pourquoi):
        self.appels.append((chemin, pourquoi))
        if self.tombe_apres is not None and len(self.appels) > self.tombe_apres:
            return {"ok": False, "refus": "interne", "message": "agent-doc ne répond pas"}
        if self.refus:
            return {"ok": False, "refus": self.refus, "message": "la note a été modifiée il y a moins de 10 minutes (synchro Obsidian en cours ?) ; réessaie plus tard"}
        existait = VP + chemin in self.s3.data
        self.s3.data[VP + chemin] = (contenu.encode(), NOW)
        return {"ok": True, "action": "ecrire", "chemin": chemin, "effet": "modifiee" if existait else "creee", "journal": 1, "version": None, "reindexation": "demandee"}

def test_a_creation_goes_through_agent_doc_then_the_index():
    s3 = FakeS3(); doc = FauxDoc(s3)
    rel, msg = disques.enregistrer(s3, "obsidian", VP, NOW, doc, artiste="A", titre="B", format="CD")
    assert msg == "note créée" and s3.puts == []                                   # nothing written with the S3 client
    assert [c for c, _ in doc.appels] == ["02 - Domaines/Loisirs/Musique/A - B (CD).md", "02 - Domaines/Loisirs/Musique/Discographie.md"]
    assert "| A | B | CD |" in s3.text(M + "Discographie.md")

def test_a_quiet_refusal_is_passed_on_and_the_index_is_left():
    s3 = FakeS3({M + "A - B (CD).md": b"---\ntype: disque\n---\n"}); doc = FauxDoc(s3, refus="calme")
    rel, msg = disques.enregistrer(s3, "obsidian", VP, NOW, doc, artiste="A", titre="B", format="CD")
    assert msg.startswith("refusé : la note a été modifiée il y a moins de 10 minutes") and len(doc.appels) == 1

def test_agent_doc_down_after_the_note_says_the_index_was_not_rebuilt():
    s3 = FakeS3(); doc = FauxDoc(s3, tombe_apres=1)
    rel, msg = disques.enregistrer(s3, "obsidian", VP, NOW, doc, artiste="A", titre="B", format="CD")
    assert msg.startswith("note créée ; index NON régénéré") and M + "A - B (CD).md" in s3.data

def test_agent_doc_down_nothing_is_said_recorded():
    s3 = FakeS3(); doc = FauxDoc(s3, tombe_apres=0)
    out = disques.repondre(s3, "obsidian", VP, scenario_enregistrer_puis("C'est fait."), "enregistre A - B en CD", NOW, doc)
    assert "refusé" in dernier_resultat_d_outil() and "A - B (CD).md" not in out.split("Sources :")[-1]

def test_the_module_never_writes_deletes_or_copies():
    src = open(os.path.join(AGENT, "disques.py")).read() + open(os.path.join(AGENT, "server.py")).read()
    assert "put_object" not in src and "delete_object" not in src and "copy_object" not in src
    assert src.count("ecrire(") >= 1 and open(os.path.join(AGENT, "disques.py")).read().count("def _ecrire(") == 1
```

  Les tests existants qui passent `quiet_minutes` ou lisent `s3.puts` sont réécrits sur `FauxDoc` (`test_a_creation_writes_the_frontmatter_and_the_index`, `…is_an_update_that_keeps_the_rest_byte_for_byte`, `test_an_update_never_truncates_or_alters_a_note`, `test_a_hand_written_discographie_is_never_overwritten`, `test_a_write_during_six_exhausted_rounds_is_said`) ; `test_an_update_inside_the_quiet_window_is_refused` est remplacé par le test du refus `calme` ci-dessus ; `test_the_module_never_deletes_or_copies_and_writes_in_one_place` par le dernier.

- [ ] **Step 2: Lancer** `make test` — attendu : ÉCHEC (`TypeError` sur les signatures).
- [ ] **Step 3: Modifier** `disques.py`.
- [ ] **Step 4: Relancer** — attendu : SUCCÈS pour `disques.py` ; le test de `server.py` échoue encore (tâche 2).
- [ ] **Step 5: Commit** `git commit -am "agent-musique: writes go through an injected ecrire(), no S3 put left in disques.py"`

---

### Task 2: `server.py` — l'appel A2A à agent-doc

**Files:**
- Modify: `agents/agent-musique/server.py`
- Test: `scripts/test_agent_musique.py`

**Interfaces:**
- Consumes: `disques.repondre(..., ecrire)`.
- Produces:
  - `DOC_URL = os.environ.get("AGENT_DOC_A2A_URL", "http://litellm:4000/a2a/agent-doc/message/send")`.
  - `ecrire_par_doc(chemin: str, contenu: str, pourquoi: str) -> dict` : `httpx.post(DOC_URL, headers={"Authorization": f"Bearer {KEY}", "A2A-Version": "1.0"}, timeout=120, json=<JSON-RPC message/send, une partie texte = json.dumps({"action": "ecrire", "chemin", "contenu", "pourquoi"}, ensure_ascii=False)>)` ; rend l'objet JSON décodé du texte de la réponse (texte = concaténation des parties texte du `result`, message ou tâche, formes A2A 0.3 et 1.0 — même lecture que `texte_de` de `backend/portail/musique.py` du dépôt AI). Statut HTTP d'erreur, délai, réponse JSON-RPC `error`, texte qui n'est pas un objet JSON avec une clé `ok` → `{"ok": False, "refus": "interne", "message": "agent-doc ne répond pas (<raison courte, sans contenu>)"}`. Ne lève jamais.
  - `MusiqueExecutor.execute` passe `ecrire_par_doc` à `disques.repondre` ; `QUIET` et `AGENT_MUSIQUE_QUIET_MINUTES` sont retirés.
  - Le client boto3 reste (lectures) ; le commentaire d'en-tête dit « reads the vault, writes through agent-doc ».

- [ ] **Step 1: Écrire les tests qui échouent** (un faux LiteLLM local, comme celui que le fichier utilise déjà, répond sur `/a2a/agent-doc/message/send`)

```python
def test_ecrire_par_doc_sends_the_deterministic_message_with_the_agent_key(faux_litellm):
    faux_litellm.reponse_a2a = {"jsonrpc": "2.0", "id": "1", "result": {"role": "agent", "parts": [{"text": '{"ok": true, "effet": "creee", "journal": 4}'}]}}
    r = server.ecrire_par_doc("02 - Domaines/Loisirs/Musique/A - B (CD).md", "x", "disque enregistré")
    assert r == {"ok": True, "effet": "creee", "journal": 4}
    req = faux_litellm.requetes[-1]
    assert req.chemin == "/a2a/agent-doc/message/send" and req.entetes["Authorization"] == "Bearer " + CLE and req.entetes["A2A-Version"] == "1.0"
    envoye = json.loads(req.json["params"]["message"]["parts"][0]["text"])
    assert envoye == {"action": "ecrire", "chemin": "02 - Domaines/Loisirs/Musique/A - B (CD).md", "contenu": "x", "pourquoi": "disque enregistré"}

def test_ecrire_par_doc_never_raises():
    for panne in (statut(403), statut(500), texte("pas du json"), texte('["liste"]'), jsonrpc_error(), delai()):
        r = server.ecrire_par_doc_avec(panne)("a.md", "x", "p")
        assert r["ok"] is False and r["refus"] == "interne" and "x" not in r["message"], panne
```

- [ ] **Step 2: Lancer** — attendu : ÉCHEC. **Step 3: Implémenter.** **Step 4: Relancer** `make test` — attendu : SUCCÈS, fichier entier.
- [ ] **Step 5: Commit** `git commit -am "agent-musique: record through agent-doc over A2A"`

---

### Task 3: Image `:4`, pile, identité de l'appelant

**Files:**
- Modify: `docker-compose.mcp.yml`, `scripts/portainer-images.sh`, `litellm/teams.yaml`, `litellm/mcp_policy.yaml`, `nginx/conf.d/litellm.conf` (commentaires qui nomment les équipes), `.env.example` (commentaire de l'identité S3)
- Test: `scripts/test_mcp_policy.py`, `scripts/test_litellm_provision.py`

**Interfaces:**
- Produces:
  - `docker-compose.mcp.yml`, service `agent-musique` : image `infra-agent-musique:4`, variable `AGENT_DOC_A2A_URL: http://litellm:4000/a2a/agent-doc/message/send`, commentaire mis à jour (« its S3 identity is Read,List; writes go through agent-doc »). `scripts/portainer-images.sh` : même balise.
  - `litellm/mcp_policy.yaml` : `agent-musique: {teams: [admin, agents]}` — `apps` retirée (commentaire : « the Portail calls with its local key portail-agents »). Le test `test_policy_agents_are_registered_built_and_keyed` attend alors `{"agent-test": {"teams": ["admin"]}, "agent-musique": {"teams": ["admin", "agents"]}, "agent-doc": {"teams": ["agents"]}}` et `agent_refusal(full, "apps", <id d'agent-musique>)` n'est plus `None`.
  - `litellm/teams.yaml` : utilisateur `{email: agent-musique@agents.invalid, team: agents}` et `user: agent-musique@agents.invalid` sur la clé `agent-musique`. Une clé existante n'est pas modifiée par `make litellm-provision` : l'utilisateur ne prend effet qu'à la rotation de la clé (tâche 4).

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_agent_musique_may_call_agent_doc_and_names_itself():
    spec = _y("litellm", "teams.yaml"); cle = next(k for k in spec["keys"] if k["alias"] == "agent-musique")
    assert cle["team"] == "agents" and cle["user"] == "agent-musique@agents.invalid"
    full = mp.load_policy(os.path.join(ROOT, "litellm", "mcp_policy.yaml"))
    assert mp.agent_refusal(full, cle["team"], hashlib.sha256(b"agent-doc").hexdigest()) is None
    svc = _y("docker-compose.mcp.yml")["services"]["agent-musique"]
    assert svc["image"] == "infra-agent-musique:4" and svc["environment"]["AGENT_DOC_A2A_URL"] == "http://litellm:4000/a2a/agent-doc/message/send"
    assert 'build "infra-agent-musique:4"' in open(os.path.join(ROOT, "scripts", "portainer-images.sh")).read()
```

- [ ] **Step 2: Lancer** — attendu : ÉCHEC. **Step 3: Modifier.** **Step 4: Relancer** `make test`, `make check-env` — attendu : tout vert.
- [ ] **Step 5: Commit** `git commit -am "agent-musique: image :4 writing through agent-doc, service user on its key"`

---

### Task 4: Documentation, étapes manuelles, PR

**Files:**
- Modify: `README.md`, `CLAUDE.md`

- [ ] **Step 1: Mettre à jour** l'entrée `agent-musique` de `CLAUDE.md` et la section du README : il lit S3 directement et écrit par agent-doc ; identité S3 `Read,List` ; plus de `AGENT_MUSIQUE_QUIET_MINUTES` (la règle est `AGENT_DOC_QUIET_MINUTES`) ; les changements de notes de disques sont au journal du documentaliste et annulables ; `Discographie.md` reste régénérée, sans journal. Adapter les tests de docs qui cherchent `provision-s3.sh agent-musique obsidian 0 Read,Write,List`.
- [ ] **Step 2: Écrire « Switching `agent-musique` to agent-doc »** dans le README — étapes **faites par Nicolas**, dans cet ordre (l'ordre évite toute fenêtre où agent-musique ne peut plus enregistrer) :
  1. agent-doc est en service et vérifié (plan 3, tâche 10) ; **le Portail est déployé avec `PORTAIL_LITELLM_CLE_AGENTS`** (plan 2, tâche 3) et la page Musique répond — sans cela, ne pas fusionner : cette PR retire `apps` d'agent-musique.
  2. `make litellm-provision rotate=agent-musique` : la clé est recréée avec son utilisateur de service ; ranger la nouvelle valeur dans `infra/env` (`AGENT_MUSIQUE_LITELLM_KEY`), `make vault-env`. (Sans cette étape tout fonctionne, mais le journal dit `appelant : inconnu`.)
  3. Fusionner : le déploiement construit `infra-agent-musique:4` et redéploie `mcp`.
  4. Vérifier, depuis la page Musique du Portail : « enregistre <un disque d'essai> en CD » → « note créée » ; la note et `Discographie.md` sont à jour dans S3 ; `Journal du documentaliste.md` a une entrée `appelant : agent-musique` pour la note et **aucune** pour `Discographie.md` ; redemander aussitôt le même disque avec un autre état → « refusé : la note a été modifiée il y a moins de 10 minutes ».
  5. **Seulement ensuite** : `./scripts/provision-s3.sh agent-musique obsidian 0 Read,List`.
  6. Vérifier : un appel à agent-musique avec la clé `ai` (équipe `apps`) est refusé (403 `agent-policy`) et la page Musique du Portail répond toujours ; avec l'identité `agent-musique`, un `aws s3 cp` vers le bucket est refusé ; une question (« qu'est-ce que j'ai de Miles Davis ? ») répond toujours ; après 10 minutes, un second enregistrement passe.
  7. Annuler l'enregistrement d'essai : `{"action": "annuler", "numero": N}` avec la clé `nicolas-doc`, puis redemander un enregistrement quelconque pour régénérer l'index.
- [ ] **Step 3: Commit et PR en brouillon** `git commit -am "docs: agent-musique writes through agent-doc, read-only S3 identity"`, `gh pr create --draft`. Ne pas fusionner.
