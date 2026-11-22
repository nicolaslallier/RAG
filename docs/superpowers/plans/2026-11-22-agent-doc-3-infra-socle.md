# agent-doc — plan 3 : l'agent, ses règles d'écriture et son branchement (dépôt Infra)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Ce plan s'exécute dans le dépôt `nicolaslallier/Infra`**, pas dans RAG. Le recopier dans `Infra/docs/superpowers/plans/` au début de la PR. Partir d'`origin/main` à jour, sur une branche, PR en brouillon ; ne jamais pousser sur `main` (un merge redéploie la production). Aucune étape sur l'hôte n'est faite par l'agent.

**Goal:** Mettre en service `agent-doc` : il répond depuis le RAG maître avec ses sources, consigne dans le coffre sous des règles que le code impose (version, journal, calme, anti-doublon, réindexation), accepte des écritures déterministes, annule un changement, et demande au Portail de documenter un dépôt.

**Architecture:** Copie du modèle d'`agent-musique` (a2a-sdk + Starlette, `X-Agent-Token`, `/health`, pile `mcp`). Cinq modules : `coffre.py` est le seul à écrire dans S3 et porte toutes les règles ; `journal.py` est du texte pur ; `clients.py` parle au RAG et au Portail ; `agent.py` aiguille entre le mode déterministe et la boucle d'outils gemma ; `server.py` câble. Le modèle ne choisit ni le frontmatter, ni la version, ni le journal.

**Tech Stack:** Python 3.12, `a2a-sdk[http-server]==1.1.0`, `httpx`, `uvicorn`, `boto3`, `pyyaml`, `tzdata` (mêmes versions épinglées qu'`agents/agent-musique/requirements.in`, plus `pyyaml`) ; tests en script autonome lancé par `make test`.

**Spec:** `docs/superpowers/specs/2026-11-21-agent-doc-design.md` du dépôt RAG. Contrat : `2026-11-22-agent-doc-0-index.md` du dépôt RAG (à garder ouvert : les tableaux « Mode déterministe », « Ce qu'agent-doc écrit dans S3 », « agent-doc → RAG maître », « Secrets et variables » sont la référence de ce plan). À lire dans l'Infra : `agents/agent-musique/` (modèle), `scripts/test_agent_musique.py` (forme des tests, `FakeS3`), `CLAUDE.md` (« Secrets (OpenBao) »), README (« LiteLLM: memory, skills, agents », « Setup of agent-musique »).

**Hors de ce plan :** déplacer, fusionner, archiver, la passe de nuit (plan 4) ; agent-musique (plan 5).

## Global Constraints

- Conventions de l'Infra : commentaires et documentation en anglais ; `make test` et `make check-env` verts à chaque commit ; images épinglées par digest ; aucun secret dans un commit ; une variable ajoutée à un fichier Compose l'est aussi à `.env.example`, `scripts/check-env.sh` et `scripts/ci-fake-env.sh`.
- **Rien ne sort du homelab** : la clé `agent-doc` est dans l'équipe `agents` (aucun modèle `cloud-*`) ; seules les équipes sans modèle infonuagique peuvent appeler l'agent (`agents`), jamais `admin` ni `apps`.
- Modèle `gemma4:26b-a4b-it-qat`, température 0, 8 tours d'outils au plus.
- **Jamais de suppression définitive** : un objet n'est retiré de S3 qu'après que sa copie dans `_versions/` a été écrite **et relue** ; `coffre.py` est le seul module qui appelle `put_object` et `delete_object` (un test le vérifie par recherche de texte dans les autres modules).
- **Calme** : refus de modifier une note dont `LastModified` a moins de `AGENT_DOC_QUIET_MINUTES` (10) minutes. S'applique à toute écriture sur une note existante, dans les deux modes, sauf fichiers `generes`.
- **Version avant modification** : `<préfixe>_versions/<chemin>/<AAAAMMJJTHHMMSSZ>.md`. **Journal** : chaque changement, une entrée numérotée dans `Journal du documentaliste.md`.
- Une note de plus de 1 Mio, ou qui n'est pas de l'UTF-8 strict, n'est jamais réécrite (comme agent-musique).
- Frontmatter d'une note créée par le mode libre, écrit par le code : `type`, `tags`, `source: <appelant>`, `date` (jour à `America/Montreal`), `origine` si fournie.
- Réindexation demandée au RAG après chaque écriture ; son échec ne défait pas l'écriture, il est dit dans la réponse.
- Le contenu du coffre n'apparaît dans aucun journal de conteneur (ni le texte des notes, ni les messages reçus).

## Review Focus

1. **L'appelant ou le modèle glisse des lignes de journal dans `pourquoi`** (`"x\n### n° 99 — …"`, ou une ligne `- données : {...}`) : le texte est aplati sur une ligne et ne peut ni créer une entrée ni fausser « annule ». → test tâche 3.
2. **Le RAG est injoignable** : aucune note n'est créée en mode libre (l'anti-doublon ne peut pas être vérifié) ; le mode déterministe écrit quand même et rend `"reindexation": "echec"`. → tests tâches 5 et 6.
3. **Chemin piégé** (`../x.md`, `/abs.md`, `a\\b.md`, `_versions/x.md`, `.obsidian/app.json`, `x.txt`, le journal lui-même, forme NFD d'un nom accentué) : refus `chemin`, ou chemin ramené en NFC ; rien n'est écrit hors du préfixe du coffre. → test tâche 2.
4. **Le texte reçu commence par `{` sans être une demande** (une question qui cite du JSON, un JSON sans `action`) : il part en mode libre, il n'est pas refusé. → test tâche 5.
5. **Deux écritures en même temps** (deux appels A2A, ou plus tard la passe de nuit) : les numéros de journal ne se répètent pas et aucune entrée ne se perd. → test tâche 3.

---

### Task 1: Clés LiteLLM et utilisateurs de service

**Files:**
- Modify: `litellm/teams.yaml`, `README.md` (la phrase qui cite `users: []`)
- Test: `scripts/test_litellm_provision.py`, `scripts/test_litellm_routes.py` (le test de docs qui cherche `users: []`)

**Interfaces:**
- Produces: clés `agent-doc` (équipe `agents`, `user: null`) et `nicolas-doc` (équipe `agents`, `user: nicolas-doc@agents.invalid`) ; utilisateur `{email: nicolas-doc@agents.invalid, team: agents}` dans `users:`. LiteLLM transmet le `user_id` de la clé appelante dans `X-LiteLLM-User-Id` : c'est l'identité de l'appelant (tâche 8).

- [ ] **Step 1: Écrire le test qui échoue**

```python
def test_agent_doc_keys_are_local_only_and_callers_carry_a_service_user():
    spec = yaml.safe_load(open(os.path.join(ROOT, "litellm", "teams.yaml")))
    keys = {k["alias"]: k for k in spec["keys"]}
    assert not [m for m in spec["teams"]["agents"]["models"] if m.startswith("cloud-")]
    assert keys["agent-doc"]["team"] == "agents" and keys["agent-doc"]["user"] is None
    assert keys["nicolas-doc"] == {"alias": "nicolas-doc", "team": "agents", "user": "nicolas-doc@agents.invalid"}
    assert {"email": "nicolas-doc@agents.invalid", "team": "agents"} in spec["users"]
```

- [ ] **Step 2: Lancer** `make test` — attendu : ÉCHEC (`KeyError: 'agent-doc'`).
- [ ] **Step 3: Modifier** `litellm/teams.yaml` (commentaire : « service users: the part before @ is the caller name agent-doc records ») ; adapter le README et le test de docs qui supposent `users: []`.
- [ ] **Step 4: Relancer** `make test` — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "litellm: keys agent-doc and nicolas-doc (team agents), service user for the caller identity"`

---

### Task 2: `coffre.py` — chemins, lecture, liste

**Files:**
- Create: `agents/agent-doc/coffre.py`, `agents/agent-doc/config.yaml`, `agents/agent-doc/requirements.in`, `agents/agent-doc/requirements.txt`
- Test: `scripts/test_agent_doc.py` (créé ici ; l'ajouter à `scripts/test.sh` sur le modèle du bloc agent-musique : `uv run -q --no-project --with-requirements agents/agent-doc/requirements.txt python scripts/test_agent_doc.py`)

**Interfaces:**
- Produces:

```yaml
# agents/agent-doc/config.yaml — not secret; baked in the image.
journal: "Journal du documentaliste.md"
dossiers: ["00 - Inbox", "01 - Projets", "02 - Domaines", "03 - Ressources", "04 - Archives", "05 - Journal", "06 - Modèles"]
inbox: "00 - Inbox"
modeles: "06 - Modèles"
proteges_prefixes: ["06 - Modèles/", "05 - Journal/", "_Claude/"]   # never moved, merged or archived
proteges_noms: ["_CLAUDE.md", "index.md", "log.md"]                  # any folder
generes: ["02 - Domaines/Loisirs/Musique/Discographie.md"]           # no version, no journal, no quiet rule
anti_doublon: {cosinus: 0.85, limite: 5}
nuit: {seuil_doublon: 0.92, max_examens: 30}
max_tours: 8
```

  - `class Refus(Exception)` : `Refus(code: str, message: str)`, attributs `code`, `message` ; codes du contrat.
  - `class Coffre(s3, bucket: str, prefixe: str, config: dict, quiet_minutes: int, horloge: Callable[[], datetime])` ; `horloge()` rend un instant avec fuseau.
  - `Coffre.chemin(chemin: str) -> str` : chemin relatif normalisé (NFC, sans `.`/segments vides) ; `Refus("chemin", …)` si vide, absolu, avec `..`, `\\` ou `\x00`, ne finissant pas par `.md`, sous `.obsidian/`, `.trash/` ou `_versions/`, ou égal au journal. Accepte `[[Nom]]` et un chemin sans `.md` (les complète), comme `cle_lecture` d'agent-musique.
  - `Coffre.cle(chemin: str) -> str` : préfixe (avec `/` final s'il n'est pas vide) + chemin normalisé.
  - `Coffre.lire(chemin: str) -> tuple[str, datetime]` : texte entier en UTF-8 strict et `LastModified` ; `Refus("absent")`, `Refus("trop_gros")` (> 1 048 576 octets), `Refus("non_utf8")`.
  - `Coffre.existe(chemin: str) -> datetime | None` (`LastModified`, ou `None`).
  - `Coffre.lister(dossier: str) -> tuple[list[str], bool]` : chemins `.md` sous le dossier (récursif), triés, sans `.obsidian/`, `.trash/`, `_versions/` ni le journal ; 2000 au plus, le booléen dit si la liste est tronquée.
  - `Coffre.protege(chemin: str, texte: str | None = None) -> bool` : préfixes et noms de la config, fichiers `generes`, et, si `texte` est donné, une note dont le frontmatter porte la clé `kanban-plugin`.
  - `Coffre.genere(chemin: str) -> bool`.
  - `FakeS3` dans le test : celui de `scripts/test_agent_musique.py`, plus `delete_object(Bucket, Key)` et `list_objects_v2` rendant `LastModified`.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_chemin():
    c = coffre()
    assert c.chemin("03 - Ressources/Poulet.md") == "03 - Ressources/Poulet.md"
    assert c.chemin("[[03 - Ressources/Poulet]]") == "03 - Ressources/Poulet.md"
    assert c.chemin("a/./b//c.md") == "a/b/c.md"
    assert c.chemin("Café.md") == "Café.md"                      # NFD -> NFC
    for mauvais in ("", "/abs.md", "../x.md", "a/../../x.md", "a\\b.md", "x.txt", "_versions/a.md",
                    ".obsidian/app.md", "a/.trash/b.md", "Journal du documentaliste.md", "a\x00.md"):
        assert refus(lambda: c.chemin(mauvais)) == "chemin", mauvais
    assert c.cle("a.md") == "Maison/a.md"

def test_lire_et_lister():
    c = coffre({"Maison/a.md": b"A", "Maison/d/b.md": b"B", "Maison/_versions/a.md/x.md": b"V",
                "Maison/.obsidian/c.md": b"", "Maison/Journal du documentaliste.md": b"J", "Maison/d/i.png": b"\x89"})
    assert c.lire("a.md")[0] == "A" and c.lister("") == (["a.md", "d/b.md"], False) and c.lister("d") == (["d/b.md"], False)
    assert refus(lambda: c.lire("absente.md")) == "absent"
    assert refus(lambda: coffre({"Maison/g.md": b"x" * 1_048_577}).lire("g.md")) == "trop_gros"
    assert refus(lambda: coffre({"Maison/l.md": b"\xff\xfe"}).lire("l.md")) == "non_utf8"

def test_protege():
    c = coffre()
    for p in ("06 - Modèles/Modèle - Projet.md", "05 - Journal/2026-11-22.md", "_Claude/x.md", "01 - Projets/index.md",
              "02 - Domaines/Loisirs/Musique/Discographie.md"):
        assert c.protege(p), p
    assert not c.protege("01 - Projets/Chalet.md")
    assert c.protege("01 - Projets/Tableau.md", "---\nkanban-plugin: board\n---\n")
```

- [ ] **Step 2: Lancer** `make test` — attendu : ÉCHEC (`ModuleNotFoundError: coffre`).
- [ ] **Step 3: Implémenter** ; reprendre `normalize_path` et la lecture bornée d'`agents/agent-musique/disques.py` plutôt que les réécrire. Générer `requirements.txt` comme pour agent-musique (voir l'en-tête de son `requirements.txt`).
- [ ] **Step 4: Relancer** `make test` — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git add agents/agent-doc scripts && git commit -m "agent-doc: vault paths, reads and listing"`

---

### Task 3: `journal.py` et `Coffre.ecrire` — version, calme, journal

**Files:**
- Create: `agents/agent-doc/journal.py`
- Modify: `agents/agent-doc/coffre.py`
- Test: `scripts/test_agent_doc.py`

**Interfaces:**
- Consumes: `Coffre.chemin`, `lire`, `existe`, `genere`.
- Produces:
  - Format du journal (texte fixé, `journal.py` est du texte pur, sans S3) :

```markdown
---
type: journal-documentaliste
source: agent-doc
---
> [!info] Tenu par agent-doc. Pour revenir en arrière, demande-lui : « Annule le changement n° N ».

## Changements

### n° 1 — 2026-11-22 14:05 — écriture
- quoi : note créée `03 - Ressources/Poulet au cidre.md`
- pourquoi : recette donnée par chef-cuisinier
- appelant : nicolas-doc
- données : {"ops":[{"op":"ecrit","chemin":"03 - Ressources/Poulet au cidre.md","version":null}]}
```

  - `journal.ajouter(texte: str | None, quand: datetime, genre: str, quoi: str, pourquoi: str, appelant: str, donnees: dict) -> tuple[str, int]` : texte complet du journal (créé avec son en-tête si `None`) et numéro attribué = plus grand `### n° N` existant + 1. `quoi`, `pourquoi` et `appelant` sont aplatis (espaces à la place des sauts de ligne, 500 caractères au plus) ; `donnees` est écrit par `json.dumps(..., ensure_ascii=False, separators=(",", ":"))` sur une seule ligne.
  - `journal.entree(texte: str, numero: int) -> dict | None` : `{"numero", "genre", "donnees"}` de l'entrée, lue **seulement** dans une ligne qui commence par `- données : ` située sous le titre `### n° <numero> — ` ; `None` si absente.
  - `journal.annule_par(texte: str, numero: int) -> int | None` : numéro de l'entrée dont `donnees["annule"] == numero`.
  - Opérations (`ops`), seul vocabulaire des `données` : `{"op": "ecrit", "chemin", "version": "<clé _versions relative au préfixe>" | null}` (null = la note n'existait pas) et `{"op": "retire", "chemin", "version"}`.
  - `Coffre.verrou` : `threading.RLock` pris par toute méthode publique qui écrit. <!-- ponytail: global lock; one writer at a time is the point here -->
  - `Coffre.poser(chemin: str, contenu: str) -> dict` : si la note existe, écrit sa version (lecture stricte, `put_object` de la copie, **relecture** de la copie et comparaison) puis écrit le nouveau contenu ; rend l'op `ecrit`. Copie relue différente de l'original → `Refus("interne", "copie de sauvegarde illisible : rien n'a été modifié")`, sans toucher à la note. `poser` et `retirer` ne journalisent pas et ne vérifient pas le calme : l'appelant (`ecrire`, `annuler`, et `menage.py` au plan 4) le fait. Pour un fichier `generes` : écrit sans version, rend `{"op": "ecrit", "chemin", "version": None, "genere": True}`.
  - `Coffre.retirer(chemin: str) -> dict` : version écrite et relue, puis `delete_object` ; rend l'op `retire`. `Refus("absent")` si la note n'existe pas.
  - `Coffre.calme(chemin: str) -> None` : `Refus("calme", "la note a été modifiée il y a moins de N minutes (synchro Obsidian en cours ?) ; réessaie plus tard")` si elle existe et que son âge est inférieur à `quiet_minutes` ; rien pour un fichier `generes`.
  - `Coffre.journaliser(genre: str, quoi: str, pourquoi: str, appelant: str, donnees: dict) -> int` : lit le journal (absent → créé), `journal.ajouter`, écrit ; pas de version ni de règle de calme pour le journal lui-même.
  - `@dataclass Ecriture: chemin: str; effet: Literal["creee", "modifiee", "inchangee"]; journal: int | None; version: str | None`
  - `Coffre.ecrire(chemin: str, contenu: str, appelant: str, pourquoi: str = "") -> Ecriture` : sous le verrou — `chemin()`, contenu > 1 Mio → `Refus("trop_gros")`, `calme()`, contenu identique à l'existant → `inchangee` (rien d'écrit, pas de journal), sinon `poser` puis `journaliser("écriture", …)` (pas de journal pour un `generes`).

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_ecrire_cree_puis_versionne():
    c = coffre(); e = c.ecrire("03 - Ressources/P.md", "v1", "nicolas-doc", "essai")
    assert (e.effet, e.journal, e.version) == ("creee", 1, None)
    vieillir(c, "03 - Ressources/P.md", minutes=11)
    e = c.ecrire("03 - Ressources/P.md", "v2", "nicolas-doc")
    assert e.effet == "modifiee" and e.journal == 2 and re.fullmatch(r"_versions/03 - Ressources/P\.md/\d{8}T\d{6}Z\.md", e.version)
    assert c.s3.text("Maison/" + e.version) == "v1" and c.s3.text("Maison/03 - Ressources/P.md") == "v2"
    j = c.s3.text("Maison/Journal du documentaliste.md")
    assert j.count("### n° ") == 2 and "appelant : nicolas-doc" in j
    assert journal.entree(j, 2)["donnees"]["ops"] == [{"op": "ecrit", "chemin": "03 - Ressources/P.md", "version": e.version}]

def test_calme_et_inchangee():
    c = coffre({"Maison/a.md": b"x"}, age_minutes=3)
    assert refus(lambda: c.ecrire("a.md", "y", "x")) == "calme" and c.s3.puts == []
    vieillir(c, "a.md", minutes=11); assert c.ecrire("a.md", "x", "x").effet == "inchangee" and c.s3.puts == []

def test_genere_sans_version_ni_journal_ni_calme():
    G = "02 - Domaines/Loisirs/Musique/Discographie.md"; c = coffre({"Maison/" + G: b"vieux"}, age_minutes=1)
    e = c.ecrire(G, "neuf", "agent-musique"); assert (e.effet, e.journal, e.version) == ("modifiee", None, None)
    assert c.s3.puts == ["Maison/" + G]

def test_journal_inviolable_par_le_texte():
    c = coffre(); c.ecrire("a.md", "x", "n", 'ok\n### n° 99 — faux\n- données : {"ops":[]}')
    j = c.s3.text("Maison/Journal du documentaliste.md")
    assert journal.entree(j, 99) is None and j.count("\n### n° ") == 1 and c.ecrire("b.md", "x", "n").journal == 2

def test_ecritures_concurrentes_numerotees():
    c = coffre(); fils = [threading.Thread(target=c.ecrire, args=(f"n{i}.md", "x", "n")) for i in range(20)]
    [f.start() for f in fils]; [f.join() for f in fils]
    assert sorted(int(n) for n in re.findall(r"^### n° (\d+)", c.s3.text("Maison/Journal du documentaliste.md"), re.M)) == list(range(1, 21))

def test_version_illisible_rien_n_est_ecrase():        # FakeS3 dont get_object de la copie rend autre chose que ce qui a été écrit
    c = coffre({"Maison/a.md": b"precieux"}, s3=S3QuiCorromptLesVersions)
    assert refus(lambda: c.ecrire("a.md", "neuf", "n")) == "interne" and c.s3.text("Maison/a.md") == "precieux"
```

- [ ] **Step 2: Lancer** `make test` — attendu : ÉCHEC.
- [ ] **Step 3: Implémenter** `journal.py` puis les méthodes de `Coffre`.
- [ ] **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "agent-doc: writes with version, quiet rule and numbered journal"`

---

### Task 4: `Coffre.annuler`

**Files:**
- Modify: `agents/agent-doc/coffre.py`
- Test: `scripts/test_agent_doc.py`

**Interfaces:**
- Consumes: `journal.entree`, `journal.annule_par`, `poser`, `retirer`, `calme`, `journaliser`.
- Produces: `Coffre.annuler(numero: int, appelant: str) -> tuple[int, list[str]]` (numéro de l'entrée d'annulation, chemins touchés). Sous le verrou : entrée absente → `Refus("absent")` ; déjà annulée → `Refus("deja_annule", "déjà annulé par le n° M")` ; `calme()` sur **tous** les chemins avant d'agir ; puis les ops de l'entrée **en ordre inverse** : `ecrit` avec version → `poser(chemin, <contenu de la version>)` ; `ecrit` sans version (création) → `retirer(chemin)` (si la note a déjà disparu : rien) ; `retire` → `poser(chemin, <contenu de la version>)` ; op `genere` → ignorée. Journalise `("annulation", …, donnees={"annule": numero, "ops": [<ops produites>]})`. Annuler une annulation est une annulation ordinaire.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_annuler_une_modification_puis_l_annulation():
    c = coffre({"Maison/a.md": b"v1"}, age_minutes=60); c.ecrire("a.md", "v2", "n")      # n° 1
    vieillir(c, "a.md", 11); n, chemins = c.annuler(1, "nicolas-doc")
    assert (n, chemins) == (2, ["a.md"]) and c.s3.text("Maison/a.md") == "v1"
    assert refus(lambda: c.annuler(1, "n")) == "deja_annule"
    vieillir(c, "a.md", 11); c.annuler(2, "n"); assert c.s3.text("Maison/a.md") == "v2"

def test_annuler_une_creation_archive_la_note():
    c = coffre(); c.ecrire("a.md", "x", "n"); vieillir(c, "a.md", 11); c.annuler(1, "n")
    assert "Maison/a.md" not in c.s3.data and any(k.startswith("Maison/_versions/a.md/") for k in c.s3.data)

def test_annuler_refuse_si_calme_ou_inconnu():
    c = coffre(); c.ecrire("a.md", "x", "n")
    assert refus(lambda: c.annuler(1, "n")) == "calme" and refus(lambda: c.annuler(7, "n")) == "absent"
```

- [ ] **Step 2: Lancer** — attendu : ÉCHEC. **Step 3: Implémenter.** **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "agent-doc: undo a journal entry from _versions"`

---

### Task 5: `clients.py` et mode déterministe

**Files:**
- Create: `agents/agent-doc/clients.py`, `agents/agent-doc/agent.py`
- Test: `scripts/test_agent_doc.py`

**Interfaces:**
- Consumes: `Coffre`, `Refus`, `Ecriture`.
- Produces:
  - `class ErreurRag(Exception)` ; `class Rag(url: str, jeton: str, jeton_reindexer: str, source: str, poster=httpx.post, lire=httpx.get)` — délai 30 s, toute erreur HTTP ou réseau devient `ErreurRag` ; méthodes, une par ligne du tableau « agent-doc → RAG maître » de l'index : `chercher(question: str, filtres: dict | None = None, limite: int = 8) -> list[dict]` (ajoute toujours `sources: [self.source]`), `lister(question: str, filtres: dict | None = None) -> dict`, `expliquer(nom: str) -> dict`, `doublons(seuil: float, limite: int = 50) -> list[dict]`, `orphelines() -> list[dict]`, `liens_entrants(chemin: str) -> list[str]` (chemins, sans le préfixe `<source>:` ; `ErreurRag` si `etat != "ok"`), `document(chemin: str) -> dict`, `liens_casses() -> list[dict]`, `reindexer() -> bool` (vrai sur 202, **faux sans lever** sinon), `etat() -> dict`.
  - `class ErreurPortail(Exception)` avec `code: int` ; `class Portail(url: str, jeton: str, poster=httpx.post)` ; `demander_doc(depot: str, demande: str, appelant: str) -> dict` (le corps et la réponse du contrat) ; jeton vide → `ErreurPortail(503, "non configuré")` sans appel réseau.
  - `@dataclass Contexte: coffre: Coffre; rag: Rag; portail: Portail; llm: Callable[[list, list], dict]; config: dict`
  - `agent.traiter(texte: str, appelant: str, ctx: Contexte) -> str` : si `texte.strip()` se décode en un objet JSON qui a une clé `action` → `json.dumps(agent.deterministe(d, appelant, ctx), ensure_ascii=False)` ; sinon → `agent.libre(texte, appelant, ctx)` (tâche 6 ; ici, un bouchon qui lève `NotImplementedError`).
  - `agent.deterministe(d: dict, appelant: str, ctx: Contexte) -> dict` : actions `ecrire`, `lire`, `lister`, `annuler` du contrat (champs exactement ceux du tableau ; champ en trop, manquant ou d'un autre type que chaîne — entier pour `numero` — → `refus: "demande"` ; action inconnue → `refus: "demande"`). Un `Refus` devient `{"ok": false, "refus": code, "message": message}` ; toute autre exception devient `refus: "interne"` avec un message sans détail (le détail va au journal du conteneur, sans contenu de note). Après une écriture ou une annulation qui a changé quelque chose : `ctx.rag.reindexer()` → `"reindexation": "demandee" | "echec"`.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_deterministe_ecrire_lire_lister_annuler():
    ctx = contexte(); d = lambda **k: json.loads(agent.traiter(json.dumps(k), "agent-musique", ctx))
    r = d(action="ecrire", chemin="02 - Domaines/Loisirs/Musique/A - B (CD).md", contenu="x", pourquoi="disque")
    assert r == {"ok": True, "action": "ecrire", "chemin": "02 - Domaines/Loisirs/Musique/A - B (CD).md",
                 "effet": "creee", "journal": 1, "version": None, "reindexation": "demandee"}
    assert ctx.rag.reindexations == 1 and "appelant : agent-musique" in ctx.coffre.s3.text("Maison/Journal du documentaliste.md")
    assert d(action="lire", chemin=r["chemin"])["contenu"] == "x"
    assert d(action="lister", dossier="02 - Domaines")["notes"] == [r["chemin"]]
    assert d(action="ecrire", chemin=r["chemin"], contenu="y")["refus"] == "calme"
    vieillir(ctx.coffre, r["chemin"], 11); assert d(action="annuler", numero=1)["ok"] is True

def test_deterministe_demandes_invalides():
    ctx = contexte(); d = lambda **k: json.loads(agent.traiter(json.dumps(k), "x", ctx))
    for k in ({"action": "ecrire", "chemin": "a.md"}, {"action": "ecrire", "chemin": "a.md", "contenu": 3},
              {"action": "ecrire", "chemin": "a.md", "contenu": "x", "extra": 1}, {"action": "detruire"}, {"action": "annuler", "numero": "1"}):
        assert d(**k) == {"ok": False, "refus": "demande", "message": ANY}, k
    assert d(action="ecrire", chemin="../x.md", contenu="x")["refus"] == "chemin"

def test_rag_injoignable_l_ecriture_deterministe_tient():
    ctx = contexte(rag=RagEnPanne()); r = json.loads(agent.traiter('{"action":"ecrire","chemin":"a.md","contenu":"x"}', "n", ctx))
    assert r["ok"] is True and r["reindexation"] == "echec"

def test_un_texte_qui_ressemble_a_du_json_part_en_mode_libre():
    for t in ('{"cle": "valeur"} que veut dire ce JSON ?', '{"question": "x"}', "{pas du json", "[1, 2]"):
        with raises(NotImplementedError): agent.traiter(t, "n", contexte())

def test_clients(…):       # serveur HTTP local (comme le faux LiteLLM de test_agent_musique.py)
    # Rag : en-tête Bearer de lecture sur les lectures, Bearer de réindexation sur /reindexer ; `sources` ajouté à /chercher ;
    # liens_entrants rend ["a.md"] pour {"etat":"ok","entrants":[{"cle":"coffre-maison:a.md"}]} ; 500 -> ErreurRag ; reindexer() rend False sur 503
    # Portail : corps exact du contrat ; 404 -> ErreurPortail(code=404) ; jeton vide -> ErreurPortail(503) sans requête
```

- [ ] **Step 2: Lancer** — attendu : ÉCHEC. **Step 3: Implémenter.** **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "agent-doc: RAG and Portail clients, deterministic mode"`

---

### Task 6: Mode libre — répondre et consigner

**Files:**
- Modify: `agents/agent-doc/agent.py`
- Test: `scripts/test_agent_doc.py`

**Interfaces:**
- Consumes: `Contexte`, `Rag`, `Coffre.ecrire`, `Coffre.lire`, `Coffre.existe`, `Coffre.lister`.
- Produces: `agent.libre(texte: str, appelant: str, ctx: Contexte) -> str` — boucle d'outils sur le modèle de `disques.repondre` (au plus `config["max_tours"]` appels au modèle, arguments d'outil : un objet de **chaînes**, une erreur d'outil est rendue au modèle en texte et ne casse pas la boucle).
  - Outils offerts au modèle (noms du spec) :
    - `rag_chercher(question, type?, tags?, dossier?)` → `ctx.rag.chercher` ; rend au modèle, par résultat : `chemin`, `fil`, `texte`, `origine`.
    - `rag_lister(question, type?, tags?, dossier?)` → `ctx.rag.lister` ; `rag_expliquer(sujet)` → `ctx.rag.expliquer`.
    - `lire_note(chemin)` → `ctx.coffre.lire` (lecture exacte dans S3).
    - `ecrire_note(chemin, type, tags, corps, mode, origine?, confirmer_nouvelle?)` — `tags` : liste séparée par des virgules ; `mode` : `creer` ou `completer` ; `confirmer_nouvelle` : `oui`.
    - `annuler_changement(numero)` → `ctx.coffre.annuler(int(numero), appelant)` (« Annule le changement n° N ») ; compté dans `Écrit :` (`- <chemins> (journal n° N', annule le n° N)`) ; `numero` non entier → erreur rendue au modèle.
    - `demander_doc_depot(depot, demande)` : ajouté à la tâche 7.
  - Règles de `ecrire_note`, **dans le code** :
    - `creer` : le premier segment du chemin doit être un des `dossiers` de la config (sinon erreur qui les liste) ; la note ne doit pas exister (sinon « existe déjà : utilise mode completer ») ; **anti-doublon** : `ctx.rag.chercher(<nom de la note> + " " + <300 premiers caractères du corps>, limite=anti_doublon.limite)` — `ErreurRag` → « refusé : le RAG ne répond pas, je ne crée pas de note sans vérifier les doublons » ; un résultat avec `cosinus >= anti_doublon.cosinus` et pas de `confirmer_nouvelle: oui` → « refusé : note proche `<chemin>` (similarité 0,91) ; complète-la (mode completer), ou rappelle avec confirmer_nouvelle: oui si c'est un autre sujet ». Contenu écrit = frontmatter construit par le code (`type`, `tags`, `source: <appelant>`, `date`, `origine` si donnée ; valeurs en `json.dumps` comme `_fm_ligne` d'agent-musique) + `# <nom de la note>` + corps ; un frontmatter en tête du `corps` du modèle est retiré.
    - `completer` : la note doit exister ; contenu = note actuelle (sans ses sauts de ligne finaux) + ligne vide + `corps` + un saut de ligne final ; frontmatter intouché.
    - Dans les deux cas `ctx.coffre.ecrire(chemin, contenu, appelant, pourquoi=<60 premiers caractères du message de l'appelant>)` ; un `Refus` est rendu au modèle (« refusé : <message> »).
  - Prompt système (rédaction libre, en français, mais il dit) : tu es le documentaliste du coffre Maison ; réponds seulement depuis les outils, sinon « Je n'ai pas trouvé cela dans vos notes. » ; pour consigner, cherche d'abord (`rag_chercher`) s'il existe une note à compléter, choisis le dossier PARA et le type, lis le modèle correspondant dans `06 - Modèles` avant d'écrire, relie avec des `[[liens]]` les notes trouvées ; ne prétends jamais avoir écrit sans l'appel ; n'écris pas de ligne « Sources : » ni « Écrit : ». Le prompt contient la liste des dossiers et celle des modèles (`ctx.coffre.lister(config["modeles"])`).
  - Fin de réponse ajoutée par le code : `Sources :` suivi d'une ligne `- <lien> (origine : <origine ou « — »>)` par note réellement rendue par un outil de lecture (dédoublonnées, dans l'ordre) ; `Écrit :` suivi d'une ligne `- <chemin> (journal n° N)` par écriture réussie. Toute ligne `Sources :` ou `Écrit :` du modèle est retirée avant. Après au moins une écriture réussie : `ctx.rag.reindexer()` une fois, et « (réindexation non confirmée) » ajouté à la ligne `Écrit :` si elle échoue.
  - Tours épuisés : « Je n'ai pas pu conclure en 8 tours d'outils ; précise ta demande. », suivi de `Écrit :` si quelque chose a été écrit.

- [ ] **Step 1: Écrire les tests qui échouent** (le faux modèle rejoue une liste de messages, comme `call(...)` dans `test_agent_musique.py`)

```python
def test_repondre_avec_sources_ajoutees_par_le_code():
    ctx = contexte(llm=scenario(outil("rag_chercher", question="poulet"), "Tu as le poulet au cidre.\nSources : inventée"),
                   rag=FauxRag(chercher=[res("03 - Ressources/Poulet au cidre.md", origine="https://exemple.org/r", cosinus=0.7)]))
    t = agent.libre("quelles recettes de poulet ?", "nicolas-doc", ctx)
    assert "inventée" not in t and t.endswith("Sources :\n- obsidian://open?vault=Maison&file=03%20-%20Ressources/Poulet%20au%20cidre (origine : https://exemple.org/r)")

def test_consigner_cree_avec_le_frontmatter_du_code():
    ctx = contexte(llm=scenario(outil("ecrire_note", chemin="03 - Ressources/Tarte.md", type="recette", tags="dessert, pommes",
                   corps="---\nsource: pirate\n---\nPommes, pâte.", mode="creer", origine="Livre de mamie"), "C'est noté."))
    t = agent.libre("consigne cette recette : …", "chef-cuisinier", ctx)
    n = ctx.coffre.s3.text("Maison/03 - Ressources/Tarte.md")
    assert n.startswith('---\ntype: "recette"\ntags: ["dessert", "pommes"]\nsource: "chef-cuisinier"\ndate: "2026-11-22"\norigine: "Livre de mamie"\n---\n# Tarte\n')
    assert "pirate" not in n and "Écrit :\n- 03 - Ressources/Tarte.md (journal n° 1)" in t and ctx.rag.reindexations == 1

def test_anti_doublon_refuse_puis_confirme():
    proche = FauxRag(chercher=[res("03 - Ressources/Tarte aux pommes.md", cosinus=0.91)])
    a = dict(chemin="03 - Ressources/Tarte.md", type="recette", tags="", corps="x", mode="creer")
    ctx = contexte(llm=scenario(outil("ecrire_note", **a), "Elle existe déjà."), rag=proche); agent.libre("…", "n", ctx)
    assert ctx.coffre.s3.puts == [] and "Tarte aux pommes.md" in dernier_resultat_d_outil(ctx) and "0,91" in dernier_resultat_d_outil(ctx)
    ctx = contexte(llm=scenario(outil("ecrire_note", **a, confirmer_nouvelle="oui"), "Créée."), rag=proche); agent.libre("…", "n", ctx)
    assert "Maison/03 - Ressources/Tarte.md" in ctx.coffre.s3.data

def test_rag_en_panne_aucune_creation():
    ctx = contexte(llm=scenario(outil("ecrire_note", chemin="03 - Ressources/T.md", type="x", tags="", corps="x", mode="creer"), "Impossible."), rag=RagEnPanne())
    agent.libre("…", "n", ctx); assert ctx.coffre.s3.puts == [] and "RAG" in dernier_resultat_d_outil(ctx)

def test_completer_garde_le_frontmatter_et_versionne():
    ctx = contexte({"Maison/03 - Ressources/T.md": b"---\ntype: recette\n---\n# T\nA"}, age_minutes=60,
                   llm=scenario(outil("ecrire_note", chemin="03 - Ressources/T.md", type="", tags="", corps="B", mode="completer"), "Complétée."))
    agent.libre("ajoute B", "n", ctx)
    assert ctx.coffre.s3.text("Maison/03 - Ressources/T.md") == "---\ntype: recette\n---\n# T\nA\n\nB\n"
    assert any(k.startswith("Maison/_versions/03 - Ressources/T.md/") for k in ctx.coffre.s3.data)

def test_annuler_en_texte_libre():
    ctx = contexte({"Maison/a.md": b"v1"}, age_minutes=60); ctx.coffre.ecrire("a.md", "v2", "n"); vieillir(ctx.coffre, "a.md", 11)
    ctx.llm = scenario(outil("annuler_changement", numero="1"), "Annulé.")
    t = agent.libre("Annule le changement n° 1", "nicolas-doc", ctx)
    assert ctx.coffre.s3.text("Maison/a.md") == "v1" and "annule le n° 1" in t

def test_dossier_hors_para_existant_et_tours_epuises(): ...
    # creer dans "Divers/x.md" -> erreur qui liste les dossiers ; creer sur une note existante -> « existe déjà » ;
    # 8 appels d'outils de suite -> « 8 tours » et, si une écriture a eu lieu, la ligne « Écrit : »

def test_aucun_contenu_dans_les_journaux(capture_des_journaux):
    agent.libre("SECRET-DU-MESSAGE", "n", contexte(llm=scenario("réponse")))
    assert "SECRET-DU-MESSAGE" not in capture_des_journaux.texte
```

- [ ] **Step 2: Lancer** — attendu : ÉCHEC (`NotImplementedError`). **Step 3: Implémenter.** **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "agent-doc: answer from the RAG with sources, record with code-enforced rules"`

---

### Task 7: Outil `demander_doc_depot`

**Files:**
- Modify: `agents/agent-doc/agent.py`
- Test: `scripts/test_agent_doc.py`

**Interfaces:**
- Consumes: `Portail.demander_doc`, `ErreurPortail`.
- Produces: outil `demander_doc_depot(depot, demande)` dans la boucle ; `depot` réduit au nom (`nicolaslallier/Infra` et `https://github.com/nicolaslallier/Infra` → `Infra`). Résultats rendus au modèle : succès → « tâche de documentation créée (ou déjà ouverte) : <lien> ; la PR sera à fusionner par Nicolas » ; 404 → « je ne peux pas : aucun projet Portail pour le dépôt <depot> » ; 409, 429 → le message du Portail ; 503 ou réseau → « je ne peux pas : le Portail n'est pas joignable ou pas configuré ». Le lien est aussi ajouté par le code à la fin de la réponse (`Tâche : <lien>`), même si le modèle l'oublie.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_demander_doc_depot():
    ctx = contexte(llm=scenario(outil("demander_doc_depot", depot="nicolaslallier/Infra", demande="Documente la pile mcp"), "Demandé."),
                   portail=FauxPortail({"tache_id": 7, "projet_id": 2, "lien": "https://portail/projets/2/kanban", "existante": False}))
    t = agent.libre("documente la pile mcp du dépôt Infra", "nicolas-doc", ctx)
    assert ctx.portail.appels == [("Infra", "Documente la pile mcp", "nicolas-doc")] and t.endswith("Tâche : https://portail/projets/2/kanban")

def test_demander_doc_depot_sans_projet_ou_sans_portail():
    for erreur, mot in ((ErreurPortail(404, "aucun projet"), "aucun projet Portail"), (ErreurPortail(503, "non configuré"), "pas joignable")):
        ctx = contexte(llm=scenario(outil("demander_doc_depot", depot="X", demande="d"), "Impossible."), portail=FauxPortail(erreur))
        agent.libre("…", "n", ctx); assert mot in dernier_resultat_d_outil(ctx)
```

- [ ] **Step 2: Lancer** — attendu : ÉCHEC. **Step 3: Implémenter.** **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "agent-doc: ask the Portail for a doc-only task"`

---

### Task 8: `server.py`, image, identité de l'appelant

**Files:**
- Create: `agents/agent-doc/server.py`, `agents/agent-doc/Dockerfile`
- Modify: `scripts/portainer-images.sh`
- Test: `scripts/test_agent_doc.py`

**Interfaces:**
- Consumes: `agent.traiter`, `Coffre`, `Rag`, `Portail`, `config.yaml`.
- Produces:
  - `server.py`, calqué sur `agents/agent-musique/server.py` : carte `agent-doc` (description en français : « Le documentaliste du coffre Maison : répond depuis le RAG, consigne, documente un dépôt (gemma4, local). »), middleware `RequireToken` identique (401 sans `X-Agent-Token`, `/health` ouvert), `llm(messages, tools)` identique avec `AGENT_DOC_LITELLM_KEY`.
  - `appelant_de(entetes: dict[bytes, bytes]) -> str` : valeur de `x-litellm-user-id`, partie avant `@`, gardée si elle correspond à `^[a-z0-9-]{1,40}$`, sinon `"inconnu"`. Le middleware la range dans une `contextvars.ContextVar` (`APPELANT`) lue par l'exécuteur ; l'exécuteur appelle `agent.traiter` par `asyncio.to_thread` (qui copie le contexte).
  - Démarrage refusé (`SystemExit` nommant la variable) si `AGENT_DOC_LITELLM_KEY`, `AGENT_DOC_TOKEN`, `AGENT_DOC_S3_SECRET_KEY`, `AGENT_DOC_RAG_JETON` ou `AGENT_DOC_RAG_JETON_REINDEXER` est vide. `AGENT_DOC_PORTAIL_JETON` vide est accepté. Variables lues : celles du tableau « Secrets et variables » de l'index.
  - `Dockerfile` : celui d'agent-musique (même image de base épinglée, `USER 10001`), `COPY server.py agent.py coffre.py journal.py clients.py config.yaml ./`.
  - `scripts/portainer-images.sh` : `build "infra-agent-doc:1" "https://github.com/nicolaslallier/Infra.git#main:agents/agent-doc"`.

- [ ] **Step 1: Écrire les tests qui échouent** (le serveur est lancé en sous-processus contre un faux LiteLLM, un faux RAG et un S3 de test, comme le fait `test_agent_musique.py` pour son serveur)

```python
def test_appelant_de():
    f = server.appelant_de
    assert f({b"x-litellm-user-id": b"agent-musique@agents.invalid"}) == "agent-musique"
    for v in (b"", b"Nicolas Lallier@x", b"a" * 41 + b"@x", b"../x@y", b"\xff"):
        assert f({b"x-litellm-user-id": v}) == "inconnu", v
    assert f({}) == "inconnu"

def test_serveur_exige_le_jeton_et_transmet_l_appelant(serveur):
    assert serveur.post("/", MESSAGE_ECRIRE).status == 401 and serveur.get("/health").status == 200
    r = serveur.post("/", MESSAGE_ECRIRE, entetes={"X-Agent-Token": JETON, "X-LiteLLM-User-Id": "nicolas-doc@agents.invalid"})
    assert json.loads(texte_a2a(r))["ok"] is True and "appelant : nicolas-doc" in serveur.s3_texte("Maison/Journal du documentaliste.md")

def test_demarrage_refuse_sans_secret():
    for var in ("AGENT_DOC_TOKEN", "AGENT_DOC_LITELLM_KEY", "AGENT_DOC_S3_SECRET_KEY", "AGENT_DOC_RAG_JETON", "AGENT_DOC_RAG_JETON_REINDEXER"):
        assert var in lancer_sans(var).stderr

def test_seul_coffre_ecrit_dans_s3():
    for f in ("agent.py", "clients.py", "journal.py", "server.py"):
        src = open(os.path.join(AGENT, f)).read()
        assert "put_object" not in src and "delete_object" not in src and "copy_object" not in src, f

def test_litellm_transmet_l_identite_de_l_appelant():      # garde-fou de montée de version, lancé avec litellm[proxy]==$LITELLM_VERSION
    from litellm.proxy.agent_endpoints.a2a_endpoints import _caller_identity_headers
    from litellm.proxy._types import UserAPIKeyAuth
    h = _caller_identity_headers(UserAPIKeyAuth(user_id="nicolas-doc@agents.invalid", team_id="t"))
    assert h["X-LiteLLM-User-Id"] == "nicolas-doc@agents.invalid"
```

  Le dernier test va dans `scripts/test_cloud_pii.py` (déjà lancé avec le paquet LiteLLM épinglé), pas dans `test_agent_doc.py`.

- [ ] **Step 2: Lancer** `make test` — attendu : ÉCHEC. **Step 3: Implémenter.** **Step 4: Relancer** — attendu : SUCCÈS ; `docker build agents/agent-doc` réussit en local.
- [ ] **Step 5: Commit** `git commit -am "agent-doc: A2A server, image, caller identity from X-LiteLLM-User-Id"`

---

### Task 9: Branchement — LiteLLM, politique, nginx, piles, variables

**Files:**
- Modify: `litellm/config.yaml`, `litellm/mcp_policy.yaml`, `nginx/conf.d/litellm.conf`, `nginx/conf.d/portail.conf`, `docker-compose.mcp.yml`, `docker-compose.platform.yml`, `.env.example`, `scripts/check-env.sh`, `scripts/ci-fake-env.sh`
- Test: `scripts/test_mcp_policy.py`, `scripts/test_litellm_routes.py`, `scripts/test_portail_vhost.py`

**Interfaces:**
- Produces (valeurs du contrat) :
  - `litellm/config.yaml`, sous `agents:` — entrée `agent-doc`, copie de celle d'agent-musique : `static_headers: {X-Agent-Token: os.environ/AGENT_DOC_TOKEN}`, `url: http://agent-doc:8000`.
  - `litellm/mcp_policy.yaml` : `agent-doc: {teams: [agents]}` avec le commentaire « local-only teams: the vault content it returns must never reach a cloud model ».
  - `nginx/conf.d/litellm.conf` : bloc `location = /v1/a2a/agent-doc/message/send`, copie de celui d'agent-musique.
  - `nginx/conf.d/portail.conf` : `location ^~ /service { return 404; }` à côté de `location ^~ /interne`.
  - `docker-compose.mcp.yml` : service `agent-doc`, copie d'`agent-musique` (image `infra-agent-doc:1`, `pull_policy: never`, même contrôle de santé), variables du tableau « Secrets et variables » ; secrets en `${VAR:?…}` sauf `AGENT_DOC_PORTAIL_JETON: ${AGENT_DOC_PORTAIL_JETON:-}` ; `AGENT_DOC_NUIT: "off"`.
  - `docker-compose.platform.yml` : `AGENT_DOC_TOKEN` dans l'environnement de `litellm`, à côté d'`AGENT_MUSIQUE_TOKEN`.
  - `.env.example`, `check-env.sh`, `ci-fake-env.sh` : `AGENT_DOC_LITELLM_KEY`, `AGENT_DOC_TOKEN`, `AGENT_DOC_S3_SECRET_KEY`, `AGENT_DOC_RAG_JETON`, `AGENT_DOC_RAG_JETON_REINDEXER` (obligatoires), `AGENT_DOC_PORTAIL_JETON` (facultative, absente de `check-env`), avec en commentaire l'égalité à garder avec `infra/rag` et `infra/portail`.

- [ ] **Step 1: Adapter les tests pour qu'ils échouent**
  - `test_mcp_policy.py`, `test_policy_agents_are_registered_built_and_keyed` : la politique attendue devient `{"agent-test": {"teams": ["admin"]}, "agent-musique": {"teams": ["admin", "apps"]}, "agent-doc": {"teams": ["agents"]}}` ; ajouter :

```python
def test_agent_doc_is_open_to_local_only_teams():
    full = mp.load_policy(os.path.join(ROOT, "litellm", "mcp_policy.yaml"))
    teams = _y("litellm", "teams.yaml")["teams"]; ad = hashlib.sha256(b"agent-doc").hexdigest()
    allowed = full["agents"]["agent-doc"]["teams"]
    assert allowed and all(not [m for m in teams[t]["models"] if m.startswith("cloud-")] for t in allowed)
    assert mp.agent_refusal(full, "agents", ad) is None
    for team in ("admin", "apps", "famille", "dev-sessions", None):
        assert mp.agent_refusal(full, team, ad) is not None, team
    reg = {a["agent_name"]: a for a in _y("litellm", "config.yaml")["agents"]}["agent-doc"]
    assert reg["static_headers"] == {"X-Agent-Token": "os.environ/AGENT_DOC_TOKEN"} and reg["agent_card_params"]["url"] == "http://agent-doc:8000"
```

  - `test_litellm_routes.py` : la liste exacte des `location =` rouvertes reçoit `/v1/a2a/agent-doc/message/send` ; les chemins `/v1/a2a/agent-doc/.well-known/agent-card.json` et `/v1/a2a/agent-doc/message/stream` rejoignent ceux qui restent fermés.
  - `test_portail_vhost.py` : `assert re.search(r"location \^~ /service \{\s*return 404;", CONF)`.
  - Le test qui vérifie que la pile `mcp` ne publie aucun port et épingle ses images, et celui des balises d'image construites par `portainer-images.sh`, couvrent `agent-doc` (les étendre s'ils énumèrent les services).
- [ ] **Step 2: Lancer** `make test` — attendu : ÉCHEC sur ces tests.
- [ ] **Step 3: Modifier** les fichiers.
- [ ] **Step 4: Relancer** `scripts/ci-fake-env.sh`, `make check-env`, `make test` — attendu : tout vert ; `docker compose -f docker-compose.mcp.yml config -q` sort en 0 avec le `.env` factice.
- [ ] **Step 5: Commit** `git commit -am "agent-doc: registered in LiteLLM (team agents only), nginx route, mcp stack, env"`

---

### Task 10: Documentation, étapes manuelles, PR, vérification en service

**Files:**
- Modify: `README.md`, `CLAUDE.md`

- [ ] **Step 1: Écrire** dans `CLAUDE.md` une entrée `agent-doc` sur le modèle de celle d'agent-musique (ce qu'il fait, ses clés, ses règles tenues par le code, « team `agents` only »), et dans le README (« LiteLLM: memory, skills, agents ») sa description, le tableau du mode déterministe, et la section « Setup of `agent-doc` » ci-dessous. Mettre à jour les tests de docs (`test_docs_say_…`) pour qu'ils cherchent `/v1/a2a/agent-doc/message/send` et les noms des secrets.
- [ ] **Step 2: Écrire la section « Setup of `agent-doc` »** — étapes **faites par Nicolas**, sur l'hôte, avant la fusion, dans cet ordre :
  1. Prérequis : le RAG maître répond (`docker exec rag-rag-1 … /sante`), avec le plan 1 du projet 2 déployé.
  2. Secrets : `make vault-cli args="kv patch -mount=infra env AGENT_DOC_TOKEN=$(openssl rand -hex 32) AGENT_DOC_S3_SECRET_KEY=$(openssl rand -hex 24) AGENT_DOC_RAG_JETON=<RAG_JETON d'infra/rag> AGENT_DOC_RAG_JETON_REINDEXER=<RAG_JETON_REINDEXER d'infra/rag>"` (et `AGENT_DOC_PORTAIL_JETON=<PORTAIL_JETON_SERVICE>` si le plan 2 est déployé) ; `make vault-env`.
  3. `./scripts/provision-s3.sh agent-doc obsidian 0 Read,Write,List`.
  4. `make litellm-provision` : il imprime une fois les clés `agent-doc` et `nicolas-doc` ; ranger la première dans `infra/env` (`AGENT_DOC_LITELLM_KEY`), garder la seconde pour les essais ; `make vault-env`.
  5. Fusionner. Le déploiement construit `infra-agent-doc:1` et redéploie `mcp`, `platform` et `infra`.
- [ ] **Step 3: Écrire la vérification en service** (à la suite, dans le README), avec `KEY=<clé nicolas-doc>` et une note d'essai `00 - Inbox/Essai agent-doc.md` :
  1. `ecrire` en mode déterministe → `effet: creee`, `journal: 1` ; la note et `Journal du documentaliste.md` arrivent dans Obsidian sur le Mac à la synchro suivante ; l'entrée dit `appelant : nicolas-doc` (**c'est la preuve que `X-LiteLLM-User-Id` arrive**).
  2. `lire` deux fois de suite, puis `ecrire` un nouveau contenu après 10 minutes, puis `lire` : le dernier `lire` rend le nouveau contenu (**le cache de réponses de LiteLLM ne s'applique pas aux appels A2A** ; s'il s'applique, s'arrêter et le dire à Nicolas).
  3. `ecrire` aussitôt après → `refus: calme`.
  4. `annuler` le n° 2 → l'ancien contenu revient ; `_versions/` contient deux copies (`aws s3 ls` avec l'identité `rag`, ou l'interface S3).
  5. Une question en texte libre (« quelles recettes j'ai pour le poulet ? ») → réponse avec `Sources :`.
  6. Une consignation en texte libre → note créée avec `source: nicolas-doc`, visible dans `rag chercher` en moins de 10 minutes.
  7. Avec la clé `nicolas-admin` (équipe `admin`) : 403 `agent-policy`.
  8. `docker logs mcp-agent-doc-1` ne contient le texte d'aucune note.
- [ ] **Step 4: Commit et PR en brouillon** `git commit -am "docs: agent-doc, setup and live checks"` puis `gh pr create --draft`. Ne pas fusionner : la fusion suit les étapes manuelles de Nicolas.
