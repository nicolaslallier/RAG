# agent-doc — plan 4 : le ménage (déplacer, fusionner, archiver, passe de nuit) (dépôt Infra)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Ce plan s'exécute dans le dépôt `nicolaslallier/Infra`.** Le recopier dans `Infra/docs/superpowers/plans/`. Branche depuis `origin/main` à jour, PR en brouillon, jamais de push sur `main`. **La tâche 1 est bloquante : aucune des tâches 2 à 7 ne commence avant que Nicolas ait lu le compte rendu du spike et choisi la branche.**

**Goal:** Qu'agent-doc garde le coffre rangé seul : il déplace, fusionne et archive sans perdre une ligne ni casser un lien, au moment d'écrire et chaque nuit après la réindexation du RAG, dans la limite de `AGENT_DOC_NUIT_MAX` changements, tous au journal et tous annulables.

**Architecture:** `liens.py` réécrit les `[[liens]]` (texte pur). `menage.py` compose les primitives de `coffre.py` (`poser`, `retirer`, `calme`, `journaliser`, sous `coffre.verrou`) en trois opérations — déplacer, fusionner, archiver — et porte la passe de nuit : candidats pris au RAG et à l'Inbox, décision confirmée par gemma en JSON, exécution par le code. Un fil du serveur interroge `GET /etat` du RAG et lance la passe quand la réindexation nocturne est finie.

**Tech Stack:** celle du plan 3. Aucune dépendance ajoutée.

**Spec:** `docs/superpowers/specs/2026-11-21-agent-doc-design.md` du dépôt RAG (« Règles appliquées par le code », « Passe de nuit », « Décisions prises par Claude » : fusion, déplacement sans redirection, zones protégées). Contrat : `2026-11-22-agent-doc-0-index.md`. Prérequis : plan 3 déployé et vérifié.

## Global Constraints

- Toutes celles du plan 3 (jamais de suppression définitive, version relue avant tout retrait, calme, journal, seul `coffre.py` écrit dans S3, rien ne sort du homelab, aucun contenu de note dans les journaux du conteneur).
- **Zones protégées** : une note pour laquelle `Coffre.protege()` est vrai n'est jamais déplacée, fusionnée, archivée, ni réécrite pour corriger un lien. Si un déplacement exige de réécrire un lien dans une note protégée, le déplacement est refusé.
- **Aucun lien cassé par le code** : une opération qui retire une note réécrit d'abord tous les liens entrants connus du RAG ; si le RAG ne répond pas, ou si une note à réécrire est en période de calme, l'opération entière est refusée, rien n'est écrit.
- **Fusion mécanique** : les deux contenus sont gardés en entier dans la note cible ; gemma n'écrit que le chapeau. Un échec de gemma ne bloque pas la fusion (pas de chapeau).
- **Une opération = une entrée de journal**, avec toutes ses `ops`, même interrompue (`"incomplet": true`).
- Passe de nuit : au plus `AGENT_DOC_NUIT_MAX` (10) changements et `nuit.max_examens` (30) appels au modèle ; appelant inscrit au journal : `nuit`.
- Ménage à la demande (« range le dossier Projets ») : hors périmètre.

## Review Focus

1. **Remotely Save renvoie la note retirée** (copie locale modifiée entre-temps, Mac éteint depuis trois jours) : le spike dit ce qui se passe ; la branche retenue ne laisse ni doublon de nom ni perte. → tâche 1.
2. **Panne S3 au milieu d'un déplacement** (nouvelle note écrite, ancienne pas encore retirée) : l'entrée de journal existe, marquée incomplète, et « annule » ramène l'état de départ. → test tâche 3.
3. **Formes de liens** : `[[Nom]]`, `[[dossier/Nom]]`, `[[Nom|alias]]`, `[[Nom#Titre]]`, `[[Nom#^bloc|alias]]`, `![[Nom]]`, casse différente ; un `[[Nomade]]` n'est pas un lien vers `Nom`. → tests tâche 2.
4. **gemma confirme un doublon qui n'en est pas un** : la fusion garde tout, et `annuler` rend les deux notes et les liens d'origine. → test tâche 4.
5. **Mêmes candidats refusés nuit après nuit** : une paire que gemma a jugée « pas un doublon » n'est pas réexaminée tant qu'aucune des deux notes n'a changé ; elle ne consomme plus le quota. → test tâche 6.

---

### Task 1: Spike — Remotely Save et la disparition d'un objet S3 (bloquant)

**Files:**
- Create: `docs/superpowers/spikes/2026-11-remotely-save-suppression.md`

Aucun code. Les manipulations touchent le vrai coffre et le Mac de Nicolas : **elles sont faites par Nicolas, ou par l'agent seulement avec son accord explicite, étape par étape**, sur des notes d'essai `00 - Inbox/Spike RS <lettre>.md` créées pour l'occasion. Rien d'autre n'est modifié. Accès S3 : identité `agent-doc` (plan 3), par `https://s3.infra.famillelallier.net`.

**Questions auxquelles le compte rendu répond, avec la manipulation et l'observation de chacune :**

- [ ] **Q1 — note inchangée sur le Mac.** Créer `Spike RS A.md` dans Obsidian, synchroniser, attendre 10 minutes, retirer l'objet de S3 (`aws s3 rm`), synchroniser. Observer : le fichier disparaît-il du Mac (et où va-t-il : corbeille d'Obsidian, corbeille du système, nulle part) ou revient-il dans S3 ?
- [ ] **Q2 — note modifiée sur le Mac après la dernière synchro.** Même chose avec `Spike RS B.md`, mais la modifier dans Obsidian **avant** de synchroniser. Observer : la version locale est-elle renvoyée (attendu : oui, et c'est le bon comportement — on ne perd pas ce que Nicolas vient d'écrire) ?
- [ ] **Q3 — Mac absent.** `Spike RS C.md` : retirer l'objet, puis ne synchroniser qu'après au moins une nuit (et, si possible, après une synchro d'un autre appareil). Même observation que Q1.
- [ ] **Q4 — déplacement.** `Spike RS D.md` : écrire le même contenu sous `03 - Ressources/Spike RS D.md` puis retirer l'ancien. Observer : un seul fichier sur le Mac, au nouvel endroit, sans copie « conflit ».
- [ ] **Q5 — `_versions/`.** Écrire `<préfixe>_versions/Spike RS E.md/20261122T000000Z.md`. Observer : arrive-t-il sur le Mac ? Relever dans les réglages du plugin : « synchroniser les fichiers et dossiers en `_` » (le coffre a déjà un dossier `_Claude`), la liste d'exclusions (expressions régulières), le seuil de protection contre les suppressions en masse, et la version du plugin. Si `_versions/` descend sur le Mac : trouver le réglage qui l'exclut **sans** exclure `_Claude` ni `_CLAUDE.md`, l'appliquer, refaire l'essai.
- [ ] **Q6 — droit de retirer.** L'identité `agent-doc` (`Read,Write,List`) peut-elle faire `DeleteObject` ? L'identité `rag` (`Read,List`) est-elle bien refusée ?
- [ ] **Q7 — rythme.** Quel est l'intervalle de synchro automatique du Mac ? La règle de calme (10 minutes) le couvre-t-elle ?

- [ ] **Nettoyer** : retirer toutes les notes `Spike RS *` du coffre et de `_versions/`.
- [ ] **Écrire le compte rendu** : une section par question (manipulation, observation, capture si utile), puis le **verdict** :

| Si | Alors |
|---|---|
| Q1, Q3 et Q4 : le fichier disparaît du Mac, rien ne revient | **Branche « retirer »** (celle du spec) : déplacer et fusionner retirent l'ancien fichier après copie dans `_versions/`. |
| Q1 ou Q3 : le fichier revient dans S3 | **Branche « rediriger »** : l'ancien chemin reçoit une note de redirection (`type: redirection`) au lieu d'être retiré ; l'Inbox ne se vide pas ; `archiver` n'est pas offert ; ouvrir une demande au projet 1 pour que `/doublons` et `/orphelines` ignorent `type: redirection`. |
| Q5 : `_versions/` descend sur le Mac et aucun réglage ne l'exclut | Arrêt : proposer à Nicolas un autre emplacement (préfixe hors du coffre ou autre bucket), mettre à jour le contrat (index, config du RAG) avant de continuer. |
| Q6 : `Write` ne permet pas de retirer | Étendre `scripts/provision-s3.sh` (action de suppression de SeaweedFS) dans la tâche 3. |

- [ ] **Commit, puis arrêt** : `git add docs/superpowers/spikes && git commit -m "spike: how Remotely Save propagates a removed S3 object"`. **Nicolas lit le compte rendu et confirme la branche.** Les tâches 3 à 5 n'implémentent que la branche retenue ; l'autre n'est pas écrite.

---

### Task 2: `liens.py` — réécrire les `[[liens]]`

**Files:**
- Create: `agents/agent-doc/liens.py`
- Modify: `agents/agent-doc/Dockerfile` (`COPY`)
- Test: `scripts/test_agent_doc.py`

**Interfaces:**
- Produces:
  - `cible_de(lien: str) -> str` : la cible d'un contenu de lien, sans `|alias` ni `#ancre` (`"Nom#Titre|a"` → `"Nom"`).
  - `pointe_vers(cible: str, chemin: str) -> bool` : vrai si `cible`, comparée sans casse et en NFC, vaut le chemin sans `.md`, le chemin avec `.md`, ou le seul nom de fichier sans `.md`.
  - `reecrire_liens(texte: str, ancien: str, nouveau: str) -> tuple[str, int]` : remplace la cible de chaque `[[…]]` et `![[…]]` qui `pointe_vers(ancien)` ; garde `#ancre` et `|alias`. Cible écrite : le nouveau nom seul si le lien était écrit par nom, le nouveau chemin sans `.md` s'il était écrit par chemin. Un lien écrit par nom dont le nom ne change pas n'est pas touché (compte 0). Rend le texte et le nombre de liens changés.
  - <!-- ponytail: links inside code blocks are rewritten too, and Markdown links [x](y.md) are not; add a code-fence skip if a note about Obsidian syntax ever gets mangled -->

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_reecrire_liens():
    r = lambda t, a="00 - Inbox/Nom.md", n="03 - Ressources/Nouveau.md": liens.reecrire_liens(t, a, n)
    assert r("[[Nom]] [[nom|x]] [[Nom#T]] [[Nom#^b|y]] ![[Nom]]") == ("[[Nouveau]] [[Nouveau|x]] [[Nouveau#T]] [[Nouveau#^b|y]] ![[Nouveau]]", 5)
    assert r("[[00 - Inbox/Nom]] [[00 - Inbox/Nom.md|x]]") == ("[[03 - Ressources/Nouveau]] [[03 - Ressources/Nouveau|x]]", 2)
    assert r("[[Nomade]] [[Autre/Nom2]] Nom [Nom](Nom.md)") == ("[[Nomade]] [[Autre/Nom2]] Nom [Nom](Nom.md)", 0)
    assert r("[[Nom]]", n="03 - Ressources/Nom.md") == ("[[Nom]]", 0)                       # même nom : rien à faire
    assert r("[[00 - Inbox/Nom]]", n="03 - Ressources/Nom.md") == ("[[03 - Ressources/Nom]]", 1)
    assert r("[[Café]]", a="Café.md", n="Bistro.md") == ("[[Bistro]]", 1)
```

- [ ] **Step 2: Lancer** `make test` — attendu : ÉCHEC. **Step 3: Implémenter.** **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "agent-doc: rewrite wikilinks to a moved note"`

---

### Task 3: `menage.deplacer` et `menage.archiver`

**Files:**
- Create: `agents/agent-doc/menage.py`
- Modify: `agents/agent-doc/Dockerfile` (`COPY`), `scripts/provision-s3.sh` (seulement si le spike, Q6, l'exige)
- Test: `scripts/test_agent_doc.py`

**Interfaces:**
- Consumes: `Coffre` (`verrou`, `chemin`, `lire`, `existe`, `protege`, `calme`, `poser`, `retirer`, `journaliser`), `Rag.liens_entrants`, `ErreurRag`, `liens.reecrire_liens`, `Contexte`.
- Produces:
  - Code de refus ajouté au contrat : `existe` (la destination existe déjà) et `rag` (le RAG ne répond pas : liens entrants inconnus).
  - `menage.deplacer(ctx, ancien: str, nouveau: str, appelant: str, pourquoi: str) -> int` (numéro de journal). Sous `ctx.coffre.verrou`, dans cet ordre, **toutes les vérifications avant la première écriture** : chemins valides et différents ; `ancien` existe (`absent`), n'est pas protégé (`protege`) ; `nouveau` n'existe pas (`existe`) et son premier segment est un des `dossiers` hors zones protégées ; `ctx.rag.liens_entrants(ancien)` (`ErreurRag` → `Refus("rag")`) ; pour chaque entrant, texte réécrit par `reecrire_liens` — s'il change : la note ne doit pas être protégée (`protege`) ni en période de calme (`calme`) ; `calme(ancien)`. Puis : `poser(nouveau, <texte d'ancien>)`, `poser(entrant, <texte réécrit>)` pour chaque entrant changé, enfin `retirer(ancien)`. Journal `("déplacement", "`ancien` → `nouveau`, N liens réécrits", …)` écrit dans un `finally` avec les ops faites ; si une exception a interrompu la suite : `donnees["incomplet"] = True` et l'exception est relevée en `Refus("interne")`.
  - `menage.archiver(ctx, chemin: str, appelant: str, pourquoi: str) -> Ecriture` : refus `absent`, `protege`, `calme` ; `retirer(chemin)` ; journal `("archivage", …)`. Rend `Ecriture(chemin, "archivee", <n° de journal>, <clé de la version>)` (`"archivee"` s'ajoute aux valeurs de `Ecriture.effet`). Les liens entrants ne sont pas réécrits (la note n'a pas de remplaçante) ; leur nombre est dit dans `quoi` (« N notes y renvoient encore »), 0 si le RAG ne répond pas.
  - **Branche « rediriger »** (seulement si le spike la retient) : à la place de `retirer(ancien)`, `poser(ancien, <note de redirection>)` avec le texte exact `---\ntype: redirection\nsource: agent-doc\n---\nDéplacée : [[<nouveau sans .md>]]\n` ; les liens entrants sont réécrits de la même façon ; `menage.archiver` n'existe pas.

- [ ] **Step 1: Écrire les tests qui échouent** (branche « retirer »)

```python
def test_deplacer_reecrit_les_liens_et_retire_l_ancien():
    ctx = contexte({"Maison/00 - Inbox/Idée.md": b"# Id\xc3\xa9e", "Maison/01 - Projets/P.md": "Voir [[Idée]] et [[00 - Inbox/Idée|ça]].".encode(),
                    "Maison/01 - Projets/Q.md": b"rien"}, age_minutes=60, rag=FauxRag(entrants={"00 - Inbox/Idée.md": ["01 - Projets/P.md", "01 - Projets/Q.md"]}))
    n = menage.deplacer(ctx, "00 - Inbox/Idée.md", "03 - Ressources/Idée neuve.md", "nuit", "tri de l'Inbox")
    s3 = ctx.coffre.s3
    assert "Maison/00 - Inbox/Idée.md" not in s3.data and s3.text("Maison/03 - Ressources/Idée neuve.md") == "# Idée"
    assert s3.text("Maison/01 - Projets/P.md") == "Voir [[Idée neuve]] et [[03 - Ressources/Idée neuve|ça]]."
    ops = journal.entree(s3.text("Maison/Journal du documentaliste.md"), n)["donnees"]["ops"]
    assert [(o["op"], o["chemin"]) for o in ops] == [("ecrit", "03 - Ressources/Idée neuve.md"), ("ecrit", "01 - Projets/P.md"), ("retire", "00 - Inbox/Idée.md")]
    vieillir_tout(ctx, 11); ctx.coffre.annuler(n, "nicolas-doc")
    assert s3.text("Maison/00 - Inbox/Idée.md") == "# Idée" and "[[Idée]]" in s3.text("Maison/01 - Projets/P.md") and "Maison/03 - Ressources/Idée neuve.md" not in s3.data

def test_deplacer_refuse_sans_rien_ecrire():
    base = {"Maison/00 - Inbox/A.md": b"a", "Maison/05 - Journal/2026-11-22.md": b"[[00 - Inbox/A]]", "Maison/03 - Ressources/B.md": b"b"}
    cas = [("protege", dict(entrants={"00 - Inbox/A.md": ["05 - Journal/2026-11-22.md"]}), "03 - Ressources/A2.md"),   # lien à réécrire en zone protégée
           ("existe", dict(entrants={}), "03 - Ressources/B.md"), ("rag", dict(panne=True), "03 - Ressources/A2.md"),
           ("chemin", dict(entrants={}), "Divers/A.md")]
    for code, rag, vers in cas:
        ctx = contexte(base, age_minutes=60, rag=FauxRag(**rag))
        assert refus(lambda: menage.deplacer(ctx, "00 - Inbox/A.md", vers, "n", "")) == code and ctx.coffre.s3.puts == [], code
    ctx = contexte(base | {"Maison/01 - Projets/P.md": b"[[00 - Inbox/A]]"}, age_minutes=60, rag=FauxRag(entrants={"00 - Inbox/A.md": ["01 - Projets/P.md"]}))
    vieillir(ctx.coffre, "01 - Projets/P.md", minutes=2)                 # Nicolas écrit dans P
    assert refus(lambda: menage.deplacer(ctx, "00 - Inbox/A.md", "03 - Ressources/A2.md", "n", "")) == "calme" and ctx.coffre.s3.puts == []
    assert refus(lambda: menage.deplacer(contexte({"Maison/06 - Modèles/M.md": b"m"}, age_minutes=60), "06 - Modèles/M.md", "03 - Ressources/M.md", "n", "")) == "protege"

def test_deplacement_interrompu_journalise_et_annulable():
    ctx = contexte({"Maison/00 - Inbox/A.md": b"a"}, age_minutes=60, s3=S3QuiRefuseDelete, rag=FauxRag(entrants={}))
    assert refus(lambda: menage.deplacer(ctx, "00 - Inbox/A.md", "03 - Ressources/A.md", "n", "")) == "interne"
    e = journal.entree(ctx.coffre.s3.text("Maison/Journal du documentaliste.md"), 1)
    assert e["donnees"]["incomplet"] is True and [o["op"] for o in e["donnees"]["ops"]] == ["ecrit"]
    vieillir_tout(ctx, 11); ctx.coffre.s3.delete_ok = True; ctx.coffre.annuler(1, "n")
    assert set(k for k in ctx.coffre.s3.data if "_versions" not in k and "Journal" not in k) == {"Maison/00 - Inbox/A.md"}

def test_archiver():
    ctx = contexte({"Maison/00 - Inbox/A.md": b"a"}, age_minutes=60, rag=FauxRag(entrants={"00 - Inbox/A.md": ["x.md"]}))
    e = menage.archiver(ctx, "00 - Inbox/A.md", "jarvis", "demandé"); assert "Maison/00 - Inbox/A.md" not in ctx.coffre.s3.data
    assert ctx.coffre.s3.text("Maison/" + e.version) == "a" and "1 notes y renvoient encore" in ctx.coffre.s3.text("Maison/Journal du documentaliste.md")
```

- [ ] **Step 2: Lancer** — attendu : ÉCHEC. **Step 3: Implémenter** la branche retenue par le spike. **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "agent-doc: move and archive notes, incoming links rewritten, one journal entry"`

---

### Task 4: `menage.fusionner`

**Files:**
- Modify: `agents/agent-doc/menage.py`
- Test: `scripts/test_agent_doc.py`

**Interfaces:**
- Consumes: celles de la tâche 3, plus `ctx.llm(messages, [])` pour le chapeau.
- Produces: `menage.fusionner(ctx, garder: str, absorber: str, appelant: str, pourquoi: str) -> int`. Mêmes vérifications préalables que `deplacer` pour `absorber` (existe, pas protégé, calme, liens entrants connus et réécrivables vers `garder`), plus : `garder` existe, n'est pas protégé, `calme(garder)`, et le contenu fusionné ne dépasse pas 1 Mio (`trop_gros`). Contenu de la note gardée après fusion, exact :

````text
<frontmatter de la note gardée, inchangé, s'il y en a un>
> [!summary] Fusion du <AAAA-MM-JJ>
> <chapeau de gemma, sauts de ligne remplacés par des espaces, 600 caractères au plus>

<corps de la note gardée, inchangé>

## Depuis « <nom de la note absorbée, sans .md> »

```yaml
<frontmatter de la note absorbée, tel quel ; bloc omis si elle n'en a pas>
```

<corps de la note absorbée, inchangé>
````

  Le bloc `> [!summary]` est omis si l'appel au modèle échoue ou rend un texte vide. Demande au modèle : les deux corps, consigne « Écris en deux phrases ce que cette note réunit. Aucune autre sortie. » Ordre des écritures : `poser(garder, …)`, `poser(entrant, …)` par entrant changé, `retirer(absorber)` (ou note de redirection, branche « rediriger »). Journal `("fusion", "`absorber` → `garder`", …)`, `finally` et `incomplet` comme `deplacer`.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_fusion_garde_tout_et_s_annule():
    A, B = "03 - Ressources/Tarte.md", "00 - Inbox/tarte pommes.md"
    ctx = contexte({"Maison/" + A: b"---\ntype: recette\n---\n# Tarte\nP\xc3\xa2te.", "Maison/" + B: b"---\ntags: [dessert]\n---\nPommes, sucre.",
                    "Maison/x.md": b"[[tarte pommes]]"}, age_minutes=60, llm=scenario("Deux notes sur la tarte\naux pommes."), rag=FauxRag(entrants={B: ["x.md"]}))
    n = menage.fusionner(ctx, A, B, "nuit", "doublon confirmé"); t = ctx.coffre.s3.text("Maison/" + A)
    assert t == ("---\ntype: recette\n---\n> [!summary] Fusion du 2026-11-22\n> Deux notes sur la tarte aux pommes.\n\n# Tarte\nPâte.\n\n"
                 "## Depuis « tarte pommes »\n\n```yaml\ntags: [dessert]\n```\n\nPommes, sucre.\n")
    assert "Maison/" + B not in ctx.coffre.s3.data and ctx.coffre.s3.text("Maison/x.md") == "[[Tarte]]"
    vieillir_tout(ctx, 11); ctx.coffre.annuler(n, "nicolas-doc")
    assert ctx.coffre.s3.text("Maison/" + B) == "---\ntags: [dessert]\n---\nPommes, sucre." and "Fusion" not in ctx.coffre.s3.text("Maison/" + A)
    assert ctx.coffre.s3.text("Maison/x.md") == "[[tarte pommes]]"

def test_fusion_sans_chapeau_si_le_modele_tombe():
    ctx = contexte({"Maison/a.md": b"A", "Maison/b.md": b"B"}, age_minutes=60, llm=modele_en_panne, rag=FauxRag(entrants={}))
    menage.fusionner(ctx, "a.md", "b.md", "nuit", ""); assert ctx.coffre.s3.text("Maison/a.md") == "A\n\n## Depuis « b »\n\nB\n"

def test_fusion_refus(): ...   # garder == absorber -> chemin ; absorber protégé -> protege ; garder en calme -> calme ; RAG en panne -> rag ; rien n'est écrit
```

- [ ] **Step 2: Lancer** — attendu : ÉCHEC. **Step 3: Implémenter.** **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "agent-doc: mechanical merge, both contents kept, summary line by the model"`

---

### Task 5: Exposer le ménage — action `archiver`, outils du modèle

**Files:**
- Modify: `agents/agent-doc/agent.py`
- Test: `scripts/test_agent_doc.py`

**Interfaces:**
- Consumes: `menage.deplacer`, `menage.fusionner`, `menage.archiver`.
- Produces:
  - Mode déterministe : `{"action": "archiver", "chemin", "pourquoi"}` → réponse du contrat ; réindexation demandée ensuite. (Absent dans la branche « rediriger » : `refus: "demande"`.)
  - Outils du mode libre, noms du spec : `deplacer_note(ancien, nouveau, pourquoi)` et `fusionner_notes(garder, absorber, pourquoi)` ; un `Refus` est rendu au modèle (« refusé : … ») ; succès compté dans `Écrit :` (`- <nouveau> (journal n° N, déplacée depuis <ancien>)`, `- <garder> (journal n° N, a absorbé <absorber>)`).
  - Ajout au prompt : « Quand tu consignes et que le RAG te montre deux notes sur le même sujet, fusionne-les avec `fusionner_notes` avant de compléter ; une note de `00 - Inbox` que tu viens de traiter se range avec `deplacer_note`. Ne range rien d'autre sans qu'on te le demande. »

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_action_archiver():
    ctx = contexte({"Maison/00 - Inbox/A.md": b"a"}, age_minutes=60, rag=FauxRag(entrants={}))
    r = json.loads(agent.traiter('{"action":"archiver","chemin":"00 - Inbox/A.md","pourquoi":"demandé par Nicolas"}', "jarvis", ctx))
    assert r["ok"] is True and r["journal"] == 1 and r["version"].startswith("_versions/00 - Inbox/A.md/") and ctx.rag.reindexations == 1
    assert json.loads(agent.traiter('{"action":"archiver","chemin":"06 - Modèles/M.md","pourquoi":"x"}', "jarvis", ctx))["refus"] in ("absent", "protege")

def test_outils_deplacer_et_fusionner(): ...
    # scenario(outil("deplacer_note", ancien=…, nouveau=…, pourquoi=…), "Rangée.") -> note déplacée, ligne « Écrit : » avec « déplacée depuis »
    # scenario(outil("fusionner_notes", garder="06 - Modèles/M.md", …)) -> le résultat d'outil commence par « refusé : », rien n'est écrit
```

- [ ] **Step 2: Lancer** — attendu : ÉCHEC. **Step 3: Implémenter.** **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "agent-doc: archive action, move and merge tools"`

---

### Task 6: La passe de nuit

**Files:**
- Modify: `agents/agent-doc/menage.py`, `agents/agent-doc/coffre.py`, `agents/agent-doc/journal.py`
- Test: `scripts/test_agent_doc.py`

**Interfaces:**
- Consumes: `Rag.doublons`, `orphelines`, `chercher`, `document`, `liens_casses` ; `Coffre.lister`, `lire`, `protege`, `existe`, `ecrire` ; `menage.deplacer`, `fusionner` ; `ctx.llm`.
- Produces:
  - `Coffre.etat_lire() -> dict` et `Coffre.etat_ecrire(d: dict) -> None` : l'objet `<préfixe>_versions/_etat/examines.json` (absent → `{}`), sans version ni journal.
  - `journal.poser_nuit(texte: str | None, fin: str, resume: list[str]) -> str` : met `derniere_nuit: <fin>` dans le frontmatter du journal et remplace (ou crée, juste sous l'encadré d'en-tête, avant `## Changements`) la section `## Dernière nuit — <AAAA-MM-JJ>` par les lignes de `resume` en liste. `journal.derniere_nuit(texte: str | None) -> datetime | None`. `Coffre.poser_nuit(fin, resume)` l'applique sous le verrou.
  - `menage.passe_de_nuit(ctx, fin: str, maxi: int, a_blanc: bool) -> dict` : bilan `{"changements": [<numéros de journal>], "prevus": [<phrases, à blanc>], "examines": int, "laisses": int, "reportes": int, "erreurs": [str], "liens_casses": int | None}`.
    - **Candidats, dans cet ordre**, jusqu'à `maxi` changements ou `nuit.max_examens` appels au modèle :
      1. **doublons** : `ctx.rag.doublons(nuit.seuil_doublon)` ;
      2. **Inbox** : `ctx.coffre.lister(config["inbox"])` ;
      3. **orphelines** : `ctx.rag.orphelines()`.
    - **Écartés sans appel au modèle** (comptés dans `reportes`, réessayés la nuit suivante) : une note protégée, générée, absente de S3, en période de calme, ou dont `sha256` dans S3 diffère de `ctx.rag.document(chemin)["sha256"]` (l'index n'a pas encore vu la dernière version).
    - **Déjà examinés** : clé `doublon|<a>|<sha a>|<b>|<sha b>` (a < b), `inbox|<chemin>|<sha>`, `orpheline|<chemin>|<sha>` présente dans `etat_lire()` → sauté sans rien compter. Une décision « ne rien faire » y est ajoutée ; un changement fait ou une réponse illisible du modèle, non.
    - **Décision du modèle** : un appel sans outil, réponse attendue = un objet JSON seul (texte autour toléré : on prend le premier objet JSON) ; illisible ou hors des valeurs permises → `erreurs`, candidat sauté.
      - doublon — on montre les deux notes entières (tronquées à 6000 caractères chacune) : `{"doublon": true | false, "garder": "a" | "b", "pourquoi": "…"}` → `fusionner(garder, absorber, "nuit", pourquoi)`.
      - Inbox — on montre la note et la liste des dossiers (`dossiers` hors Inbox et hors zones protégées) avec leurs sous-dossiers existants : `{"action": "deplacer" | "laisser", "dossier": "<dossier ou sous-dossier existant>", "nom": "<facultatif, sans .md>", "pourquoi": "…"}` → `deplacer(chemin, dossier + "/" + (nom ou nom actuel) + ".md", "nuit", pourquoi)` ; un dossier qui n'existe pas → `erreurs`.
      - orpheline — on montre la note et les 5 voisines de `ctx.rag.chercher(<nom de la note>, limite=6)` (elle-même retirée) : `{"liens": ["<chemin parmi les voisines>", … 3 au plus], "pourquoi": "…"}` ; liste vide = laisser ; sinon `ctx.coffre.ecrire` de la note + `\n\n## Voir aussi\n- [[<nom>]]\n…` (lignes ajoutées à la section si elle existe déjà), appelant `nuit`.
    - Un `Refus` ou une `ErreurRag` sur un candidat va dans `erreurs` (code et chemin, jamais de contenu) et la passe continue. `ErreurRag` sur la liste des doublons ou des orphelines : ce genre de candidats est sauté, les autres sont traités.
    - **À blanc** : aucune écriture de note ; chaque décision devient une phrase dans `prevus` ; `examines.json` n'est pas modifié.
    - Fin : `ctx.rag.liens_casses()` compté (`None` si le RAG ne répond pas) ; `ctx.coffre.poser_nuit(fin, resume)` avec, en lignes : « N changements (n° x à y) : a fusions, b déplacements, c liens ajoutés » (ou « À blanc : … » suivi des `prevus`), « examinés : N, laissés : N, reportés : N », « liens cassés dans le coffre : N », une ligne par erreur ; `ctx.rag.reindexer()` si au moins un changement.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_nuit_plafonnee_et_resumee():
    notes = {f"Maison/00 - Inbox/n{i}.md": f"note {i}".encode() for i in range(5)}
    ctx = contexte(notes, age_minutes=60, rag=FauxRag(entrants={}, a_jour=True), llm=toujours('{"action":"deplacer","dossier":"03 - Ressources","pourquoi":"ressource"}'))
    b = menage.passe_de_nuit(ctx, "2026-11-23T03:40:00-05:00", maxi=3, a_blanc=False)
    assert len(b["changements"]) == 3 and len(ctx.coffre.lister("00 - Inbox")[0]) == 2 and ctx.rag.reindexations == 1
    j = ctx.coffre.s3.text("Maison/Journal du documentaliste.md")
    assert "derniere_nuit: 2026-11-23T03:40:00-05:00" in j and "## Dernière nuit — 2026-11-23" in j and "3 changements (n° 1 à 3)" in j
    assert j.index("## Dernière nuit") < j.index("## Changements") and j.count("appelant : nuit") == 3

def test_nuit_a_blanc_n_ecrit_aucune_note():
    ctx = contexte({"Maison/00 - Inbox/a.md": b"a"}, age_minutes=60, rag=FauxRag(entrants={}, a_jour=True), llm=toujours('{"action":"deplacer","dossier":"03 - Ressources","pourquoi":"x"}'))
    b = menage.passe_de_nuit(ctx, FIN, maxi=10, a_blanc=True)
    assert b["changements"] == [] and len(b["prevus"]) == 1 and ctx.coffre.s3.puts == ["Maison/Journal du documentaliste.md"]
    assert "À blanc" in ctx.coffre.s3.text("Maison/Journal du documentaliste.md")

def test_nuit_ecarte_protege_calme_et_index_en_retard():
    ctx = contexte({"Maison/a.md": b"A", "Maison/b.md": b"A", "Maison/05 - Journal/j.md": b"J", "Maison/c.md": b"C", "Maison/d.md": b"D"}, age_minutes=60,
                   rag=FauxRag(doublons=[("a.md", "05 - Journal/j.md"), ("a.md", "c.md"), ("b.md", "d.md")], a_jour={"a.md", "b.md", "c.md"}), llm=modele_interdit)
    vieillir(ctx.coffre, "c.md", minutes=2)                       # c en calme ; d pas encore réindexée
    b = menage.passe_de_nuit(ctx, FIN, 10, False); assert b["reportes"] == 3 and b["changements"] == [] and b["examines"] == 0

def test_nuit_ne_reexamine_pas_un_refus_tant_que_rien_ne_change():
    ctx = contexte({"Maison/a.md": b"A", "Maison/b.md": b"B"}, age_minutes=60, rag=FauxRag(doublons=[("a.md", "b.md")], a_jour=True),
                   llm=compteur('{"doublon": false, "pourquoi": "sujets différents"}'))
    menage.passe_de_nuit(ctx, FIN, 10, False); menage.passe_de_nuit(ctx, FIN, 10, False); assert ctx.llm.appels == 1
    ctx.coffre.s3.data["Maison/b.md"] = (b"B2", VIEUX); menage.passe_de_nuit(ctx, FIN, 10, False); assert ctx.llm.appels == 2

def test_nuit_modele_illisible_rag_en_panne_orpheline(): ...
    # réponse « bof » -> erreurs, rien d'écrit, pas dans examines.json ; FauxRag(panne_doublons=True) -> l'Inbox est quand même traitée ;
    # orpheline + {"liens": ["03 - Ressources/V.md"]} -> la note finit par « ## Voir aussi\n- [[V]]\n » ; un chemin hors des voisines -> erreurs ;
    # max_examens = 2 et 5 candidats que le modèle laisse -> 2 appels au modèle
```

- [ ] **Step 2: Lancer** — attendu : ÉCHEC. **Step 3: Implémenter.** **Step 4: Relancer** — attendu : SUCCÈS.
- [ ] **Step 5: Commit** `git commit -am "agent-doc: capped night pass (duplicates, Inbox, orphans), summary on top of the journal"`

---

### Task 7: Déclencheur, image `:2`, documentation, première nuit à blanc

**Files:**
- Modify: `agents/agent-doc/server.py`, `agents/agent-doc/menage.py`, `docker-compose.mcp.yml`, `scripts/portainer-images.sh`, `README.md`, `CLAUDE.md`
- Test: `scripts/test_agent_doc.py`, `scripts/test_mcp_policy.py` (balise d'image)

**Interfaces:**
- Consumes: `Rag.etat`, `journal.derniere_nuit`, `menage.passe_de_nuit`.
- Produces:
  - `menage.a_lancer(etat: dict, derniere_nuit: datetime | None) -> str | None` (pure) : rend `etat["derniere_nocturne"]["fin"]` si `etat["en_cours"]` est nul, que `derniere_nocturne` existe avec `ok` vrai et `fin` non nul, et que `fin` est postérieur à `derniere_nuit` (ou que `derniere_nuit` est `None`) ; sinon `None`.
  - `menage.veilleur(ctx, mode: str, maxi: int, attendre=time.sleep)` : boucle sans fin — `attendre(600)`, `ctx.rag.etat()`, `a_lancer(...)`, puis `passe_de_nuit(ctx, fin, maxi, a_blanc=(mode == "blanc"))`. Toute exception est journalisée (type et message, sans contenu) et la boucle continue. Une nuit du RAG avec `ok` faux ne déclenche rien (index peut-être incomplet).
  - `server.py` : `AGENT_DOC_NUIT` ∈ `off` (défaut, pas de fil), `blanc`, `on` (autre valeur : arrêt au démarrage) ; le fil `veilleur` est démarré en `daemon` ; `AGENT_DOC_NUIT_MAX` (défaut 10, entier de 1 à 50 sinon arrêt).
  - Image `infra-agent-doc:2` dans `docker-compose.mcp.yml` et `scripts/portainer-images.sh` (les deux ensemble) ; `AGENT_DOC_NUIT: "blanc"` dans la pile.

- [ ] **Step 1: Écrire les tests qui échouent**

```python
def test_a_lancer():
    n = lambda fin, ok=True: {"en_cours": None, "derniere_nocturne": {"debut": "x", "fin": fin, "ok": ok}}
    hier = datetime.fromisoformat("2026-11-22T03:40:00-05:00")
    assert menage.a_lancer(n("2026-11-23T03:40:00-05:00"), hier) == "2026-11-23T03:40:00-05:00"
    assert menage.a_lancer(n("2026-11-23T03:40:00-05:00"), None) == "2026-11-23T03:40:00-05:00"
    assert menage.a_lancer(n("2026-11-22T03:40:00-05:00"), hier) is None                    # déjà faite
    assert menage.a_lancer(n("2026-11-23T03:40:00-05:00", ok=False), hier) is None
    assert menage.a_lancer({"en_cours": None, "derniere_nocturne": None}, hier) is None
    assert menage.a_lancer(n("2026-11-23T03:40:00-05:00") | {"en_cours": {"genre": "demandee", "debut": "x"}}, hier) is None

def test_veilleur_survit_a_une_panne_et_ne_lance_qu_une_fois():
    # FauxRag dont etat() lève une fois, puis rend la même nuit finie trois fois ; attendre = compteur qui arrête après 4 tours
    # -> passe_de_nuit appelée une seule fois (la 2e fois, derniere_nuit du journal vaut déjà `fin`)
```

- [ ] **Step 2: Lancer** — attendu : ÉCHEC. **Step 3: Implémenter.** **Step 4: Relancer** `make test`, `make check-env` — attendu : tout vert.
- [ ] **Step 5: Documenter** dans `README.md` et `CLAUDE.md` : ce que fait la passe, les trois valeurs d'`AGENT_DOC_NUIT`, comment lire le résumé en tête du journal, comment annuler (« Annule le changement n° N », ou `{"action": "annuler", "numero": N}`), la liste des zones protégées et où la changer (`agents/agent-doc/config.yaml`, nouvelle image), et le réglage de Remotely Save retenu par le spike.
- [ ] **Step 6: Commit et PR en brouillon** `git commit -am "agent-doc: night pass after the RAG's nightly reindex (dry run first), image :2"`, `gh pr create --draft`. Ne pas fusionner.
- [ ] **Step 7: Mise en service, par Nicolas** (écrit dans le README, pas fait par l'agent) :
  1. fusionner : la pile part avec `AGENT_DOC_NUIT=blanc` ;
  2. le lendemain matin, lire `## Dernière nuit` dans le journal : chaque ligne « prévu » doit être un changement que Nicolas aurait fait lui-même ; régler `nuit.seuil_doublon` et les zones protégées si besoin ;
  3. quand deux nuits à blanc de suite sont bonnes : passer `AGENT_DOC_NUIT` à `on` (une ligne dans `docker-compose.mcp.yml`, PR, fusion) ;
  4. le matin suivant : vérifier sur le Mac, après synchro, que les notes déplacées sont à leur nouvel endroit, qu'aucune n'est revenue dans l'Inbox, que les liens s'ouvrent ; annuler un changement pour essai.
