# agent-doc (projet 2) — index des plans

Spec : `docs/superpowers/specs/2026-11-21-agent-doc-design.md`. Le projet touche trois dépôts : **Infra** (l'agent lui-même, LiteLLM, nginx, secrets, S3, agent-musique), **RAG** (ce qui manque à l'API du RAG maître) et **AI** (authentification de service du Portail). Cinq plans, chacun livrable seul.

## État de départ vérifié (2026-11-22, `origin/main` des trois dépôts)

| Demandé au projet 1 | État dans `RAG` `origin/main` |
|---|---|
| Doublons, notes orphelines, liens entrants | **Livrés** (plan 2) : `GET /doublons?seuil=&limite=`, `GET /orphelines`, `GET /liens-entrants?cle=` (`rag/entretien.py`, `rag/api.py`) |
| Exclusion de `_versions/` | **Livrée** : `exclure: [".obsidian/**", ".trash/**", "_versions/**"]` sur la source `coffre-maison` (`config/rag.yaml`) |
| « Réindexer maintenant », signal de fin de nuit | **Planifiés, pas livrés** : `POST /reindexer` et `GET /etat` (`derniere_nocturne.fin`) sont la tâche 8 du plan 3 du projet 1 |
| `rag_lister`, `rag_repondre` | **Planifiés, pas livrés** : `POST /lister`, `/repondre`, `/question` (plan 3 du projet 1, tâches 3 à 6) |
| État d'indexation par document (projet 3) | **Absent, non planifié** → plan 1 ci-dessous |
| Champ `origine` distinct de `source` (projet 4) | **Absent** : `documents.origine` est rempli depuis le `source` du frontmatter → plan 1 ci-dessous |
| RAG déployé | **Pas encore.** Il sera déployé **dans Colima sur le Mac**, et non plus sur `192.168.2.10` (amendement du plan 4 du projet 1 en cours, branche `claude/rag-plan-4-colima`). L'URL du RAG vue d'agent-doc et le chemin réseau entre les deux sont **à fixer** par cet amendement. |

Autres constats qui fixent le design :
- **LiteLLM 1.108.14** (version épinglée dans l'Infra) transmet à l'agent appelé les en-têtes `X-LiteLLM-Team-Id` et `X-LiteLLM-User-Id` de la clé appelante (`litellm/proxy/agent_endpoints/a2a_endpoints.py`, `_caller_identity_headers`), et retire tout `x-litellm-*` fourni par l'appelant. L'alias de la clé n'est **pas** transmis : l'identité de l'appelant passe donc par le `user_id` de sa clé.
- `scripts/provision-s3.sh` n'accepte que `Read`, `Write`, `List`, `Tagging` et ne sait pas limiter un préfixe. La suppression d'un objet relève de `Write` (à confirmer par le spike).
- Le Portail (`AI`) crée une tâche par `POST /api/projets/{id}/taches` (`DonneesTache.doc = true` pour une tâche de documentation seule), toujours dans la colonne `a_faire` ; l'ordonnanceur ne prend que la colonne `prete`. `/api/` n'est protégé que par l'en-tête `X-Auth-Request-Email` posé par nginx : il n'existe aucune authentification de service.
- Le Portail appelle agent-musique avec la clé `ai` (équipe `apps`, qui a des modèles infonuagiques). Décision de Nicolas : il reçoit une clé locale `portail-agents` (plan 2, tâche 3) et `apps` est retirée d'agent-musique (plan 5). agent-musique appelle agent-doc avec sa propre clé de l'équipe `agents`.

## Ordre de construction

| # | Plan | Dépôt | Livre | Tâches |
|---|---|---|---|---|
| 1 | `2026-11-22-agent-doc-1-rag-ajouts.md` | RAG | Champ `origine`, `GET /document` (état d'indexation par document), `GET /liens-casses`, journal du documentaliste exclu de l'index, test de contrat de tout ce qu'agent-doc consomme. | 5 |
| 2 | `2026-11-22-agent-doc-2-portail-service.md` | AI | `POST /service/taches-doc` avec jeton de service : crée une tâche doc-only directement « Prête » dans le projet du dépôt. Clé LiteLLM locale pour les appels du Portail aux agents. | 4 |
| 3 | `2026-11-22-agent-doc-3-infra-socle.md` | Infra | `agents/agent-doc/` : répondre, consigner (règles du code : versions, journal, calme, anti-doublon, réindexation), mode déterministe, annulation, documenter un dépôt ; LiteLLM, nginx, OpenBao, S3. | 10 |
| 4 | `2026-11-22-agent-doc-4-infra-menage.md` | Infra | **Spike Remotely Save (bloquant)**, puis déplacer / fusionner / archiver avec réécriture des liens, et la passe de nuit plafonnée. | 7 |
| 5 | `2026-11-22-agent-doc-5-infra-musique.md` | Infra | agent-musique écrit par agent-doc ; son identité S3 passe en lecture seule. | 4 |

Dépendances :
- **Plan 1** s'exécute après le plan 3 du projet 1 (il s'appuie sur `/reindexer`, `/etat`, `/lister`). Il doit être déployé avant le déploiement d'agent-doc.
- **Plan 2** est indépendant ; il peut partir tout de suite.
- **Déploiement du RAG dans Colima** : tant que l'amendement du plan 4 du projet 1 n'a pas fixé comment un conteneur d'`infra-net` (sur `192.168.2.10`) joint le RAG sur le Mac, la valeur d'`AGENT_DOC_RAG_URL` reste à fixer ; la PR du plan 3 ne se fusionne pas avant.
- **Plan 3** se développe sans attendre (tests contre un faux S3, un faux RAG, un faux LiteLLM), mais sa PR ne se fusionne qu'une fois le RAG maître déployé avec le plan 1 (sinon agent-doc démarre et refuse toute création : RAG injoignable). Sans le plan 2 déployé, seul l'outil `demander_doc_depot` répond « non configuré ».
- **Plan 4** attend le plan 3 déployé. Sa tâche 1 (spike) bloque les six autres.
- **Plan 5** attend le plan 3 déployé et vérifié ; il est indépendant du plan 4.

Points d'arrêt où Nicolas décide : plan 3 tâche 10 (étapes manuelles et fusion), plan 4 tâche 1 (étape 0 du spike, accord à chaque étape, puis lecture du compte rendu : retirer ou rediriger), plan 4 tâche 7 (première nuit, à blanc d'abord), plan 5 tâche 4 (rotation de la clé, identité S3 en lecture seule).

## Décisions prises en écrivant les plans (laissées au builder par le spec)

- **Format des messages A2A** : texte. Un message dont le texte est un objet JSON portant `"action"` est le **mode déterministe** (aucun modèle) ; tout autre texte part dans la boucle d'outils gemma. La réponse du mode déterministe est un objet JSON ; celle du mode libre est du texte terminé par les lignes `Sources :` et `Écrit :` ajoutées par le code.
- **Identité de l'appelant** : partie locale de `X-LiteLLM-User-Id` (ex. `agent-musique@agents.invalid` → `agent-musique`). Chaque clé qui appelle agent-doc porte un utilisateur de service dans `litellm/teams.yaml`. En-tête absent ou mal formé → `inconnu`.
- **Déclenchement nocturne** : agent-doc interroge `GET /etat` du RAG toutes les 10 minutes et lance la passe quand `derniere_nocturne.fin` dépasse le `derniere_nuit` inscrit dans le frontmatter du journal, que `ok` est vrai et qu'aucune passe n'est en cours. Pas de cron, pas de webhook.
- **`_versions/`** : sous le préfixe du coffre (`<préfixe>_versions/<chemin>/<AAAAMMJJTHHMMSSZ>.md`), comme le dit le spec. La façon de le tenir hors de Remotely Save sort du spike (plan 4, tâche 1).
- **Journal** : `Journal du documentaliste.md` à la racine du coffre, un seul fichier, entrées ajoutées à la fin, chacune avec une ligne `données` en JSON que « annule » relit. Exclu de l'index du RAG.
- **Config** : `agents/agent-doc/config.yaml` dans l'image (dossiers PARA, zones protégées, fichiers générés, seuils) ; les secrets et les deux réglages nommés par le spec (`AGENT_DOC_QUIET_MINUTES`, `AGENT_DOC_NUIT_MAX`) en variables d'environnement.
- **Tours d'outils** : 8 au plus (gemma est lent ; agent-musique en a 6 pour 3 outils).
- **Répondre** : par `rag_chercher` / `rag_lister` / `rag_expliquer` comme le dit le spec ; `/repondre` et `/question` du RAG ne sont pas utilisés (un second appel à gemma dans la même requête).

## Décisions de Nicolas (2026-11-22, « OK aux 5 recommandations »)

1. **Dépôt → projet** : le Portail retrouve le projet par l'URL du dépôt ; 409 s'il y en a deux. Validé (écart 1 ci-dessous).
2. **Spike Remotely Save** : fait par un agent, étape par étape, avec l'accord de Nicolas à chaque étape. Étape 0 : Nicolas exclut `_versions/` dans le plugin Remotely Save, avant toute manipulation.
3. **Le Portail reçoit une clé locale** (équipe `agents`) pour appeler agent-musique et agent-doc ; l'équipe `apps` est ensuite retirée d'agent-musique. Plus aucune clé ayant accès à un modèle infonuagique ne reçoit de contenu du coffre par un agent.
4. **Zones protégées** : `Accueil.md`, `À propos de moi.md` et `04 - Archives/` s'ajoutent à la liste.
5. **RAG en panne** : en mode libre, refus de créer une note (pas de création « non vérifiée ») ; le mode déterministe écrit quand même.

## Écarts avec le spec (validés par Nicolas le 2026-11-22)

1. **Correspondance dépôt → projet** : faite par le Portail (recherche sur `projet.depot_url`), pas par une table dans la config d'agent-doc. Une table d'identifiants de projets se périme sans prévenir ; le Portail connaît déjà ses dépôts. Sans projet pour le dépôt, le Portail répond 404 et agent-doc dit qu'il ne peut pas, comme prévu.
2. **Outils ajoutés pour le projet 3** (demandés par `2026-11-21-jarvis-sans-rag-design.md`) : `lister` et `archiver` dans le mode déterministe. `archiver` est la même opération que « retirer l'ancien fichier après copie dans `_versions/` ».
3. **`origine`** (demandé par le projet 4) : agent-doc écrit `source: <appelant>` et, quand l'appelant la donne, `origine: <URL ou livre>` ; le RAG lit `origine`, et retombe sur `source` pour les notes d'avant.

## Contrat entre les dépôts Infra, RAG et AI

Ce bloc est la référence. Chaque plan en recopie les lignes qu'il met en œuvre ; changer une valeur, c'est la changer ici et dans les plans qui la citent.

### Accès à agent-doc

| Élément | Valeur | Fourni par |
|---|---|---|
| Code, image, pile | Infra `agents/agent-doc/`, image `infra-agent-doc:<n>` (construite par `make app-images`), pile `mcp`, service `agent-doc`, port 8000, réseau `infra-net`, aucun port publié | Infra |
| Route (LAN) | `POST https://litellm.infra.famillelallier.net/v1/a2a/agent-doc/message/send` — `location =` exacte dans `nginx/conf.d/litellm.conf` | Infra |
| Route (conteneurs) | `POST http://litellm:4000/a2a/agent-doc/message/send`, `Authorization: Bearer <clé LiteLLM de l'appelant>`, `A2A-Version: 1.0`, JSON-RPC `message/send` | Infra |
| Équipes autorisées | `agents` seulement (`litellm/mcp_policy.yaml`, `agents: agent-doc: teams: [agents]`) ; jamais `admin` ni `apps` | Infra |
| Clés LiteLLM | `agent-doc` (la sienne, équipe `agents`, pour appeler gemma) ; `nicolas-doc` (équipe `agents`, utilisateur `nicolas-doc@agents.invalid`, pour les essais de Nicolas) ; `agent-musique` (existante, reçoit l'utilisateur `agent-musique@agents.invalid` au plan 5) ; `portail-agents` (équipe `agents`, utilisateur `portail@agents.invalid` : les appels du Portail à agent-musique et à agent-doc) | Infra |
| Jeton de l'agent | `X-Agent-Token: <AGENT_DOC_TOKEN>`, envoyé par LiteLLM (`static_headers`) ; toute route sauf `/health` répond 401 sans lui | Infra |
| Identité de l'appelant | en-tête `X-LiteLLM-User-Id` posé par LiteLLM ; agent-doc garde la partie avant `@` si elle correspond à `^[a-z0-9-]{1,40}$`, sinon `inconnu` | LiteLLM, Infra |
| Modèle | `gemma4:26b-a4b-it-qat`, température 0, par `http://litellm:4000` | Infra |

### Mode déterministe (texte du message = un objet JSON)

| Demande | Réponse (`ok: true`) |
|---|---|
| `{"action": "ecrire", "chemin": "<chemin .md relatif au coffre>", "contenu": "<texte complet>", "pourquoi": "<facultatif>"}` | `{"ok": true, "action": "ecrire", "chemin", "effet": "creee" \| "modifiee" \| "inchangee", "journal": <n> \| null, "version": "<clé _versions>" \| null, "reindexation": "demandee" \| "echec"}` |
| `{"action": "lire", "chemin": "…"}` | `{"ok": true, "action": "lire", "chemin", "contenu", "modifie_le": "<ISO 8601>"}` |
| `{"action": "lister", "dossier": "<dossier relatif, \"\" = racine>"}` | `{"ok": true, "action": "lister", "dossier", "notes": ["<chemin>", …], "tronque": bool}` |
| `{"action": "archiver", "chemin": "…", "pourquoi": "…"}` (plan 4) | `{"ok": true, "action": "archiver", "chemin", "journal": <n>, "version": "…"}` |
| `{"action": "annuler", "numero": <n>}` | `{"ok": true, "action": "annuler", "numero": <n>, "journal": <n'>, "chemins": […]}` |

Refus : `{"ok": false, "refus": "<code>", "message": "<phrase en français>"}`, codes : `calme` (note modifiée depuis moins de `AGENT_DOC_QUIET_MINUTES`), `chemin` (hors coffre, pas `.md`, `.obsidian/`, `_versions/`, le journal), `absent`, `protege` (zone protégée, pour archiver / déplacer / fusionner), `trop_gros` (plus de 1 Mio), `non_utf8`, `demande` (JSON ou champs invalides), `deja_annule`, `existe` (destination déjà prise, plan 4), `rag` (liens entrants inconnus car le RAG ne répond pas, plan 4), `interne`.

Un fichier listé dans `generes` de la config (au départ : `02 - Domaines/Loisirs/Musique/Discographie.md`) est écrit sans version, sans journal et sans règle de calme (`journal: null`, `version: null`).

### Ce qu'agent-doc écrit dans S3

| Élément | Valeur |
|---|---|
| Bucket, préfixe | `obsidian`, préfixe du coffre = `ORGANIZER_VAULT_PREFIX` de l'Infra (variable de conteneur `OBSIDIAN_VAULT_PREFIX`) |
| Identité S3 | `agent-doc`, `Read,Write,List` sur `obsidian`, secret `AGENT_DOC_S3_SECRET_KEY` ; `agent-musique` repasse à `Read,List` au plan 5 ; `rag` et `obsidian-mcp` restent `Read,List` |
| Versions | `<préfixe>_versions/<chemin de la note>/<AAAAMMJJTHHMMSSZ>.md` (UTC) |
| État de la passe de nuit | `<préfixe>_versions/_etat/examines.json` |
| Journal | `<préfixe>Journal du documentaliste.md`, frontmatter `type: journal-documentaliste`, `source: agent-doc`, `derniere_nuit: <ISO 8601>` |
| Frontmatter d'une note créée | `type`, `tags`, `source: <appelant>`, `date: <AAAA-MM-JJ, America/Montreal>`, `origine: <…>` si fournie |

### agent-doc → RAG maître

Base : variable `AGENT_DOC_RAG_URL`, **valeur à fixer** par l'amendement « Colima » du plan 4 du projet 1 (le RAG ne sera pas sur `infra-net` ; ne pas supposer `http://rag:8000`). Lecture : `Authorization: Bearer <AGENT_DOC_RAG_JETON>` (= `RAG_JETON`, niveau `public-maison`). Réindexation : `Bearer <AGENT_DOC_RAG_JETON_REINDEXER>` (= `RAG_JETON_REINDEXER`). Source du coffre : `coffre-maison` (variable `AGENT_DOC_RAG_SOURCE`) ; clé d'une note : `coffre-maison:<chemin>`.

| Appel | Corps / paramètres | Réponse utilisée | État |
|---|---|---|---|
| `POST /chercher` | `{"question", "filtres": {"sources"?, "dossier"?, "tags"?, "type"?}, "limite"}` | `resultats[]` : `cle`, `chemin`, `titre`, `fil`, `texte`, `lien`, `origine`, `score`, `cosinus` | livré |
| `POST /lister` | `{"question", "filtres"}` | `elements[]` : `cle`, `titre`, `lien`, `origine`, `certitude` ; `total` | projet 1, plan 3 |
| `POST /expliquer` | `{"nom", "limite"}` | dictionnaire rendu tel quel au modèle ; `etat: "introuvable"` | livré |
| `GET /doublons` | `?seuil=<0.5..1>&limite=<1..200>` | `doublons[]` : `a.cle`, `b.cle`, `similarite` | livré |
| `GET /orphelines` | — | `orphelines[]` : `cle`, `titre`, `lien` | livré |
| `GET /liens-entrants` | `?cle=coffre-maison:<chemin>` | `etat`, `entrants[]` : `cle`, `titre` | livré |
| `GET /document` | `?cle=coffre-maison:<chemin>` | `etat: "indexe" \| "exclu" \| "absent"`, `sha256`, `modifie_le`, `morceaux`, `demande_en_attente` | **plan 1** |
| `GET /liens-casses` | — | `liens[]` : `note.cle`, `cible` | **plan 1** |
| `POST /reindexer` | `{"source": "coffre-maison"}` | 202 `{"demande": ["coffre-maison"]}` | projet 1, plan 3 |
| `GET /etat` | — | `en_cours`, `derniere_nocturne: {"debut", "fin", "ok"} \| null` | projet 1, plan 3 |

### agent-doc → Portail

| Élément | Valeur | Fourni par |
|---|---|---|
| Route | `POST http://portail:8000/service/taches-doc`, `Authorization: Bearer <jeton>` ; fermée sur le vhost (`location ^~ /service { return 404; }`) | AI (plan 2), Infra (copie du vhost) |
| Corps | `{"depot": "<nom, sans propriétaire>", "demande": "<texte>", "appelant": "<nom>"}` | AI |
| Réponse | 201 (ou 200 si une tâche identique est déjà ouverte) `{"tache_id", "projet_id", "lien", "existante": bool}` ; 404 aucun projet pour ce dépôt ; 409 plusieurs projets ; 429 trop de tâches ouvertes ; 401 jeton ; 503 jeton non configuré | AI |
| Jeton | `PORTAIL_JETON_SERVICE` côté Portail (secret OpenBao `infra/portail`, facultatif) = `AGENT_DOC_PORTAIL_JETON` côté agent-doc (`infra/env`) | Nicolas (valeur) |

### Secrets et variables

OpenBao, secret `infra/env` (rendu dans `.env` par `make vault-env`), tous obligatoires sauf mention :

| Clé | Rôle | Valeur |
|---|---|---|
| `AGENT_DOC_TOKEN` | jeton `X-Agent-Token` (lu par `litellm` et par `agent-doc`) | `openssl rand -hex 32` |
| `AGENT_DOC_LITELLM_KEY` | clé LiteLLM d'alias `agent-doc` | imprimée une fois par `make litellm-provision` |
| `AGENT_DOC_S3_SECRET_KEY` | secret de l'identité S3 `agent-doc` | `openssl rand -hex 24` |
| `AGENT_DOC_RAG_JETON` | lecture du RAG maître | **égale à** `RAG_JETON` de `infra/rag` |
| `AGENT_DOC_RAG_JETON_REINDEXER` | « réindexer maintenant » | **égale à** `RAG_JETON_REINDEXER` de `infra/rag` |
| `AGENT_DOC_PORTAIL_JETON` (facultative) | jeton de service du Portail | **égale à** `PORTAIL_JETON_SERVICE` de `infra/portail` |

OpenBao, secret `infra/portail` : `PORTAIL_JETON_SERVICE` (facultative ; vide = route `/service/` en 503), `openssl rand -hex 32` ; `PORTAIL_LITELLM_CLE_AGENTS` (facultative ; clé LiteLLM d'alias `portail-agents`, imprimée une fois par `make litellm-provision` ; vide = le Portail appelle les agents avec `PORTAIL_LITELLM_CLE`, comme avant).

Politique d'agent-musique (`litellm/mcp_policy.yaml`) : `[admin, apps]` aujourd'hui → `[admin, apps, agents]` au plan 3 → `[admin, agents]` au plan 5, une fois le Portail déployé avec `PORTAIL_LITELLM_CLE_AGENTS`.

Variables du conteneur `agent-doc` (non secrètes, dans `docker-compose.mcp.yml`) : `LITELLM_URL=http://litellm:4000`, `S3_ENDPOINT=http://s3:8333`, `OBSIDIAN_BUCKET=obsidian`, `OBSIDIAN_VAULT_PREFIX=${ORGANIZER_VAULT_PREFIX:-}`, `AGENT_DOC_S3_ACCESS_KEY=agent-doc`, `AGENT_DOC_RAG_URL=` (**à fixer**, voir « agent-doc → RAG maître »), `AGENT_DOC_RAG_SOURCE=coffre-maison`, `AGENT_DOC_PORTAIL_URL=http://portail:8000`, `AGENT_DOC_QUIET_MINUTES=10`, `AGENT_DOC_NUIT_MAX=10`, `AGENT_DOC_NUIT=` (`off` au plan 3, `blanc` puis `on` au plan 4). Le choix « retirer » ou « rediriger » l'ancien chemin sort du spike et se fige dans le code : pas de variable.

Variable ajoutée au conteneur `agent-musique` (plan 5) : `AGENT_DOC_A2A_URL=http://litellm:4000/a2a/agent-doc/message/send` ; il s'authentifie avec sa clé existante `AGENT_MUSIQUE_LITELLM_KEY`.

## Risques ouverts

1. **Remotely Save et la disparition d'un objet S3** : si le plugin renvoie la copie locale au lieu de supprimer le fichier du Mac, « retirer » ne tient pas et l'Inbox ne se vide pas ; le plan 4 bascule alors sur les notes de redirection. C'est la raison du spike en tête du plan 4.
2. **`_versions/` synchronisé vers le Mac** : le coffre contient déjà un dossier `_Claude`, donc le réglage « synchroniser les fichiers et dossiers en `_` » de Remotely Save est peut-être actif. Nicolas exclut `_versions/` dans le plugin à l'étape 0 du spike ; le spike vérifie que l'exclusion tient.
2 bis. **RAG dans Colima sur le Mac** : agent-doc (sur `192.168.2.10`) dépend alors d'un service qui dort quand le Mac dort. Conséquences à prévoir une fois l'amendement connu : créations refusées en mode libre, pas de passe de nuit, tant que le RAG ne répond pas.
3. **`X-LiteLLM-User-Id` à travers une montée de version de LiteLLM** : lu dans le code de la 1.108.14, pas dans une documentation. Un test contre le paquet épinglé (plan 3, tâche 8) casse si l'en-tête disparaît ; la vérification réelle est dans les étapes de déploiement.
4. **Cache de réponses de LiteLLM** (`cache: true`) appliqué aux appels A2A : deux `lire` identiques pourraient rendre la même réponse après une écriture. Vérifié pour vrai au plan 3, tâche 10.
5. **Lenteur de gemma** : une consignation = 3 à 5 appels au modèle, soit une à plusieurs minutes. Le délai de lecture de l'appelant doit être long (agent-musique n'utilise que le mode déterministe, sans modèle).
6. **Graphe du RAG en retard** au moment d'un déplacement : un lien entrant créé depuis la dernière indexation n'est pas réécrit. Le mode « retirer » le laisse cassé ; `GET /liens-casses` le fait apparaître dans le résumé de nuit. Pas de réparation automatique en v1.
7. **Écriture non atomique** (`head` puis `put`) : Remotely Save qui écrit entre les deux perd un côté. La règle de calme réduit la fenêtre ; la version dans `_versions/` permet de revenir. `If-Match` si SeaweedFS le supporte un jour.
8. **Écritures directes du Mac** (agents Claude Code, plugin) : hors périmètre (« serveur seulement »). agent-doc les voit par S3 et les traite comme des écritures de Nicolas (règle de calme).
9. **Seuils** (`anti_doublon.cosinus`, `nuit.seuil_doublon`) posés à l'œil (0,85 et 0,92) : à régler au banc du RAG une fois agent-doc en service.
