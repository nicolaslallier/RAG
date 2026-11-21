# agent-doc, le documentaliste point d'entrée — design (projet 2)

Date : 2026-11-21. Statut : brouillon à valider par Nicolas.
Dépend de : `2026-11-21-rag-maitre-design.md` (projet 1, API du RAG maître).

## Intention

Les agents du serveur ne savent pas ce que contient le coffre, et chacun y écrit à sa façon : l'information s'éparpille. `agent-doc` devient **le** documentaliste : tout agent du serveur l'appelle pour interroger le RAG maître, faire consigner quelque chose dans le coffre ou faire documenter un dépôt. Il est le **rédacteur en chef** du coffre Maison : il décide où et sous quelle forme les choses sont écrites, et il garde le coffre rangé.

## Objectifs

- Un point d'entrée A2A unique pour les agents du serveur, avec trois casquettes :
  1. **répondre** sur le contenu (coffre + code) via le RAG maître, sources citées (lien + origine) ;
  2. **consigner** dans le coffre : reçoit de la matière brute, décide dossier, type, modèle de note, frontmatter, création ou complément d'une note existante, liens `[[…]]` ; renvoie le chemin écrit ;
  3. **documenter un dépôt** : crée une tâche doc-only au Portail (qui ouvre la PR) et renvoie son lien.
- **Seul écrivain du coffre côté serveur.** `agent-musique` garde ses lectures ; ses écritures passent par `agent-doc`.
- **Garde le coffre rangé, seul** (choix C de Nicolas) : fusion de doublons, déplacements, renommages, réécritures, liens manquants, tri de l'Inbox.
  - au moment d'écrire (notes voisines) ;
  - et chaque nuit, après la réindexation complète du RAG, avec un plafond de changements par nuit.

## Non-objectifs

- Les agents Claude Code du Mac (chef-cuisinier, veille-techno, conseiller-dev, orchestre, plugin obsidian-second-brain) continuent d'écrire directement dans le coffre local (décision : « serveur seulement pour l'instant »). agent-doc voit leurs changements via S3.
- Écrire lui-même dans les dépôts de code : il passe par le Portail.
- Ménage déclenché sur demande (« range le dossier Projets ») : pas en v1.
- Retrait du RAG de Jarvis (projet 3).

## Anti-objectifs

- Perdre une information : aucune suppression définitive, aucun changement sans trace ni moyen de revenir en arrière.
- Écraser ce que Nicolas est en train d'écrire (conflits Remotely Save).
- Casser des liens `[[…]]` en déplaçant ou fusionnant.
- Écrire une note sans vérifier dans le RAG qu'elle n'existe pas déjà (nouveau doublon).
- Envoyer le contenu du coffre à un modèle infonuagique.

## Contraintes

- Le coffre est dans S3 (SeaweedFS, bucket `obsidian`, Remotely Save) : seule vérité.
- Modèle : `gemma4:26b-a4b-it-qat` via LiteLLM, clé propre (équipe `agents`, modèles locaux seulement). Rien ne sort du homelab.
- Suit le modèle d'`agent-musique` (Infra `agents/agent-musique/`, pile `mcp`) : a2a-sdk + Starlette, `X-Agent-Token` envoyé par LiteLLM (`static_headers`), route `/health` ouverte, `agents:` dans `litellm/config.yaml` et `litellm/mcp_policy.yaml`, `location` exacte dans `nginx/conf.d/litellm.conf`, secrets dans OpenBao (`infra env`), identité S3 provisionnée.
- Hôte Windows, GTX 1650 4 Go : gemma est lent ; prévoir des réponses longues (dizaines de secondes).

## Approches considérées

1. **Retenu : agent A2A dans Infra**, sur le modèle d'`agent-musique` — modèle déjà en prod, LiteLLM route et filtre par équipe.
2. Agent du Portail : rejeté, les agents du Portail travaillent dans des espaces de dépôt, pas en service permanent.
3. Serveur MCP au lieu d'A2A : rejeté, les agents du serveur passent déjà par A2A via LiteLLM.

Pour les documents de dépôt (choix A de Nicolas) : un seul agent qui délègue au documentaliste du Portail, plutôt que deux agents distincts (B) ou un agent qui écrit directement dans git hors Portail (C, contourne les protections).

## Ce qui est décidé

### Emplacement et accès

- Code : Infra `agents/agent-doc/`, pile `mcp`, image `infra-agent-doc`.
- Route : `POST /v1/a2a/agent-doc/message/send` ; équipes autorisées : seulement celles limitées aux modèles locaux (voir plus bas).
- Jeton `AGENT_DOC_TOKEN` (`X-Agent-Token`), clé LiteLLM `AGENT_DOC_LITELLM_KEY`, secret S3 `AGENT_DOC_S3_SECRET_KEY`, jeton de l'API du RAG maître, jeton Portail pour créer une tâche — tous dans OpenBao `infra env`.

### Outils de l'agent (boucle d'outils gemma, température 0)

- RAG : `rag_chercher(question, filtres)`, `rag_lister(filtres)`, `rag_expliquer(sujet)` → API du RAG maître.
- Coffre : `lire_note`, `ecrire_note`, `deplacer_note`, `fusionner_notes`.
- Dépôt : `demander_doc_depot(depot, demande)` → tâche doc-only au Portail.

### Règles appliquées par le code (jamais laissées au modèle)

- **Versions** : avant toute modification, déplacement ou fusion, copie de la version précédente sous `_versions/<chemin>/<horodatage>.md` dans S3 (préfixe exclu de Remotely Save).
- **Redirections** : un déplacement ou une fusion laisse à l'ancien chemin une note de redirection (`type: redirection`, lien vers la nouvelle), pour que les `[[liens]]` continuent de fonctionner ; les liens entrants connus sont mis à jour.
- **Jamais de suppression définitive.**
- **Journal** : chaque changement ajoute une entrée numérotée dans `Journal du documentaliste.md` (quoi, pourquoi, appelant, chemin de la version précédente). « Annule le changement n° N » restaure depuis `_versions/`.
- **Calme** : refus de toucher une note modifiée depuis moins de 10 min (`AGENT_DOC_QUIET_MINUTES`).
- **Anti-doublon** : avant de créer une note, recherche dans le RAG ; si une note proche existe, la compléter.
- **Réindexation** : après chaque écriture, appel « réindexer maintenant » du RAG maître pour les chemins touchés.
- **Conventions** : dossiers PARA du coffre (`00 - Inbox` … `06 - Modèles`), modèles de `06 - Modèles`, frontmatter `type`/`tags`/`source`/`date`, `source` = l'agent appelant.

### Passe de nuit

- Déclenchée après la réindexation complète du RAG maître.
- Candidats : doublons repérés par le RAG, notes orphelines (aucun lien), Inbox, liens manquants.
- Plafond `AGENT_DOC_NUIT_MAX` changements par nuit (départ : 10), tous au journal ; résumé de la nuit en tête du journal.

### agent-musique

Ses écritures (`enregistrer_disque`, régénération de `Discographie.md`) passent par `agent-doc` ; son identité S3 passe en lecture seule.

## Décisions prises par Claude après relecture (à vérifier par Nicolas)

- **Fusion** = mécanique : les deux contenus sont gardés intégralement dans la note cible (sections « Depuis [[ancienne]] »), gemma n'écrit qu'un court chapeau de synthèse au-dessus. Rien ne se perd. **Doublon** = proposé par le RAG (similarité au-dessus d'un seuil réglé au banc), confirmé par gemma.
- **Déplacer / fusionner sans note de redirection** : le code réécrit tous les liens entrants (connus du graphe du RAG) puis retire l'ancien fichier de S3 après l'avoir copié dans `_versions/` (archivage, pas suppression). Pas de doublon de nom, l'Inbox se vide. Un spike vérifie d'abord comment Remotely Save propage la disparition d'un objet S3 vers le Mac.
- **Zones protégées** (jamais touchées par le ménage, seulement par une écriture explicite) : `06 - Modèles`, `05 - Journal` (notes quotidiennes), fichiers générés (`Discographie.md`, `_CLAUDE.md`, `index.md`, `log.md`, tableaux kanban), `_versions/`. Liste dans la config.
- **Écriture structurée sans modèle** : en plus du message libre, agent-doc accepte « écris ce contenu à ce chemin » (mode déterministe, seules les règles du code s'appliquent). agent-musique l'utilise ; `Discographie.md`, généré, est exempté de version et de journal.
- **Contrat avec le projet 1** : agent-doc se planifie contre l'API du RAG maître, et le projet 1 ajoute ce dont agent-doc a besoin : doublons, notes orphelines, liens entrants d'une note, exclusion de `_versions/`, signal de fin de réindexation nocturne. agent-doc se construit après le projet 1.
- **Documentation de dépôt** : agent-doc crée la tâche doc-only directement « Prête » dans le projet Portail du dépôt (correspondance dépôt → projet en config ; s'il n'y en a pas, il répond qu'il ne peut pas) et renvoie tout de suite le lien de la tâche ; la PR reste à fusionner par Nicolas. Il faut une authentification de service au Portail (petit changement dans le dépôt AI).
- **Appelants (décision de Nicolas)** : seules des clés d'équipes limitées aux modèles locaux peuvent appeler agent-doc (aujourd'hui `agents`) ; `admin` et `apps` sont retirés. Le Portail, Jarvis et agent-musique l'appellent avec une clé dédiée d'une équipe locale seulement, pour que le contenu du coffre ne rebondisse jamais vers un modèle infonuagique.
- **Identité de l'appelant** : déduite de la clé LiteLLM (équipe / alias transmis par LiteLLM), pas déclarée dans le message.

## Laissé au builder

- Format exact des messages A2A (texte libre ou parties structurées) tant que la réponse contient la réponse, les sources et les chemins écrits.
- Nombre maximal de tours d'outils, prompt système.
- Mécanisme exact du déclenchement nocturne (appel du RAG, cron dans la pile).
- Comment le préfixe `_versions/` est exclu de Remotely Save (réglage du plugin ou autre préfixe/bucket).
