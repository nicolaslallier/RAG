# RAG maître — design (projet 1)

Date : 2026-11-21. Statut : brouillon à valider par Nicolas.

## Intention

Nicolas a de l'information partout (coffre Obsidian Maison, 13 dépôts, un RAG dans Jarvis, un store `second-brain` dans LiteLLM) et ses agents ne savent pas ce que contient le coffre. Le **RAG maître** devient la source de vérité interrogeable : une seule base, réglable finement, mesurée. D'autres agents pourront garder de petits RAG locaux, mais celui-ci est la référence.

Il s'inscrit dans une suite de 4 projets (ordre validé) :
1. **RAG maître réglable + banc de mesure** ← ce document ;
2. le documentaliste devient le point d'entrée A2A unique (interroge le RAG, écrit seul dans le coffre) ;
3. retrait du RAG de Jarvis ;
4. fine-tuning de gemma sur le coffre, nourri et jugé par le banc de mesure.

## Objectifs

- Répondre bien à trois natures de questions (exemples réels de Nicolas, premiers cas du banc) :
  - **lister** : « quelles recettes j'ai pour le poulet, avec les sources » → exhaustif ;
  - **synthétiser** : « quelle est l'architecture de mon projet AI » → plusieurs notes + code ;
  - **lister sans type dédié** : « quelles sont mes dashboards Grafana » → recherche large + regroupement.
- « Avec les sources » = le lien vers la note/le fichier **et** l'origine du contenu (champ `source` du frontmatter, URL, livre).
- Tous les leviers de qualité sont dans une config versionnée, et un banc de mesure dit si un réglage aide.
- Sources v1 : **le coffre Maison et le code des dépôts**. Le schéma accepte d'autres sources (PDF…) plus tard.
- Un **graphe de connaissances** à la graphify à côté de la recherche textuelle.

## Non-objectifs (v1)

- L'accès des autres agents en A2A, l'écriture dans le coffre par le documentaliste (projet 2). La v1 expose une API HTTP ; le projet 2 la consomme.
- Retirer le RAG de Jarvis (projet 3), fine-tuning (projet 4).
- Réécriture de requête, HyDE, contextual retrieval par LLM, BM25 ParadeDB, comparaison d'autres modèles d'embedding : après la v1, une fois le banc fiable.
- Indexer d'autres branches que `origin/main`, les worktrees, les PDF.

## Anti-objectifs (échec même si « ça marche »)

- Créer une nouvelle « info partout » : une deuxième base de graphe, un store parallèle non référencé.
- Un RAG qui invente : répondre sans source citable plutôt que dire « je n'ai pas trouvé ».
- Des réglages codés en dur, impossibles à comparer.
- Citer du contenu périmé (note supprimée/renommée encore indexée).

## Contraintes

- Auto-hébergé ~~sur l'hôte Docker du homelab (`192.168.2.10`)~~ dans Colima sur le Mac Studio (VM `192.168.2.66`, voir `docs/superpowers/plans/2026-11-22-rag-maitre-4b-colima.md`), déployé par Portainer comme le reste, secrets dans OpenBao.
- Modèles via LiteLLM (Ollama derrière) : embeddings `qwen3-embedding:0.6b` (1024 dims), génération `gemma4:26b-a4b-it-qat`. Pas de gros GPU.
- **Le coffre vit dans S3** (SeaweedFS, bucket `obsidian`, synchronisé par Remotely Save depuis le Mac) : c'est la seule vérité (décision de Nicolas, 2026-11-21). Le dépôt git `obsidian-maison` est abandonné comme source (dernier commit le 8 novembre). `agent-musique` (Infra) lit/écrit déjà ce bucket : reprendre son accès S3 et sa règle « ne pas toucher une note modifiée depuis moins de 10 min ». Notes en français, frontmatter (`type`, `tags`, `source`, `date`…), liens `[[…]]`, commentaires `%%…%%`.
- Le code : les dépôts `nicolaslallier/*` (AI, Infra, Jarvis, EA, DarkAngel, Final, Glee, Heaven, Midas, Moria, orchestre, mcp-fichiers…), `origin/main` seulement.
- Nouveau dépôt `nicolaslallier/RAG` (aujourd'hui local, sans remote — sa création sur GitHub se fera avec l'accord de Nicolas).

## Approches considérées

**Moteur**
1. Réutiliser la table `embeddings` de LiteLLM (`litellm_rag`) et sa recherche : rejeté, recherche vectorielle pure sans hybride, rerank ni filtres.
2. Moteur RAG tout fait (R2R, LightRAG) : rejeté, deuxième pile + jette l'existant + « info partout ».
3. **Retenu (choix de Nicolas : « fresh new »)** : service neuf, sa propre base Postgres + pgvector, recherche hybride à nous. LiteLLM ne sert que de fournisseur de modèles.

**Graphe**
1. **Retenu (choix de Nicolas)** : graphify comme extracteur, graphe chargé dans le même Postgres (tables nœuds/liens reliées aux morceaux).
2. Notre propre extracteur (tree-sitter + networkx) : rejeté pour la v1, refait ce que graphify fait déjà. Reste le plan B si son format ne tient pas.
3. Base de graphe séparée (Neo4j/FalkorDB) : rejeté, deuxième base.

## Ce qui est décidé

### Architecture

Un service Python (dépôt `RAG`) avec trois rôles, une base Postgres+pgvector dédiée :

```
sources (git) ──► indexeur ──► Postgres (documents, morceaux, nœuds, liens, index_versions)
                     │                    ▲
                 graphify --update        │
                                     API recherche ──► aiguillage lister | répondre
                                          ▲
                                     banc de mesure
```

### Sources et synchronisation (décision de Nicolas : A + B + C)

- **Coffre** : lu dans S3 (bucket `obsidian`) avec une identité en lecture seule ; changements détectés par etag/date de modification, puis sha256 du contenu.
- **Code** : le service garde ses propres clones des dépôts, `origin/main`.
- **A** : réindexation incrémentale — coffre : listage S3 toutes les 5 min ; code : webhook GitHub ou interrogation toutes les 5 min. Côté Nicolas, rien ne change (Remotely Save).
- **B** : réindexation complète chaque nuit, filet de sécurité ; supprime ce qui a disparu ou été renommé.
- **C** (préparé pour le projet 2) : l'API offre « réindexer maintenant » pour qu'un écrivain (le documentaliste) déclenche l'indexation juste après son commit.

### Schéma (notion de source dès la v1)

- `sources` (id, type `obsidian|code|…`, dépôt, branche, dernier commit indexé).
- `documents` (source, chemin, titre, alias, tags, type, frontmatter JSONB, dates, sha256, symbole pour le code).
- `morceaux` (document, ordre, fil d'Ariane, texte, parent, embedding `halfvec(1024)`, tsvector, version d'index).
- `noeuds`, `liens` (type, confiance EXTRAIT/INFÉRÉ/AMBIGU, communauté) — issus de graphify et des `[[liens]]`, chaque nœud relié à ses documents/morceaux.
- `index_versions` : config d'indexation figée (stratégie de découpage, modèle, dimensions) ; on peut garder deux versions côte à côte pour A/B.

### Découpage (3 stratégies comparables, choisies par config)

Notes :
1. **par titres** (défaut) : fil d'Ariane « Note > H1 > H2 » préfixé avant embedding et tsvector ; cible 300-500 tokens, plafond ~700 ; fusion des sections < ~80 tokens ; repli récursif sur les sections trop longues ;
2. **récursif fixe** : 400 tokens, 10-15 % de recouvrement ;
3. **parent-enfant** : enfants ~200 tokens pour chercher, section entière renvoyée au LLM.
Nettoyage : frontmatter en métadonnées (pas en texte), `%%…%%` retirés, blocs de code gardés entiers.

Code : par symbole (fonction, classe, méthode), préfixé par « dépôt > chemin > symbole » ; les fichiers non analysables (YAML, Markdown du dépôt, Dockerfile) en récursif. Le découpage sémantique est écarté (morceaux trop petits).

### Recherche

- **Hybride** : top 30-50 vecteur + top 30-50 plein texte (`french` + `unaccent`, poids A titre/chemin/symbole, B tags, C corps), fusion RRF k=60, poids par branche réglables.
- **Flou** : `pg_trgm` sur titres, alias et noms de symboles, pour noms propres et fautes de frappe.
- **Instruction de requête Qwen3** : les requêtes sont encodées avec l'instruction de tâche, les documents sans. À vérifier de bout en bout via LiteLLM dès le premier jour.
- **Rerank** : `bge-reranker-v2-m3` sur 30 candidats, garder 5-8 ; désactivable.
- **Filtres** : source, dépôt, dossier, tags, type, dates ; `hnsw.iterative_scan` activé.
- **Graphe** : expansion à 1 saut depuis les 2-3 meilleurs résultats, score atténué, plafonnée ; désactivée par défaut, à valider au banc. Endpoints `chemin(A, B)` et `expliquer(X)`.

### Aiguillage lister | répondre

- **lister** : requête sur les métadonnées (ex. `type=recette` ∧ tag `poulet`), résultat exhaustif, chaque élément avec lien et origine. Si aucun type ne correspond (Grafana), recherche hybride large puis regroupement par document.
- **répondre** : recherche hybride → rerank → (graphe) → génération par gemma, 5-8 passages numérotés, citations [n] obligatoires.

### Réponse et refus

- Refus avant LLM si le meilleur score est sous un seuil calibré au banc (« Je n'ai pas trouvé cela dans vos notes »).
- Citations vérifiées : au moins une, identifiants existants.
- L'API renvoie la liste des sources : lien `obsidian://` ou `dépôt/chemin#Lsymbole`, et origine.
- Température 0-0,2.

### Banc de mesure

- Jeu manuel (étalon) : 30-50 vraies questions, dont les 3 ci-dessus, avec les notes/fichiers attendus ; ~15 questions sans réponse.
- Jeu synthétique : 200-300 questions générées par gemma depuis des morceaux tirés au hasard, filtrées.
- Vérité au niveau du document (pas du morceau), pour comparer les découpages.
- Métriques retrieval sans LLM : recall@5/10, MRR, nDCG@10, latence ; génération : fidélité et pertinence avec un juge différent du générateur, échantillon relu à la main.
- Une commande : (ré)indexe si besoin, rejoue le banc, compare à la dernière référence. Un paramètre à la fois.
- Le banc mesure aussi la non-régression du coffre quand on ajoute le code.

## Réponses de Nicolas à la relecture

- **Rien ne sort du homelab** : aucun modèle infonuagique, ni pour le juge du banc, ni pour le jeu synthétique, ni pour graphify. Tout passe par des modèles locaux via LiteLLM ; le juge est un second modèle local différent de gemma.
- **Tout est indexé** : tout le coffre, tous les dépôts `nicolaslallier/*` (y compris RAG et le coffre lui-même comme source obsidian). Seules exclusions techniques : fichiers générés, lockfiles, binaires, et fichiers qui ressemblent à des secrets (`.env`, clés), détectés à l'indexation.
- **Jeu étalon** : Claude propose 30-50 questions tirées du coffre et du code (avec documents attendus et ~15 questions sans réponse), Nicolas corrige.

## Décisions prises par Claude (à vérifier par Nicolas)

- **Aiguillage** : le service classe la question (règles d'abord, gemma en repli) et extrait les filtres à partir de la taxonomie réelle du coffre (types et tags présents en base, rafraîchis à l'indexation). L'appelant peut forcer le mode et les filtres. « Exhaustif » = résultat du filtre **plus** un complément issu de la recherche hybride, marqué « probable, non tagué » ; l'aiguillage a ses propres cas au banc.
- **Graphify** : extraction structurelle (AST, sans LLM) à chaque commit sur le code ; extraction sémantique (notes et liens doc → code) la nuit seulement, par un modèle local via LiteLLM. En v1, le graphe sert `chemin`, `expliquer` et les communautés ; l'expansion reste un interrupteur. Si graphify ne tourne pas proprement en conteneur sans Claude Code, on passe au plan B (tree-sitter + networkx), sans changer le schéma.
- **Seuil de refus** : sur le score du reranker ; sans rerank, repli sur la similarité cosinus du meilleur passage, avec un seuil calibré séparément. Jamais sur le score RRF.
- **Hôte** : ~~Windows, GTX 1650 (4 Go), 12 cœurs~~ VM Colima du Mac Studio, arm64, 8 cœurs, sans GPU (voir `docs/superpowers/plans/2026-11-22-rag-maitre-4b-colima.md`). Le reranker (568M) tourne dans le service, sur GPU s'il est libre, sinon CPU ; budget de latence cible : < 2 s pour lister et chercher, < 30 s pour répondre, avec streaming de la réponse.
- **Appelants v1** : le banc, Nicolas (CLI/HTTP) et un serveur MCP pour Claude Code. Réseau `infra-net` seulement, jeton dans OpenBao ; « réindexer » demande un jeton distinct.
- **Cibles v1** fixées après la première mesure de base : la v1 est finie quand le banc tourne, que les 3 questions de Nicolas passent, et que chaque levier a été mesuré au moins une fois contre la base.

## Ajouts demandés par les projets 2 et 3

- Pour agent-doc : endpoints doublons (paires de notes au-dessus d'un seuil), notes orphelines, liens entrants d'une note ; exclusion du préfixe `_versions/` de l'indexation ; signal (webhook ou statut) de fin de réindexation nocturne.
- **Visibilité par source** (projet 3, décision de Nicolas) : chaque source a un niveau (`public-maison` : coffre, code ; `prive` : fichiers, courriels, mémoires et réunions de Jarvis). Chaque jeton d'API donne droit à des niveaux ; le filtre est appliqué par le service dans toutes les requêtes (recherche, liste, graphe), jamais par le modèle. Le schéma porte ce niveau dès la v1, même si seules des sources `public-maison` existent alors.

## Laissé au builder

- Framework HTTP, ORM/migrations, structure du paquet ; forme exacte de l'API (tant que lister, répondre, chercher, chemin, expliquer, réindexer et les filtres y sont).
- Où tourne le reranker (dans le service via sentence-transformers, ou derrière LiteLLM) selon ce que l'hôte supporte.
- Webhook ou interrogation périodique pour A.
- Index HNSW ou scan exact au départ (petit volume), paramètres pgvector.
- Format des fichiers du banc.
