# Jarvis sans RAG propre — design (projet 3)

Date : 2026-11-21. Statut : brouillon à valider par Nicolas.
Dépend de : projet 1 (RAG maître, visibilité par source) et projet 2 (agent-doc).

## Intention

« Arrêter d'avoir de l'info partout » : Jarvis garde son propre index vectoriel (fichiers, courriels, mémoires, réunions). Ces contenus deviennent des sources du RAG maître ; Jarvis cesse d'indexer et de chercher lui-même, et cesse d'écrire directement dans le coffre.

## Ce qui existe (Jarvis, `origin/main`)

- `ingest/` (conteneur) : découpage, embeddings, description d'images, rendu PDF ; fichiers dans MinIO ; table `file_chunks` (+ `files`). Courriels ingérés par `batch/app/jobs/email_ingest.py`.
- `backend/app/rag.py` : recherche vectorielle pure, top 4, utilisée par le chat.
- `memories` et `meeting_summaries` : chacune avec une colonne `embedding` (recherche sémantique dans `memory.py`, `search_service.py`, `meeting_summaries.py`).
- `backend/app/obsidian_service.py` : outils du chat qui lisent/écrivent/suppriment dans le coffre via le plugin Obsidian Local REST API.

## Objectifs

- Fichiers, courriels, mémoires et résumés de réunions deviennent des sources du RAG maître, au niveau de visibilité **`prive`** (décision de Nicolas : choix A pour les trois, B pour la visibilité).
- Le chat de Jarvis cherche via le RAG maître (jeton donnant droit à `public-maison` + `prive`) : il gagne au passage le coffre et le code.
- Les écritures de Jarvis dans le coffre passent par agent-doc (règle « un seul écrivain côté serveur »).
- Jarvis garde ses données et son interface : téléversement, création de mémoires, réunions, stockage MinIO, base.

## Non-objectifs

- Déplacer les données de Jarvis dans le coffre.
- Changer l'interface de Jarvis au-delà des sources citées.

## Anti-objectifs

- Un chat de Jarvis cassé ou moins bon pendant ou après la bascule.
- Des contenus `prive` visibles par le Portail, agent-musique ou tout jeton sans ce niveau.
- Supprimer des données (fichiers, mémoires) : seuls l'index et le code de recherche disparaissent.

## Approches considérées

1. **Retenu : connecteurs du RAG maître en lecture seule sur les sources de Jarvis** (MinIO pour les fichiers, base Jarvis pour mémoires et réunions) ; réindexation depuis les originaux.
2. Jarvis pousse ses contenus vers une API d'ingestion du RAG maître : rejeté, Jarvis garderait une logique d'ingestion.
3. Faire entrer ces contenus dans le coffre via agent-doc : rejeté par Nicolas (coffre trop gros).

## Ce qui est décidé

- **Connecteurs (dépôt RAG)** :
  - `jarvis-fichiers` : lit MinIO (identité lecture seule), reprend le rendu PDF et la description d'images d'`ingest` (modèles locaux uniquement), courriels inclus ;
  - `jarvis-memoires`, `jarvis-reunions` : lisent les tables de Jarvis avec un rôle Postgres en lecture seule ; changements détectés par date de mise à jour / identifiant ;
  - suppression côté Jarvis → retirée de l'index à la passe suivante.
- **Pas de copie des anciens embeddings** : réindexation depuis les originaux avec le découpage et le modèle du RAG maître.
- **Bascule par étapes, chacune réversible** :
  1. le RAG maître indexe les sources Jarvis ; le banc reçoit un jeu de questions Jarvis (tirées de ses fichiers, mémoires, réunions) ;
  2. le chat de Jarvis appelle le RAG maître derrière un réglage (`JARVIS_RECHERCHE=maitre|local`), `local` par défaut ; comparaison sur le banc ;
  3. bascule du réglage à `maitre` après accord de Nicolas ;
  4. après une période d'observation, suppression d'`ingest`, de `rag.py`, des colonnes `embedding` et des recherches sémantiques locales (migration Alembic), et du réglage.
- **Écritures dans le coffre** : les outils d'écriture/suppression d'`obsidian_service.py` sont remplacés par des appels à agent-doc (clé d'une équipe limitée aux modèles locaux). La lecture passe par le RAG maître.

## Décisions prises par Claude après relecture (à vérifier par Nicolas)

- **Outils Obsidian du chat** : lecture d'une note ou d'un dossier → `lire_note` / liste d'agent-doc (lecture exacte, pas le RAG) ; `search_notes` → RAG maître ; `write_note` / `append_note` → mode déterministe d'agent-doc (« écris ce contenu à ce chemin ») ; `delete_note` → archivage par agent-doc (copie dans `_versions/` puis retrait, à ajouter à agent-doc). Un refus « note modifiée il y a moins de 10 min » est montré tel quel à l'utilisateur.
- **Fraîcheur** : Jarvis appelle « réindexer maintenant » (jeton distinct) après un téléversement, une mémoire ou une réunion, pour que le contenu soit cherchable au message suivant ; passe périodique en filet. Mémoires : changements détectés par hash du contenu (pas d'`updated_at`).
- **Hors chat** : la recherche globale (`search_service.py`) interroge le RAG maître ; le badge « Indexé / En attente » vient d'un état d'indexation par document exposé par le RAG maître ; l'étape 4 retire `ingest_trigger`, `ingest_consumer`, `routers/ingest_status.py`, les files RabbitMQ d'ingestion et `files.ingested_at`.
- Le connecteur fichiers lit aussi la table `files` (nom, dossier) ; la recherche de Jarvis va directement à l'API du RAG maître (le jeton d'agent-doc n'a pas `prive`).
- **Courriels (décision de Nicolas : B)** : connecteur `courriels` du RAG maître, IMAP en lecture seule (`BODY.PEEK`, ne marque rien comme lu, ne déplace rien), niveau `prive`. Propositions de Claude : tous les dossiers sauf Indésirables/Corbeille/Brouillons, 2 ans d'historique, pièces jointes texte/PDF indexées, identifiants IMAP dans OpenBao (secret distinct de celui de Jarvis si possible). Le job `email_ingest` de Jarvis est inchangé.
- **Vision (décision de Nicolas : A)** : description d'images et de PDF scannés par `gemma4:26b-a4b-it-qat` via LiteLLM (plus de LM Studio). Premier pas : vérifier qu'Ollama/LiteLLM transmettent les images à gemma4 ; le banc compare ses descriptions à celles de qwen3.6 sur un échantillon de fichiers Jarvis avant la bascule.
- **Ajouts aux autres projets** : projet 1 — état d'indexation par document ; projet 2 — outils de liste de dossier et d'archivage.

## Laissé au builder

- Format des appels du chat vers le RAG maître (API directe ou agent-doc) pour la recherche.
- Durée de la période d'observation avant l'étape 4.
- Rôle et secrets exacts (OpenBao) pour l'accès en lecture seule à MinIO et à la base Jarvis.
