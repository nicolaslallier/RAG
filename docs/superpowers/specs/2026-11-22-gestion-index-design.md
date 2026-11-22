# Gestion de l'index : interface web et outils MCP — design

Date : 2026-11-22. Statut : brouillon à valider par Nicolas.
Dépend de : RAG maître (projet 1, plans 1 à 4b), déployé sur la VM Colima du MacBook (`192.168.2.40:8000`).

## Intention

Le RAG maître n'a qu'une API sans interface : Nicolas ne voit pas ce qui est indexé et ne peut rien retirer de l'index sans toucher à la config et redéployer. Il veut, depuis son Mac :

1. **voir ce qui est indexé** : par source, les fichiers indexés et ceux qui sont exclus (et pourquoi) ;
2. **exclure des fichiers** de l'index, et annuler une exclusion ;
3. que **ses agents** puissent voir la même chose par **MCP**.

Le RAG reste un index : il ne possède aucun fichier. Exclure retire de l'index, jamais de la source (coffre S3 ou dépôt GitHub).

## Objectifs

- Écran **Sources** : chaque source visible du jeton, avec type, visibilité, nombre de documents indexés, nombre d'exclus (automatiques / manuels), révision et date de la dernière passe, demande de réindexation en attente, passe en cours.
- Écran **Fichiers d'une source** : liste paginée, filtrable par texte sur le chemin et par état (indexé, exclu automatiquement avec sa raison, exclu manuellement) ; colonnes chemin, titre, type, tags, nombre de morceaux, modifié le.
- **Détail d'un fichier indexé** : métadonnées (frontmatter, origine, liens sortants) et ses morceaux (fil d'Ariane + texte), pour voir *ce que le RAG a réellement retenu*.
- Écran **Exclusions** : liste des exclusions manuelles, ajout (fichier ou motif) avec aperçu des fichiers touchés avant confirmation, annulation, export et import YAML.
- **Effet immédiat** : une exclusion retire tout de suite les documents concernés de l'index ; ils n'y reviennent pas aux passes suivantes.
- **Annulation** : le fichier revient à la passe suivante de sa source (demande de réindexation de la source posée automatiquement).
- **MCP** : deux nouveaux outils en lecture, mêmes jetons et mêmes niveaux que l'API.

## Non-objectifs

- Modifier, supprimer, renommer ou téléverser un fichier dans la source (coffre : rôle d'agent-doc ; code : git).
- Ajouter ou retirer une **source** depuis l'interface (reste dans `config/rag.yaml`).
- Interroger le RAG (chercher, répondre) depuis l'interface : le chat reste ailleurs (Jarvis, LiteLLM).
- Accès depuis un autre appareil que le Mac, HTTPS, vhost NGINX.
- Outil MCP d'exclusion : les agents voient, ils n'excluent pas.
- Tableaux de bord du banc, réglages de recherche.

## Anti-objectifs (échec même si « ça marche »)

- Un contenu `prive` visible avec un jeton `public-maison`, dans une page ou un outil MCP.
- Un fichier exclu qui réapparaît dans une réponse, une recherche, `lister`, `chemin` ou `expliquer`.
- Une exclusion perdue sans moyen de la retrouver (cf. Sauvegarde).
- Le jeton lisible par du JavaScript ou écrit dans un journal.
- Une exclusion trop large faite par erreur sans que Nicolas ait vu ce qu'elle touchait.

## Contraintes

- Même processus et même image que l'API (`rag servir`, port 8000, HTTP, lié à `192.168.2.40`). Pas de nouveau service dans la pile.
- Image `linux/arm64`, scannée par Trivy, `pip` retiré : le moins de dépendances possible.
- `RAG_JETON_PRIVE` ne s'utilise que depuis le MacBook (le port est en HTTP).
- La base se reconstruit par `rag indexer --complet` et n'est pas sauvegardée (README) ; les exclusions manuelles sont la seule exception, d'où l'export.
- Tests : `pytest` sur un vrai Postgres (`RAG_TEST_DATABASE_URL`), `ruff`.
- Garde-fou existant de l'indexeur (`CoffrePartielError`) : une passe refuse si plus de la moitié des documents d'une source « disparaissent ». Une exclusion large ne doit pas le déclencher.

## Approches considérées

Où ranger les exclusions manuelles :
- **A. Dans `config/rag.yaml` (git)**, le bouton ouvrant une PR. Traçable et sauvegardé, mais quelques minutes entre le clic et l'effet et un jeton GitHub en écriture dans l'interface.
- **B. Dans la base** — **retenu par Nicolas** : effet immédiat, simple pour l'interface et le MCP. Coût : casse « la base se reconstruit » ; compensé par l'export/import.

Où vit l'interface :
- **1. Servie par l'API du RAG** — **retenu par Nicolas** (usage depuis le Mac seulement) : rien de neuf à déployer.
- 2. Dans le Portail (dépôt AI, Vue) : accessible partout mais contrat inter-dépôts et jeton d'exclusion dans le Portail. Écarté.
- 3. Application à part : un service de plus, contraire à « arrêter d'avoir de l'info partout ». Écarté.

Comment fabriquer les pages :
- **A. Jinja2 + htmx, rendu par FastAPI** — **retenu par Nicolas** : tout en Python, testé par `pytest`/`TestClient`, une seule dépendance ajoutée.
- B. Vue 3 + Vite : outillage habituel de Nicolas, mais Node dans la CI et l'image pour trois écrans. Écarté.
- C. HTML + JS natif : sans dépendance mais peu testable. Écarté.

## Ce qui est décidé

### Données

Nouvelle migration `005_exclusions_manuelles.sql` :

```sql
CREATE TABLE exclusions_manuelles (
  id serial PRIMARY KEY,
  source text NOT NULL,        -- nom de la source (pas l'id : survit à un import, à une base recréée)
  motif text NOT NULL,         -- chemin exact ou motif fnmatch (« dossier/** », « *.csv »)
  raison text,                 -- facultative, saisie par Nicolas
  cree_le timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source, motif)
);
```

- Le motif suit `fnmatch` comme `exclure:` de la config (donc `*` traverse les `/`). Un chemin sans joker = un fichier.
- *(Choix de Claude)* La table `exclus` devient **le registre de tous les fichiers écartés** à chaque passe, quelle que soit la raison : `secret` (nom ou contenu), `binaire`, `genere`, `trop_gros`, `config` (motif `exclure:`) et `manuelle`. Aujourd'hui seules les exclusions décidées après lecture y sont, et une exclusion sur le nom efface la ligne : l'écran Fichiers n'aurait rien à montrer. Pour les raisons décidées sur le nom, la signature est celle du listage et `deja_exclus` n'est pas consulté (rien n'est lu de toute façon). Le nettoyage de fin de passe ne retire que les chemins **absents du listage**, plus ceux qui ne sont plus exclus.
- L'écran Fichiers = `documents` (indexés) ∪ `exclus` (avec raison) ; « exclu auto » = toute raison sauf `manuelle`.

### Indexeur

- `indexer_source` lit les motifs manuels de la source et les applique **avec** `source.exclure`, au même endroit (filtre des entrées sur le nom, avant lecture) ; raison `manuelle`. Ces fichiers ne sont ni lus, ni comptés comme « absents » : ils ne pèsent pas dans `CoffrePartielError`.
- Le **graphe** : à l'exclusion, les nœuds rattachés aux documents supprimés sont supprimés aussi (sinon `document_id` passe à NULL et le nœud reste visible dans `chemin`/`expliquer`). Le chargement du graphe de structure (graphify) et la passe sémantique ignorent les fichiers exclus manuellement.
- Ajouter une exclusion manuelle ne change pas l'empreinte d'indexation (comme `exclure:` aujourd'hui) : aucune réindexation des vecteurs.
- *(Choix de Claude)* **Exclusion pendant une passe** : l'exclusion prend effet tout de suite, sans attendre le verrou. L'indexeur relit les motifs manuels de la source **juste avant d'écrire chaque document** (une requête, dans la même transaction que l'écriture) et saute le fichier s'il vient d'être exclu. Aucun refus ni attente dans l'interface.
- *(Choix de Claude)* **Graphe après annulation** sur une source de code : l'annulation remet `graphe_revision` à NULL, ce qui relance graphify à la passe suivante (jusqu'à 600 s ; les annulations sont rares). Après une exclusion, la suppression des nœuds par `document_id` suffit ; graphify n'est pas relancé.

### Actions

- **Exclure** (transaction unique) : insère la ligne, supprime les `documents` de la source dont le chemin correspond (morceaux en cascade) et les nœuds du graphe liés. Aucun appel à un modèle.
- **Aperçu** avant confirmation : nombre et premiers chemins (50) des documents indexés touchés par le motif. Au-delà de 100 documents, l'interface exige une seconde confirmation.
- **Annuler** : supprime la ligne, pose `demande_le` sur la source et `NOTIFY rag_reindexer` (comme `/reindexer`) ; le fichier revient à la prochaine passe.
- Une source inconnue ou non visible du jeton : refus, sans révéler si elle existe (même règle que `/reindexer`).

### Accès et sécurité

- Connexion : page `/ui/connexion` où Nicolas colle un jeton ; le serveur le vérifie (`niveaux_de`) et pose un cookie `rag_session` **`HttpOnly`, `SameSite=Strict`, `Path=/ui`**, sans `Secure` (HTTP). Le cookie contient le jeton lui-même (pas de table de sessions : le jeton se tourne déjà par OpenBao + redéploiement, ce qui invalide le cookie). Déconnexion : efface le cookie.
- **Lecture** : `RAG_JETON` (voit `public-maison`) ou `RAG_JETON_PRIVE` (voit tout). Toutes les requêtes filtrent par `visibilite = ANY(niveaux)`, comme l'API.
- **Écriture** (exclure, annuler, importer, et un bouton « Réindexer » par source — *choix de Claude*) : **`RAG_JETON_PRIVE`** (décision de Nicolas, plutôt qu'un nouveau jeton). Avec `RAG_JETON`, les boutons n'apparaissent pas et les routes répondent 403 ; si `RAG_JETON_PRIVE` n'est pas défini, l'interface est en lecture seule. `/reindexer` (API) garde son `RAG_JETON_REINDEXER` inchangé ; le bouton de l'interface pose la même demande par la logique partagée.
- **Changement de contrat** : `RAG_JETON_PRIVE` passe de « lecture `public-maison` + `prive` » à « lecture de tout + gestion de l'index ». À reporter dans la ligne « Niveaux de visibilité » du tableau de contrat du README. Conséquence acceptée : tout porteur de ce jeton (le chat de Jarvis au projet 3) pourrait gérer l'index ; les routes d'écriture n'existent que sous `/ui/` (cookie), pas dans l'API Bearer ni le MCP.
- CSRF : `SameSite=Strict` plus vérification de l'en-tête `HX-Request` (posé par htmx) et de l'`Origin` sur les requêtes qui écrivent.
- Aucune page n'affiche ni ne journalise le jeton ; le contenu des fichiers `exclus` automatiquement (secrets) n'est jamais lu ni affiché, seulement le chemin et la raison.

### Interface

- Routes sous `/ui/` (HTML), servies par la même app FastAPI ; `/` redirige vers `/ui/`. Gabarits Jinja2 dans `rag/ui/gabarits/`, `htmx.min.js` (version épinglée) et une feuille CSS dans `rag/ui/statique/`, servis par l'app ; aucun CDN.
- Trois écrans (Sources, Fichiers d'une source + détail, Exclusions) en français, sobres, lisibles en clair et en sombre.
- Pagination côté serveur (50 par page) : le coffre et certains dépôts ont des milliers de fichiers.
- La logique (requêtes « sources », « fichiers », « exclure », « annuler », « exporter », « importer ») vit dans un module Python **partagé** (`rag/gestion.py`) appelé par les pages, le MCP et la CLI.

### MCP

Deux outils ajoutés à `rag/mcp_serveur.py`, en lecture, mêmes niveaux que le jeton :
- `rag_sources()` : la liste de l'écran Sources (JSON).
- `rag_fichiers(source, etat=None, contient=None, page=1)` : la liste de l'écran Fichiers ; `etat` ∈ `indexe | exclu_auto | exclu_manuel`.

Ils passent par la route MCP existante (LiteLLM `mcp_servers.rag`) ; rien ne change côté Infra.

### Sauvegarde des exclusions

- **Export** : bouton dans l'écran Exclusions et `rag exclusions exporter > exclusions.yaml` ; format `- {source, motif, raison}`.
- **Import** : bouton (fichier téléversé) et `rag exclusions importer exclusions.yaml` ; fusion (les motifs déjà présents sont ignorés), puis application immédiate comme une exclusion.
- Nicolas range l'export où il veut (le coffre, par exemple). Le RAG n'écrit pas dans S3 (identité en `Read,List`).

## Ce qui est laissé au constructeur

- Découpage exact des gabarits, du CSS et des fragments htmx.
- Noms des fonctions et organisation interne de `rag/gestion.py`.
- Ajout de `jinja2` et de `python-multipart` (formulaires) aux dépendances, en vérifiant Trivy.
- Les tests : au minimum visibilité (`public-maison` ne voit jamais `prive`, ni en page ni en MCP), exclusion immédiate, non-retour à la passe suivante, `CoffrePartielError` non déclenché par une exclusion large, annulation, 403 en écriture avec `RAG_JETON`, export/import aller-retour, jeton absent des journaux.
- Mise à jour du README : section Exploitation (adresse `http://192.168.2.40:8000/ui/`, export des exclusions) et ligne « Niveaux de visibilité » du tableau de contrat. Rien ne change côté Infra ni OpenBao.
