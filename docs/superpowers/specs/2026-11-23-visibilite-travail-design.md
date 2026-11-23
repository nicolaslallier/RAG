# Interface pleine largeur et niveau de visibilité « travail » — design court

Date : 2026-11-23 · Taille : petit changement, une PR.

## Intention

1. L'interface `/ui/` doit occuper toute la largeur de la fenêtre : le tableau Sources (10 colonnes)
   est à l'étroit dans la colonne centrée actuelle (`main { max-width: 72rem }`).
2. Nicolas veut étiqueter certaines sources comme **travail**, à côté de `public-maison` et `prive`.

## Décidé par Nicolas

- Seul `RAG_JETON_PRIVE` voit les sources `travail` ; `RAG_JETON` reste limité à `public-maison`.

## Approches

Une seule raisonnable : ajouter la valeur `travail` partout où les niveaux sont fixés (contrainte
SQL, `Literal` et `TOUT` de `rag/config.py`, menu de `source_git.html`). Écartées : une étiquette
séparée de la visibilité (deux notions pour un même besoin, et la visibilité filtre déjà tout) ; un
jeton propre au travail (non demandé : seul le jeton privé doit voir `travail`).

## Design

1. **Pleine largeur** : retirer `max-width` de `main` dans `rag/ui/statique/rag.css`. La carte de
   connexion (`.connexion`, 26rem) reste étroite.
2. **Base** : migration `rag/migrations/007_visibilite_travail.sql` qui remplace le `CHECK` de
   `sources.visibilite` par `('public-maison', 'prive', 'travail')`. Aucune source existante ne change.
3. **Config** : `travail` ajouté au `Literal` de `visibilite` et à `TOUT` dans `rag/config.py`. Tout
   le filtrage passe déjà par `niveaux` (recherche, lister, graphe, MCP, gestion), donc le jeton privé
   voit `travail` partout sans autre changement. Le droit d'écriture de l'UI (`niveaux == TOUT`) reste
   juste.
4. **Interface** : `travail` dans le menu de visibilité de `source_git.html`.
5. **Tests** : une source `travail` est invisible avec `RAG_JETON` et visible avec le jeton privé
   (`/chercher`, MCP, `/ui/`) ; un import YAML avec `visibilite: travail` est accepté ; la migration
   accepte `travail` et refuse une valeur inconnue.

## Non-buts

- Un jeton qui ne voit que `travail` (possible plus tard sans casser ce design).
- Un filtre par visibilité dans l'écran Sources ou la recherche.

## Choix de Claude

- La valeur s'écrit `travail` (minuscules, comme les autres).
- Le basculement des sources vers `travail` se fait à la main par Nicolas dans l'interface.

## Laissé au constructeur

- Le nom exact de la contrainte dans la migration (vérifier celui créé par `001_socle.sql`).
- Le README : mentionner le troisième niveau dans « Gestion de l'index ».
