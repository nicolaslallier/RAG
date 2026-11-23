# Sources git gérées depuis l'interface — design

Date : 2026-11-23. Statut : brouillon à valider par Nicolas.
Dépend de : gestion de l'index (`2026-11-22-gestion-index-design.md`, interface `/ui/` livrée par #15).
Revient sur un non-objectif de ce design : « Ajouter ou retirer une source depuis l'interface (reste dans `config/rag.yaml`) ».

## Intention

Aujourd'hui, la liste des dépôts indexés est dans `config/rag.yaml`, copié dans l'image : ajouter un dépôt veut dire modifier le YAML, ouvrir une PR, merger et redéployer. Nicolas trouve ça trop lourd. Il veut :

1. **gérer les sources git depuis l'interface** : ajouter, modifier, supprimer ;
2. que ce soit **dynamique** : une source ajoutée est indexée tout de suite, une source modifiée est réindexée, une source supprimée disparaît entièrement de l'index ;
3. à terme, **distribuer le RAG** : chaque personne installe **sa propre instance** et part d'une liste de sources vide qu'elle remplit dans l'interface.

Ce design couvre le premier chantier (sources git dans l'interface). Rendre le RAG installable par d'autres (coffre S3 et OpenBao facultatifs, documentation d'installation, rien de « maison » en dur) est un **second chantier, à concevoir après celui-ci**. Ce design ne doit pas lui fermer de porte : rien de ce qui est construit ici ne suppose GitHub, le compte `nicolaslallier` ou la maison.

## Objectifs

- Écran **Sources** : bouton « Ajouter une source git », et pour chaque source git les actions « Modifier » et « Supprimer ».
- **Ajouter** : nom, URL https du dépôt, branche (défaut `main`), visibilité, motifs `exclure` (facultatifs), jeton d'accès (facultatif). La source est vérifiée (`git ls-remote`), enregistrée, puis indexée tout de suite.
- **Modifier** : URL, branche, visibilité, motifs `exclure`, jeton. La source est réindexée tout de suite (voir « Effets d'une modification »).
- **Supprimer** : après une confirmation qui montre le nombre de documents touchés, tout ce qui vient de la source est effacé : documents, morceaux, nœuds et liens du graphe, fichiers exclus, exclusions manuelles, clone sur disque.
- **Export / import YAML des sources**, comme pour les exclusions : la base n'est pas sauvegardée, l'export est la sauvegarde.
- Les sources git existantes de Nicolas (une cinquantaine, dans `config/rag.yaml`) passent en base **sans réindexation**.

## Non-objectifs

- Gérer les sources **Obsidian / S3** depuis l'interface : le coffre `coffre-maison` reste dans `config/rag.yaml`.
- Rendre le RAG installable par d'autres (second chantier).
- Comptes, plusieurs utilisateurs sur une instance, droits par source : chacun a son instance, l'accès reste celui d'aujourd'hui (jeton `public-maison` / jeton privé).
- Choisir un dépôt dans une liste tirée d'un compte GitHub ou GitLab : on saisit l'URL. *(Mon choix : ça lierait l'interface à une forge précise.)*
- Dépôts en SSH (`git@…`), chemins locaux, `file://` : https seulement.
- Réindexation déclenchée par un push (webhook) : les passes de 5 min et de nuit restent.
- Renommer une source.

## Anti-objectifs (échec même si « ça marche »)

- Un jeton de dépôt lisible : réaffiché dans l'interface, présent dans un export, une page HTML, un journal, une erreur git ou une URL.
- Une source supprimée qui revient : par une passe déjà en cours qui la réécrit, par le YAML, ou par des documents, nœuds ou passages qui restent cherchables.
- Une URL qui fait lire autre chose qu'un dépôt distant au service (`file://`, `ext::`, chemin local) : l'interface ne doit pas devenir un moyen de lire le disque du conteneur.
- Une suppression faite sans avoir vu ce qu'elle efface.
- La migration des sources existantes qui réindexe une cinquantaine de dépôts, ou qui en perd un.
- Un contenu `prive` visible avec un jeton `public-maison` (inchangé depuis la gestion de l'index).

## Contraintes

- Même processus, même image et même pile que l'API et le travailleur (`rag servir`, `rag travailleur`, port 8000 en HTTP, volume `rag-donnees` partagé). Pas de nouveau service.
- L'écriture dans l'interface demande le jeton privé et une origine locale (dépendance `ecrivain` existante de `rag/ui/__init__.py`).
- Image `linux/arm64` scannée par Trivy : le moins de dépendances possible. `cryptography` est déjà dans `uv.lock` (dépendance transitive) ; il devient une dépendance directe.
- La base se reconstruit par `rag indexer --complet` et n'est pas sauvegardée : les sources rejoignent les exclusions manuelles comme données à exporter.
- Garde-fou de l'indexeur (`CoffrePartielError`) : une passe refuse si plus de la moitié des documents d'une source disparaissent. Une modification voulue (URL, branche, motifs `exclure`) ne doit pas le déclencher.
- Tests : `pytest` sur un vrai Postgres (`RAG_TEST_DATABASE_URL`), `TestClient` pour l'interface, `ruff`.

## Approches considérées

Où vit la liste des sources :

- **A. En base, le YAML ne sert qu'une fois** — **retenu par Nicolas**. Les sources git sont dans la table `sources`, et la base fait foi. Le travailleur et `rag indexer` lisent la base au lieu de `config.sources`. Les sources git du YAML sont copiées une seule fois. Un export/import YAML remplace la sauvegarde par git. C'est simple pour l'interface, l'effet est immédiat avec le mécanisme `demande_le` + `NOTIFY rag_reindexer` existant, et c'est la bonne base pour une instance qui part de zéro.
- B. Un fichier YAML modifiable sur un volume, que l'interface réécrit. Écarté : écritures concurrentes, travailleur à recharger, et la base doit rester cohérente avec un fichier qu'on peut aussi modifier à la main.
- C. Un mélange : les sources du YAML restent fixes et celles de l'interface s'ajoutent par-dessus. Écarté : il faudrait expliquer pourquoi certaines sources ne s'effacent pas, et ça n'apporte rien à qui part de zéro.

## Ce qui est décidé

### Données

Nouvelle migration `006_sources_gerees.sql`, sur la table `sources` existante :

- `exclure text[] NOT NULL DEFAULT '{}'` : les motifs `exclure` de la source (même sens que dans le YAML).
- `jeton_chiffre bytea` : le jeton d'accès chiffré, `NULL` si aucun.
- `geree boolean NOT NULL DEFAULT false` : vrai pour une source git gérée par l'interface. Les sources du YAML (le coffre) restent `false`.
- `cree_le timestamptz NOT NULL DEFAULT now()`.

Une table `instance (cle text PRIMARY KEY, valeur text NOT NULL)` garde le fait que l'amorçage depuis le YAML a eu lieu (`sources_amorcees`).

La suppression d'une ligne de `sources` efface déjà en cascade `documents` (donc `morceaux`), `exclus` et `noeuds` (donc `liens`). `exclusions_manuelles` est rattachée par le nom et doit être effacée explicitement.

### Source de vérité pour les passes

- Le travailleur (`rag/travailleur.py`, `passe`) et `rag indexer` (`rag/cli.py`, `_indexer_verrouille`) parcourent : les sources `obsidian` du YAML, puis les sources `geree` de la base, converties en `SourceConf`.
- Après l'amorçage, les sources `type: code` encore présentes dans le YAML sont **ignorées**, avec un avertissement au démarrage. Dans la même PR, elles sont **retirées** de `config/rag.yaml` et déplacées dans `config/sources-maison.yaml`, au format de l'export, qui sert d'export de référence pour l'instance de Nicolas.
- `indexer_source` ne crée plus une source de type `code` qui n'existe pas en base (fin de l'`INSERT … ON CONFLICT` pour elles) : une source supprimée pendant une passe ne peut pas être recréée. Il prend `verrouiller_source` avant d'écrire, et s'arrête proprement si la source a disparu.

### Amorçage (passage des sources existantes en base)

*(Mon choix, après la relecture : le YAML principal n'a plus de sources de code dans l'image livrée, donc l'amorçage ne peut pas le lire.)*

Un réglage facultatif `RAG_SOURCES_AMORCE` (vide par défaut) désigne un fichier au format de l'export. La pile `rag` de Nicolas le met à `/app/config/sources-maison.yaml`. Au démarrage de `rag migrer` (ou au premier démarrage du travailleur, au choix du builder, mais une seule fois), si `instance.sources_amorcees` est absent et que le fichier est défini :

- chaque source du fichier est insérée ou mise à jour **par son nom** dans `sources`, avec `geree = true` et ses motifs `exclure` ;
- une ligne existante est gardée telle quelle (même `id`, même `revision`, mêmes documents) ;
- puis `sources_amorcees` est posé.

Ça ne réindexe rien, et `RAG` garde sa règle `banc/**`. Sur une instance neuve, sans ce réglage, l'amorçage ne fait rien et la liste est vide. Une ligne `type = 'code'` déjà en base mais absente du fichier est signalée au journal, sans être gérée ni effacée.

### Jetons d'accès

- Jeton facultatif par source. Sans jeton propre, une source dont l'hôte est `github.com` utilise `RAG_GITHUB_JETON` comme aujourd'hui. Les autres hôtes clonent sans authentification.
- Chiffré avec une clé fournie par `RAG_CLE_SECRETS` (Fernet, de `cryptography`). Si `RAG_CLE_SECRETS` est absente, l'interface refuse d'enregistrer un jeton avec un message clair, et les dépôts publics restent possibles.
- **Jamais réaffiché** : le formulaire de modification montre « jeton enregistré », un champ vide le garde, et une case « retirer le jeton » le supprime. Jamais dans l'export.
- Passé à git comme aujourd'hui, par l'environnement (`http.<origine>.extraheader`), généralisé à l'hôte de l'URL de la source au lieu de `https://github.com` seulement. Les secrets restent masqués dans les erreurs (`LecteurGit._git`).
- **Nom d'utilisateur facultatif** par source (colonne `utilisateur text`, défaut `x-access-token`), envoyé avec le jeton en authentification basique. *(Mon choix : certaines forges, comme Bitbucket, exigent un vrai nom d'utilisateur ; sans ce champ, leurs dépôts privés échoueraient sans bruit.)* Il apparaît dans le formulaire et dans l'export, contrairement au jeton.

### Validation d'une source

- **Nom** : même règle que `SourceConf.nom` (`^[\w.-]+$`, ni `.` ni `..`), unique dans `sources`, coffre compris. Il ne change pas après la création.
- **URL** : `https://` seulement, avec un hôte, sans identifiants dans l'URL. Git est lancé avec `GIT_ALLOW_PROTOCOL=https`. Les adresses du réseau local (`192.168.…`, `gitea.maison`, etc.) sont **permises**. *(Mon choix : seul le détenteur du jeton privé, depuis une origine locale, peut ajouter une source ; il administre déjà l'instance, et un Gitea ou un GitLab à la maison est un cas normal pour une instance installée chez soi.)*
- **Branche** : non vide, refusée si elle commence par `-`.
- **Motifs `exclure`** : un par ligne, même validation que les motifs manuels (`valider_motif`).
- **Vérification avant d'enregistrer** : `git ls-remote --heads <url> <branche>` avec le jeton, délai court (30 s). Si le dépôt est injoignable, le jeton refusé ou la branche absente, rien n'est enregistré et le message (secrets masqués) s'affiche dans le formulaire.

### Effets d'une modification

| Ce qui change | Effet |
|---|---|
| Visibilité | Immédiat (`UPDATE sources`), sans réindexation : les lectures filtrent déjà par la visibilité de la source. |
| Jeton | Enregistré ; réindexation demandée. |
| Motifs `exclure` | Enregistrés ; réindexation demandée, **sans le garde-fou `CoffrePartielError`** pour cette passe de cette source (les fichiers nouvellement exclus disparaissent, c'est voulu). |
| URL ou branche | Les documents, nœuds et `exclus` de la source sont effacés tout de suite (le contenu n'est plus le même dépôt), `revision` et `graphe_revision` sont remis à `NULL`, le clone est marqué à refaire ; la réindexation complète est demandée. Les exclusions manuelles sont gardées. |

Toute modification enregistrée appelle `demander_reindexation` (donc `NOTIFY rag_reindexer`) : la passe part tout de suite si aucune n'est en cours, sinon juste après.

### Ajout

Validation, vérification `ls-remote`, `INSERT` avec `geree = true` et `demande_le = now()`, `NOTIFY`. La ligne apparaît dans l'écran Sources « en attente », puis « jamais indexée » pendant la passe, puis avec ses comptes.

### Suppression

- Écran de confirmation : nom, URL, nombre de documents, de fichiers exclus et d'exclusions manuelles qui seront effacés. Bouton « Supprimer définitivement ».
- En une transaction, sous `verrouiller_source` : `DELETE FROM exclusions_manuelles WHERE source = nom`, puis `DELETE FROM sources WHERE nom = nom AND geree` (cascade sur le reste).
- Le clone sur disque est effacé par le travailleur : à chaque tour, il supprime les dossiers de `donnees/clones/` qui ne correspondent à aucune source (la règle existante « jamais de `rmtree` hors du dossier des clones » s'applique). Un clone marqué à refaire après une modification d'URL ou de branche est traité de la même façon.
- Le coffre (source non `geree`) ne peut pas être supprimé ni modifié depuis l'interface.

### Export / import

- `GET /ui/sources/exporter` et `rag sources exporter` : YAML, liste de `{nom, emplacement, branche, visibilite, exclure}`. Jamais de jeton.
- `POST /ui/sources/importer` et `rag sources importer <fichier>` : tout est validé avant d'écrire, en une transaction. Une source inconnue est ajoutée, une source connue est mise à jour (mêmes effets qu'une modification) ; aucune n'est supprimée. La vérification `ls-remote` n'est pas faite à l'import (cinquante dépôts) : une source injoignable apparaît en échec à sa passe. Les sources importées sans jeton utilisent `RAG_GITHUB_JETON` si elles sont sur `github.com` ; sinon le jeton se saisit ensuite dans l'interface.
- `rag sources lister` pour voir la liste en ligne de commande.

### MCP et API

`rag_sources` (MCP) et `/etat` restent en lecture seule et ne montrent jamais de jeton. Pas d'outil MCP d'écriture : les agents voient, ils ne gèrent pas les sources (même règle que pour les exclusions).

### Écrans (texte)

```
Sources                                          [Ajouter une source git] [Exporter] [Importer]
Source        Type  Visibilité  Révision  Indexée le  Documents ... Réindexation  Actions
coffre-maison obs.  public      …         …           1 204         [Réindexer]   (config)
AI            code  public      3f2a…     …           812           [Réindexer]   [Modifier] [Supprimer]
```

```
Ajouter une source git
Nom          [__________]   (lettres, chiffres, . _ - ; ne change plus ensuite)
URL https    [https://github.com/…/….git]
Branche      [main]
Visibilité   (•) public-maison  ( ) prive
Exclure      [banc/**          ]   un motif par ligne
Jeton        [••••••]  facultatif, jamais réaffiché
                                   [Vérifier et ajouter]
```

## Laissé au builder

- Le découpage en tâches et en PR (migration, lecture des sources par le travailleur, amorçage, écrans, export/import, CLI).
- Où exactement l'amorçage se déclenche (`rag migrer` ou démarrage du travailleur), tant qu'il n'a lieu qu'une fois et ne réindexe rien.
- Comment l'indexeur reçoit la consigne « sans garde-fou pour cette passe » (colonne, valeur de `demande_le`, paramètre), tant qu'elle ne vaut que pour la passe qui suit la modification.
- Le détail htmx des formulaires et des messages, dans le style des écrans d'exclusions.
- L'emplacement de la clé `RAG_CLE_SECRETS` dans OpenBao et la pile Portainer (même chemin que les autres secrets de la pile `rag`).
- Arrêter ou laisser finir une passe déjà en cours sur l'ancienne URL ou l'ancienne branche, tant que son résultat ne survit pas à l'effacement.
- Le ménage des dossiers `donnees/graphe/<nom>` d'une source supprimée, en même temps que celui des clones.
- Rendre `RAG_GITHUB_JETON` facultatif (`rag/demarrage.py`, pile Portainer), puisque des dépôts publics ou hors de GitHub n'en ont pas besoin.
- Refuser un nouveau nom déjà pris par une source du YAML (le coffre), même si sa ligne n'existe pas encore en base.
