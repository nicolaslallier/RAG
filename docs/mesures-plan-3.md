# Mesures du plan 3

Base `rag_plan3` (copie de `rag_plan2` : coffre local, 91 notes, et 12 dépôts de code ; 21 783 morceaux),
config `config/dev.yaml`, jeu `banc/etalon.yaml` (64 cas dont 18 `sans_reponse`, encore à statut
`propose`). Modèles locaux par LiteLLM ; rerank `BAAI/bge-reranker-v2-m3` sur le CPU du Mac.

## Rerank (tâche 1)

- `noter` sur 30 passages d'environ 400 tokens : **1,6 s** sur le Mac (CPU/MPS, modèle déjà chargé).
- Banc avec `rerank.actif: true` (candidats 30, garder 8) contre le même banc sans rerank :

| config | rappel@5 | rappel@10 | MRR | nDCG@10 | p50 | p95 |
|---|---|---|---|---|---|---|
| sans rerank | 0,770 | 0,788 | 0,878 | 0,769 | 292 ms | 509 ms |
| rerank 30 → 8 | 0,764 | 0,817 | 0,839 | 0,773 | 1 748 ms | 2 159 ms |

Le rerank gagne du rappel@10 et perd du MRR ; le p95 dépasse le budget de 2 s sur le Mac.

## Calibrage du refus (tâche 2)

`rag banc --calibrer`, 46 cas avec réponse, 18 sans réponse. Critère : moyenne des F1 des deux
classes (refuser un `sans_reponse`, rendre un cas avec réponse), seuil le plus bas à égalité.

| score | seuil | F1 moyen | refus justes | faux refus |
|---|---|---|---|---|
| cosinus (sans rerank) | 0,5723 | 0,867 | 83 % | 8,7 % |
| rerank | 0,2143 | 0,943 | 94 % | 4,3 % |

Le refus sans rerank tient (F1 > 0,7) sur cet index, mais le rerank sépare nettement mieux.
Seuils reportés dans `config/rag.yaml` et `config/dev.yaml`. Calibré sur le jeu même qui le
mesure (18 cas) : à revérifier quand Nicolas aura validé l'étalon.

## Aiguillage (tâche 4)

`rag banc --etiquette aiguillage` : `exactitude_mode` **0,8125** (12 cas mal aiguillés sur 64).
Règles corrigées (avec tests) : « combien » ne liste plus (« combien coûte / de temps / de VM »
attendent un fait) ; « c'est quoi mes… », « qu'est-ce que j'ai… » et un « quels/quelles » en
milieu de phrase listent. Après correction : **0,9375**. Restent mal aiguillés, sans règle sûre :
`famille` (« C'est qui ma famille ? », attendu lister), `vaisselle` (« C'est quand la vaisselle
cette semaine ? », attendu lister : étiquette discutable), `heaven-clouds` (gemma, variable),
`fichier-alertes-portail` (« Quelles alertes Grafana… », attendu répondre : contredit la règle
« quelles → lister » du plan).

## Les trois questions (tâche 6, `rag question`, config dev sans rerank)

- `recettes-poulet` : lister par règles, `type=recette`, `tags=[poulet]`, mode `metadonnees`,
  1 note taguée + 4 « probable » (menu de la semaine au poulet, spéciaux IGA…), liens et
  origines ; 0,5 s.
- `architecture-ai` : répondre par règles, citations valides ; **mais l'aiguillage extrait
  `sources=[AI]`** (règle du plan : en mode répondre, seule la source est extraite), donc seuls
  des fichiers du dépôt AI sont cités, aucune note du coffre : le critère de la tâche 10 (une
  note et un fichier) ne peut pas passer tel quel.
- `dashboards-grafana` : lister par règles, mais « grafana » est un **tag** du coffre : mode
  `metadonnees` (3 notes taguées + fichiers `Infra/monitoring/grafana/…` en « probable ») au lieu
  du `regroupement` attendu par la tâche 10.

## Banc de génération (tâche 7, `generation-base`, 64 cas, sans rerank)

| fidélité | pertinence | refus justes | faux refus | citations invalides | exactitude du mode | p95 |
|---|---|---|---|---|---|---|
| 4,87 | 4,68 | 100 % | 6,5 % (3 cas : famille, vue-infra, keycloak-machine, tous par seuil) | 0 | 0,9375 | 66 s |

Le p95 dépasse le budget de 30 s (gemma partagé avec un autre banc pendant la mesure ; une
génération a dépassé 300 s au premier essai, d'où le correctif « une panne n'arrête plus le
banc »). Fichier de relecture (20 cas) : `banc/resultats/20261122-065343-generation-base-relecture.md`.

## Leviers (tâche 10, étape 1 — mesures seulement, rien d'appliqué)

Étalon seulement (pas de jeu synthétique dans cette base), contre la config dev actuelle
(référence `aiguillage-2` : rappel@5 0,770, rappel@10 0,788, MRR 0,874, nDCG@10 0,768, p95 467 ms).

| levier | rappel@5 | rappel@10 | MRR | nDCG@10 | p95 | refus justes / faux refus | verdict |
|---|---|---|---|---|---|---|---|
| rerank 30 → 8 | 0,764 | 0,806 (+0,02) | 0,838 (−0,04) | 0,768 | 2 790 ms | 94 % / 4,3 % | aide le refus, neutre au rappel, casse le budget de 2 s sur le Mac |
| vecteur seul (texte 0) | 0,766 | 0,797 | 0,812 (−0,06) | 0,749 | 367 ms | 78 % / 8,7 % | nuit |
| texte seul (vecteur 0) | 0,648 | 0,648 (−0,14) | 0,762 | 0,644 | 389 ms | 83 % / 22 % | nuit |
| sans flou | 0,748 | 0,768 (−0,02) | 0,844 (−0,03) | 0,742 | 130 ms | 83 % / 8,7 % | nuit (mais p95 ÷ 3,6) |
| top 30 | 0,777 | 0,788 | 0,876 | 0,766 | 505 ms | = | neutre |
| top 50 | 0,763 | 0,781 | 0,874 | 0,763 | 619 ms | = | neutre / léger moins |
| rerank, 50 candidats | 0,753 | 0,785 | 0,852 | 0,756 | 8 777 ms | 89 % / 4,3 % | nuit (latence) |
| rerank, garder 5 | 0,737 | 0,737 | 0,830 | 0,732 | 3 456 ms | 94 % / 4,3 % | nuit (rappel@10 borné à 5) |
| sans instruction Qwen3 | 0,728 | 0,753 (−0,04) | 0,856 | 0,745 | 520 ms | 56 % / 6,5 % | nuit |

Non mesuré : `reponse.passages: 5` (banc de génération, ~1 h par passage avec gemma partagé).
