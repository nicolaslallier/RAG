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
