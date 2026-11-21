# Banc de mesure

## Format du jeu
`banc/etalon.yaml` : une liste de cas. `banc/synthetique.jsonl` : mêmes champs, un cas par ligne.

| Champ | Valeurs |
|---|---|
| `id` | unique dans le jeu |
| `question` | telle que Nicolas la poserait |
| `nature` | `lister`, `synthetiser`, `factuel`, `sans_reponse` |
| `attendus` | clés de document `"<source>:<chemin>"` ; vide pour `sans_reponse` |
| `mode_attendu` | `lister` ou `repondre` (utilisé au plan 3) |
| `statut` | `propose` (à relire) ou `valide` |
| `note` | pourquoi cette question |

La vérité est au niveau du **document** : un cas est réussi si la note attendue sort, peu importe le morceau.
Les cas `sans_reponse` ne comptent pas dans les moyennes de retrieval ; leur meilleur cosinus sert à calibrer le refus (plan 3).

## Lancer
    uv run rag banc --etiquette essai                  # indexe si besoin, mesure, compare à reference.json
    uv run rag banc --jeu banc/synthetique.jsonl --sans-indexer

Rapports horodatés dans `banc/resultats/` (hors git).

## Règle : un paramètre à la fois
Changer **un seul** levier de `config/rag.yaml` par mesure, sinon on ne sait pas lequel a joué.
Un écart de 0,03 de rappel sur ~45 questions, c'est une ou deux questions : ne pas sur-interpréter.

## Poser une référence
    uv run rag banc --etiquette base --reference

Refusé (code 2) tant qu'un cas est à `statut: propose`.
