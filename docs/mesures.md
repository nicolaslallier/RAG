# Mesures du banc

## Étalon, comparaison des découpages (avant le rebase sur le plan 2)

Mesuré le 2026-11-21 sur le coffre seul (35 cas avec réponse, 15 sans réponse), vrais embeddings
`qwen3-embedding:0.6b`, code d'avant le plan 2. Une comparaison après le rebase a été lancée puis arrêtée
à la demande de Nicolas : ces chiffres ne sont pas refaits.

| étiquette | rappel@5 | rappel@10 | MRR | nDCG@10 | p95 (ms) |
|---|---|---|---|---|---|
| base (titres) | 0,865 | 0,886 | 0,930 | 0,853 | 116 |
| recursif | 0,863 | 0,892 | 0,945 | 0,864 | 118 |
| parent_enfant | 0,872 | 0,887 | 0,930 | 0,855 | 452 |

Aucune stratégie ne gagne 0,05 de rappel@10 sur `titres` : **`titres` reste le défaut**.
`parent_enfant` multiplie la latence p95 par quatre sans gain.

## Référence posée (`banc/reference.json`, après le rebase)

64 cas valides (50 coffre + 14 code), sources de code incluses, 2026-11-22.
Global (46 cas avec réponse) : rappel@5 0,763, rappel@10 0,781, MRR 0,862, nDCG@10 0,752.

| nature | n | rappel@10 | MRR |
|---|---|---|---|
| factuel | 23 | 0,913 | 0,841 |
| lister | 12 | 0,647 | 0,778 |
| synthetiser | 11 | 0,652 | 1,000 |
| sans_reponse | 18 | cosinus moyen 0,499 | |

Latences (p50 437 ms, p95 580 ms) faussées : un banc de génération du plan 3 tournait sur le même hôte.

Cas à `rappel10 = 0` : `symbole-lanceur-conteneurs`, `agents-du-portail`.

Refus : le meilleur cosinus des cas sans réponse monte à 0,645 alors que 12 des 46 cas avec réponse
ont un meilleur cosinus plus bas ; un seuil de cosinus seul ne sépare pas les deux (à revoir avec le
rerank, plan 2/3).

## Jeu synthétique

Pas encore produit. Une première génération (27 cas) a été retirée à la relecture : environ la
moitié des questions n'étaient pas compréhensibles seules (« dans ces notes », « cette stack »,
questions de journal sans date). Consignes du générateur et du juge resserrées, filtre « vague »
ajouté. À produire par `rag banc-synthetique --n 250` (coffre seul par défaut, au plus 182 cas :
2 morceaux par note), puis `rag banc --jeu banc/synthetique.jsonl --etiquette base-synthetique
--sans-indexer`.
