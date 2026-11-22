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

## Calibrage du juge (relecture de Nicolas, 2026-11-22)

Nicolas a relu les 20 cas de `generation-base-relecture.md` : « les 20 question générer sont
bonne ». Sur ces 20 cas, 11 réponses non refusées ont été notées par le juge, toutes avec une
fidélité ≥ 4 et une pertinence ≥ 4 (9 à 5/5, une à 5/4, une à 4/4) : **accord de 11 sur 11**
(aucune bonne réponse sous-notée). Les 9 autres cas n'ont pas de note du juge (4 listes, 5 refus).
Nicolas les juge bons aussi, y compris le refus de `vue-infra`, que le banc compte pourtant
comme un faux refus.

## Tâche 10 : config v1 et cibles (décision de Nicolas, 2026-11-22)

Config retenue (`config/rag.yaml`, `config/dev.yaml`) : vecteur + texte + flou, instruction
Qwen3, top 40, 8 passages, refus au cosinus 0,5723, rerank désactivé par défaut (à activer si
la mesure sur le Mac Studio, plan 4 tâche 6, tient p95 ≤ 2 s ; seuil 0,2143).

**Source nommée en mode répondre** : elle donne un bonus de score (`recherche.bonus_source`,
score × 1,2) et ne filtre plus. Réglage au banc : 0,2 → banc identique à la config sans bonus ;
0,5 → MRR −0,02 ; 1,0 → rappel@10 −0,03 et faux refus 10,9 %. Valeur retenue : **0,2**.
`architecture-ai` cite maintenant des notes du coffre (« AI - Documentation ») mais aucun
fichier du dépôt AI (47 s) : le critère « une note et un fichier » n'est toujours pas atteint ;
le poids du code (0,6) écarte les fichiers du dépôt.

`dashboards-grafana` : le critère du banc accepte maintenant `metadonnees` + « probable ».

| cible v1 | seuil | mesure (`v1-recherche` / `generation-base`) | état |
|---|---|---|---|
| rappel@10 | ≥ 0,78 | 0,788 | tenue |
| MRR | ≥ 0,85 | 0,874 | tenue |
| refus justes | ≥ 0,85 | 0,833 au banc de recherche (seuil seul) ; 1,00 au banc de génération | tenue en génération, juste sous le seuil sur le seuil seul |
| faux refus | ≤ 0,08 | 0,087 (recherche) ; 0,065 (génération) | tenue en génération, juste au-dessus en recherche |
| fidélité | ≥ 4,5 | 4,87 | tenue |
| exactitude du mode | ≥ 0,9 | 0,9375 | tenue |
| répondre p95 | ≤ 30 s, sans autre charge sur gemma | 66 s (gemma partagé) | **non vérifiée** |

### Banc de génération final `v1-generation` (2026-11-22, config v1 avec bonus de source)

| fidélité | pertinence | refus justes | faux refus | citations invalides | exactitude du mode | p95 | juges illisibles |
|---|---|---|---|---|---|---|---|
| 4,85 | 4,74 | 100 % | 6,5 % (famille, keycloak-machine, litellm-config-et-notes) | 0 | 0,9375 | **204 s** | 4 |

Réponses générées (31 non refusées) : p50 135 s, max 230 s. **gemma était partagé pendant toute
la mesure** : le plan 1 (`RAG-wt-plan1b`) faisait tourner `rag banc-synthetique` (génération
par gemma), et deux bancs de recherche, sur toute la durée (≈ 63 min). Le p95 ≤ 30 s n'est donc
toujours pas vérifié ; à mesurer gemma libre (ou sur le Mac Studio, plan 4).
`architecture-ai` : non refusée, fidélité 5, pertinence 5.

| cible v1 | seuil | mesure finale | état |
|---|---|---|---|
| rappel@10 | ≥ 0,78 | 0,788 | tenue |
| MRR | ≥ 0,85 | 0,874 | tenue |
| refus justes | ≥ 0,85 | 1,00 (génération) ; 0,833 (seuil seul) | tenue |
| faux refus | ≤ 0,08 | 0,065 (génération) ; 0,087 (seuil seul) | tenue |
| fidélité | ≥ 4,5 | 4,85 | tenue |
| exactitude du mode | ≥ 0,9 | 0,9375 | tenue |
| répondre p95 | ≤ 30 s sans autre charge | 204 s, gemma partagé | non vérifiée |

### Latence sans charge `v1-latence` (2026-11-22, gemma libre)

Le banc synthétique du plan 1 était suspendu (processus arrêté, état `T`) ; un banc de recherche
du plan 1 (sans génération) a fini pendant la mesure. Sous-ensemble de 20 cas de l'étalon
(ni listes ni sans réponse) ; 18 réponses générées non refusées.

| p50 | p95 | max | fidélité | pertinence | faux refus | citations invalides |
|---|---|---|---|---|---|---|
| **25,8 s** | **170,9 s** | 189,0 s | 4,94 | 4,78 | 5 % | 0 |

**Verdict : cible « répondre p95 ≤ 30 s » non tenue.** La médiane tient (26 s), mais quatre cas
dépassent 2 minutes (version-litellm 189 s, mcp-fichiers 171 s, separation-stacks 153 s,
hotpot-bouillons 125 s) alors que deux d'entre eux ont une réponse courte (265 et 124
caractères) : la longueur n'explique pas tout. Hypothèse non vérifiée : rechargement de modèle
côté Ollama, car le banc alterne gemma (générateur) et `qwen3.8:27b-mlx` (juge) ; une seconde
génération (citations à refaire) est l'autre cause possible. À trancher au plan 4 sur le Mac
Studio, avec un journal du temps par appel au modèle, sans le juge.
