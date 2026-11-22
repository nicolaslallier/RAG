# Mesures — plan 2 (code et graphe), 2026-11-21

Base locale `rag_plan2` (conteneur `rag-dev-postgres`), `config/dev.yaml`, coffre lu dans
`~/Obsidian/Maison`, 12 dépôts clonés (`origin/main`). Fichier séparé de `docs/mesures.md` pour ne
pas entrer en conflit avec le banc de référence (plan 1, tâches 12-13) mené en parallèle.

## Indexation du code (première passe, embeddings `qwen3-embedding:0.6b` via LiteLLM)

| Dépôt | Fichiers | Morceaux | Exclus | Durée | Nœuds | Liens | Nœuds reliés à un document |
|---|---|---|---|---|---|---|---|
| AI | 257 | 3110 | 4 | 709 s | 4861 | 14209 | 93 % |
| Infra | 234 | 1920 | 3 | 617 s | 2781 | 4128 | 96 % |
| Jarvis | 198 | 1207 | 3 | 226 s | 2016 | 4435 | 84 % |
| EA | 860 | 9568 | 6 | 2307 s | 16333 | 52533 | 91 % |
| DarkAngel | 253 | 2247 | 3 | 572 s | 2824 | 6991 | 90 % |
| Final | 60 | 576 | 84 | 151 s | 917 | 2483 | 93 % |
| Glee | 7 | 10 | 1 | 4 s | 17 | 13 | 88 % |
| Heaven | 56 | 470 | 2 | 98 s | 939 | 2134 | 91 % |
| Midas | 29 | 63 | 4 | 27 s | 115 | 115 | 95 % |
| Moria | 98 | 1486 | 24 | 397 s | 2686 | 7777 | 88 % |
| orchestre | 11 | 39 | 0 | 15 s | 64 | 59 | 100 % |
| RAG | 48 | 412 | 4 | 128 s | 541 | 1027 | 90 % |
| coffre-maison | 91 | 675 | 0 | 8 s | 91 | 211 (`[[liens]]`) | 100 % |

Durées : indexation texte (le gros du temps est l'embedding) ; graphify (en conteneur
`--network none`) ajoute de 2 s à ~60 s par dépôt. Secrets : aucun `PRIVATE KEY`, `ghp_`, `AKIA`
ni `sk-…` en base pour le code (`exclus` de raison `secret` : `Infra:scripts/test.sh`, deux tests
et un plan du dépôt RAG qui contiennent des clés factices). Le seul `sk-…` trouvé en base vient
d'une note de veille du coffre (déjà connu au plan 1, faux positif accepté).

## Non-régression du coffre (35 questions du coffre, jeu inchangé, `--sans-indexer`)

| Mesure | Coffre seul | Coffre + code |
|---|---|---|
| rappel@5 | 0,86 | 0,80 (−0,06) |
| rappel@10 | 0,89 | 0,81 (−0,07) |
| MRR | 0,93 | 0,88 (−0,05) |
| nDCG@10 | 0,85 | 0,78 (−0,07) |
| latence p50 / p95 | 35 / 377 ms | 252 / 385 ms |

**Régression au-delà du seuil de 0,02.** Cas perdus : `projets-en-cours`, `notes-techno`,
`vaisselle`, `delais-portail`, `keycloak-machine`. Cause observée (`keycloak-machine`, question
avec une faute de frappe « keycloack ») : la liste floue (20 places, sur `titre_norm` = chemin pour
le code) se remplit de fichiers de code dont le chemin contient « keycloak » (ADR et specs d'EA),
et la note « Infra - Stack Keycloak » qui n'était trouvée que par le flou sort du top 10. Pour les
autres cas, des fichiers de code (docs, specs des dépôts) passent devant des notes attendues.

Remède proposé, **non écrit** (nouveau levier à valider par Nicolas) : un poids par type de source
dans la fusion RRF (`recherche.poids_source: {obsidian: 1.0, code: 0.6}` par exemple), et/ou une
liste floue séparée par type de source. À mesurer au banc avant de toucher `config/rag.yaml`.

Index HNSW : la CTE `base` matérialisée est supprimée ; la liste vecteur passe maintenant par
`morceaux_embedding_idx` (vérifié par `EXPLAIN ANALYZE`, 24 ms au lieu de 82 ms sur 21 783
morceaux) à deux conditions : statistiques à jour (`rag indexer` fait `ANALYZE` à la fin) et
`hnsw.ef_search = 200` (à 40 par défaut, le rappel@10 tombait à 0,74). Le reste de la latence vient
des listes plein texte et floue, qui balaient encore tous les morceaux.

## Graphe

- `rag expliquer indexer_source` : fonction de `RAG/rag/indexeur.py#L94`, communauté de 30 nœuds,
  26 voisins (`_indexer` l'appelle ; elle appelle `version_courante`, `motif_exclusion`…).
- `rag chemin Portail LiteLLM` : `ambigu` (4 candidats « portail », 3 « litellm », dans AI, Infra,
  EA) ; entre `id:3374` (AI, stack Portainer) et `id:6026` (Infra, compose) : `aucun_chemin` — les
  dépôts ne se rejoignent que par la passe sémantique (mentions dans les notes).
- `[[liens]]` : 211 liens, 16 cibles non résolues. Deux bogues possibles à trancher : `[[2026-11-12]]`
  etc. (le journal de Claude) visent un nom porté par deux notes (`05 - Journal/` et
  `03 - Ressources/Veille IA/`), ni l'une ni l'autre dans le même dossier : non résolu par la règle
  du plan, alors qu'Obsidian en choisit une. Les autres (`Jarvis`, `create a link`, un dossier) sont
  normaux.
- `rag doublons` (seuil 0,92) : les 10 premières paires sont des journaux quotidiens ou des veilles
  au même gabarit (0,956 à 0,988) — vrais « gabarits proches », pas des doublons ; le seuil est
  probablement trop bas pour ces dossiers. `rag orphelines` : 15 notes.

## Passe sémantique (`rag graphe-semantique --max 10`, gemma `gemma4:26b-a4b-it-qat`)

- 5 notes traitées, 5 en échec (`ReadTimeout` à 300 s côté client), 164 liens, **1947 s**. À ce
  rythme la passe complète (91 notes) ne tient pas en 2 h : passe complète **non lancée**. À
  trancher : modèle plus petit pour les concepts, délai plus long, ou `--max` par nuit (les notes en
  échec sont reprises la nuit suivante).
- Concepts sensés (RAG, Claude, MLX, LoRA, Obsidian, Jarvis, dev-ai). Mentions de dépôts justes
  (AI, Jarvis, RAG). En revanche la règle « symbole d'au moins 6 caractères » liait des mots
  courants (`format`, `documents`, `compare`, `service`) à des fonctions homonymes : corrigé, un
  libellé n'est retenu que s'il a une forme de code (`_` ou casse chameau) ou s'il est cité entre
  backticks.

## Poids par type de source (`recherche.poids_source`), 2026-11-22

Accord de Nicolas (« Oui, poids par source »). Le score RRF de chaque morceau est multiplié par le
poids de son type de source (`obsidian`, `code` ; 1.0 si absent). La liste floue garde maintenant
20 places **par type de source** : les chemins de code ne chassent plus les titres de notes (cause
de `keycloak-machine`). Banc sur `rag_plan2`, `--sans-indexer` ; « code » = les 11 questions de code
avec attendus (les 3 `sans_reponse` ne comptent pas dans le rappel).

| poids code | coffre rappel@10 | coffre MRR | coffre nDCG@10 | code rappel@10 | code MRR |
|---|---|---|---|---|---|
| (coffre seul, sans code) | 0,886 | 0,930 | 0,853 | — | — |
| 1.0 | 0,812 | 0,883 | 0,781 | 0,522 | 0,727 |
| 0.8 | 0,858 | 0,919 | 0,835 | 0,522 | 0,712 |
| **0.6** | **0,872** | **0,933** | **0,846** | **0,522** | **0,700** |
| 0.5 | 0,872 | 0,933 | 0,847 | 0,431 | 0,682 |
| 0.4 | 0,881 | 0,933 | 0,855 | 0,431 | 0,606 |
| 0.3 | 0,881 | 0,933 | 0,855 | 0,310 | 0,500 |
| 0.2 | 0,881 | 0,933 | 0,855 | 0,310 | 0,500 |

**Retenu : `code: 0.6`** dans `config/rag.yaml` et `config/dev.yaml`. C'est la plus haute valeur qui
atteint la cible du coffre (rappel@10 ≥ 0,87, MRR ≥ 0,91) ; le rappel du code y est inchangé par
rapport à 1.0 (0,522) et son MRR ne perd que 0,03. En dessous de 0,6, le code s'effondre
(0,43 puis 0,31) pour un gain du coffre de 0,01. Rappel du coffre encore à −0,014 de la
référence sans code (sous le seuil de 0,02). Les questions de code sont encore `propose` : à
remesurer après correction par Nicolas.
