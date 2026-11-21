# Rédacteur du coffre : exemples puis petit modèle fine-tuné — design (projet 4)

Date : 2026-11-21. Statut : brouillon à valider par Nicolas.
Dépend de : projet 1 (RAG maître, banc), projet 2 (agent-doc).

## Intention

agent-doc doit écrire les notes **comme Nicolas les écrirait** : style et conventions du coffre Maison (choix de Nicolas : le fine-tuning sert le comportement, pas la connaissance — la connaissance reste dans le RAG).

## Objectifs

- Une note produite par agent-doc respecte les conventions : bon dossier PARA, bon modèle de `06 - Modèles`, frontmatter complet et cohérent (`type`, `tags`, `source`, `date`, champs propres au type, ex. `portions`, `temps_total` pour une recette), titres, longueur, ton, liens `[[…]]` pertinents.
- Mesurer cette conformité au banc et ne garder que ce qui gagne.

## Non-objectifs

- Apprendre au modèle le contenu du coffre (rôle du RAG).
- Fine-tuner gemma4:26b.
- Utiliser des modèles ou du calcul infonuagiques (rien ne sort du homelab).

## Anti-objectifs

- Un modèle qui invente des faits absents de la matière fournie.
- Un modèle figé qui dérive quand les conventions changent, sans que personne ne le voie.

## Approches considérées

1. **Retenu (choix de Nicolas, combiné avec 3)** : d'abord des exemples (few-shot) tirés du RAG, puis un entraînement qui doit battre ce niveau au banc.
2. LoRA directement sur gemma4:26b : rejeté, mémoire juste sur 36 Go, et sans référence on ne sait pas s'il apporte.
3. **Retenu (combiné)** : un petit modèle spécialisé rédacteur.

## Ce qui est décidé

### Étape 1 — exemples (few-shot)
- À chaque écriture, agent-doc tire du RAG 3 à 5 notes du même `type`/dossier et les donne comme modèles au rédacteur. Suit les conventions au fil de l'eau.

### Partage des rôles
- **gemma4:26b-a4b-it-qat** (agent-doc) : décide dossier, type, créer/compléter, liens ; rassemble la matière.
- **Rédacteur** : reçoit matière + décisions + exemples, rend la note finale (markdown + frontmatter). D'abord gemma4:26b lui-même avec exemples (étape 1), puis le petit modèle fine-tuné s'il gagne au banc.

### Données d'entraînement (choix de Claude)
- Paires « brouillon → note finale » fabriquées à partir des notes existantes : gemma4:26b dégrade chaque note (notes en vrac, sans frontmatter ni liens) ; la note d'origine est la cible.
- Notes écrites par Nicolas ou validées par lui en priorité ; exclusion des zones générées (`Discographie.md`, index, journaux) et du niveau `prive`.
- Jeu de test tenu à part (notes jamais vues à l'entraînement), stratifié par `type`.

### Modèle et entraînement (choix de Claude)
- Base : un petit modèle instruct multilingue (~4B, famille gemma ou équivalente), choisi par un court comparatif au banc avant entraînement.
- LoRA avec MLX sur le Mac (M4 Max, 36 Go), adaptateur fusionné et quantifié, servi par Ollama derrière LiteLLM (équipe `agents`, modèle local). Sur la GTX 1650 de l'hôte si ça tient (≈3 Go), sinon sur la machine des autres modèles.
- Réentraînement déclenché à la main quand le banc montre une dérive.

### Banc « conformité »
- Contrôles automatiques : frontmatter valide et champs requis par type, dossier attendu, modèle respecté, liens qui existent dans le coffre, aucun fait absent de la matière (vérifié par un juge local distinct).
- Note globale par type ; comparaison : gemma4 sans exemples, gemma4 avec exemples, petit modèle fine-tuné (avec et sans exemples), plus latence.
- Échantillon relu par Nicolas pour calibrer le juge sur le « ton ».
- Critère d'adoption : le petit modèle remplace le rédacteur s'il égale ou bat gemma4+exemples en conformité, sans hausse des faits inventés.

## Décisions prises par Claude après relecture (à vérifier par Nicolas)

- **Rédacteur** : couvre « créer » et « compléter » (deux jeux de paires : brouillon → note ; note existante + matière nouvelle → note mise à jour). Entraîné avec les exemples dans le prompt, tronqués à un budget fixe ; c'est cette variante qui doit battre gemma4+exemples.
- **Banc hors ligne** dans le dépôt RAG : les décisions (dossier, type, liens) sont fournies comme vérité, extraites de la note d'origine ; on mesure le rédacteur seul. Le plan du projet 4 inclut aussi les changements dans agent-doc (rôle rédacteur séparé, tirage des exemples).
- **Schéma par type** : inféré des notes et des modèles existants, puis une courte liste validée par Nicolas ; types sans modèle (ex. recette) : champs observés.
- **`source` / origine** : `source` = qui a écrit (l'agent appelant ou `nicolas`) ; nouveau champ `origine` = d'où vient le contenu (URL, livre). À reporter dans les specs des projets 1 et 2.
- **Exclusions d'entraînement** : sources Jarvis/courriels, `Personnes`, Inbox des courriels, zones générées.
- **Dérive** : le banc conformité tourne après la réindexation nocturne ; résultat en tête du journal du documentaliste.
- **Adoption** : sur un jeu de test petit, la relecture à l'aveugle d'un échantillon par Nicolas tranche ; la latence est mesurée sur la machine de service réelle.

## Décision de Nicolas : seuil, puis entraînement

Le coffre compte ~65 notes utilisables (relecture du 2026-11-21) : trop peu. Livré maintenant : étape 1 (exemples), banc conformité, pipeline de données. L'entraînement du petit modèle ne se lance qu'au seuil : **300 notes utilisables et au moins 10 par type visé** (types sous le seuil exclus de l'entraînement). Pas de notes synthétiques. Le banc nocturne signale dans le journal quand le seuil est atteint.

## Référence de style (décision de Nicolas : A)

Les notes `source: claude` (61 au 2026-11-21) et `source: mcp` sont la référence : écrites par ses agents selon ses conventions, et gardées par lui. Les notes écrites par Nicolas portent `source: nicolas` et comptent double à l'entraînement (pondération ou duplication).

## Laissé au builder

- Choix exact du modèle de base et des hyperparamètres LoRA.
- Format des jeux de données (JSONL chat).
- Emplacement du code d'entraînement (dépôt RAG, dossier `finetune/`) et des artefacts (hors git).
