# RAG maître

Index et recherche hybride (vecteur + plein texte + flou) sur le coffre Obsidian Maison
et, plus tard, le code des dépôts. Spec : `docs/superpowers/specs/2026-11-21-rag-maitre-design.md`.

## Développement

    docker compose -f compose.dev.yml up -d     # Postgres 18 + pgvector sur localhost:5434
    uv sync
    uv run ruff check . && uv run pytest        # tests (les tests `vivant` sautent sans RAG_LITELLM_CLE)

Les tests utilisent `RAG_TEST_DATABASE_URL` (défaut `postgresql://rag:rag@localhost:5434/rag_test`,
créée au besoin). Pour travailler sur le vrai coffre (lecture seule) :

    export RAG_DATABASE_URL=postgresql://rag:rag@localhost:5434/rag RAG_LITELLM_URL=... RAG_CONFIG=config/dev.yaml
    uv run rag migrer && uv run rag indexer --complet
    uv run rag chercher "recette de poulet"

## Réponse diffusée (SSE)

`POST /repondre` ou `/question` avec `"flux": true` émet des événements `data: <json>` :
`sources`, puis des `texte` (`delta`), puis `fin`. En flux, la réponse n'est vérifiée qu'à la
fin et il n'y a pas de seconde tentative : **un client doit traiter `fin.citations_valides:
false` comme un refus** (masquer le texte reçu, afficher « Je n'ai pas trouvé cela dans vos
notes. »). Un événement `erreur` signale une panne du modèle en cours de flux.
