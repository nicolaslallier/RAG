# Preuve graphify en conteneur (plan 1, tâche 2) — 2026-11-21

Décision : A
Sémantique : maison (provisoire — à refaire quand la clé LiteLLM `rag` existera)

## Image
- `spike/graphify/Dockerfile` (python:3.13-slim + git + `pip install graphifyy`).
- `graphifyy` **0.9.114**, image **591 Mo**.

## Extraction structurelle sans réseau ni LLM (`--network none`)
- `graphify update <chemin>` n'a **pas d'option de sortie** : il écrit toujours `<chemin>/graphify-out/`.
  Sur un montage `:ro` : `OSError: [Errno 30] Read-only file system: '/src/graphify-out'`.
  Contournement retenu : monter la source en `:ro`, la copier dans le conteneur (`cp -a /src /w`),
  lancer `graphify update /w`, recopier `/w/graphify-out/` vers un volume de sortie.
  (`graphify extract --out DIR` existe mais fait aussi l'extraction sémantique par LLM.)
- Piège Colima : seul `$HOME` est partagé avec la VM ; un montage sous `/private/tmp` arrive vide
  (« No code files found »).
- Code de sortie 0, `--network none` :

| Dépôt | Langages | Durée | Nœuds | Liens |
|---|---|---|---|---|
| RAG (ce dépôt) | py, md, yaml | 3 s | 294 | 334 |
| Moria | Rust (1893 nœuds `.rs`) | 2 s | 2686 | 7777 |
| AI | py, rs, ts (143), vue (198) | 62 s | 4868 | 14238 |

- `.sql` ignoré sans `graphifyy[sql]` (avertissement explicite). Pas de `.git` requis.

## Forme du JSON (format node-link de networkx)
- Clés racine : `directed`, `multigraph`, `graph` (`{schema_version: 1, graphify_version}`),
  `nodes`, `links` (**`links`**, pas `edges`), `hyperedges`, parfois `built_at_commit`.
- Nœud : identité **`id`** (ex. `crates_moria_client_src_game_session`), `label`, `norm_label`,
  `source_file` (relatif), `source_location` (`"L901"`), `file_type`, `node_kind`, `type`,
  `community` (int), `community_name`, `_origin` (`ast`), plus des champs `_py_*`, `_rust_*`, `_ts_*`.
- Lien : `source`, `target`, `relation` (`contains`, `calls`, `references`, `imports`,
  `imports_from`, `method`, `inherits`, `uses`, `case_of`, `depends_on`, `implements`,
  `rationale_for`…), `confidence` (**`EXTRACTED` | `INFERRED`** observés ; `AMBIGUOUS` documenté,
  non observé), `confidence_score`, `weight`, `source_file`, `source_location`, `context`,
  `also_seen` (doublons fusionnés).
- Extrait : `tests/fixtures/graphify/graph.json` (138 nœuds de `rag/`, `tests/`, `config/` et leurs 161 liens).

## Incrémental
- Après modification d'un fichier : « Incremental: re-extracting 2 of 21 scanned files that changed ».
- Après suppression d'un fichier : ses nœuds disparaissent (295 → 277) **sans** `--force`.
  `--force` reste à prévoir quand la reconstruction a moins de nœuds que le graphe existant
  (garde-fou documenté par `graphify update --help`), donc le passer après des suppressions.

## Extraction sémantique par modèle local
- `extract_corpus_parallel(files, backend='kimi', api_key=None, model=None, …)`.
- Backends : gemini, kimi, claude, openai, deepseek, ollama. `openai` respecte `OPENAI_BASE_URL`
  et `OPENAI_MODEL` → LiteLLM (`/v1`, compatible OpenAI) est techniquement visable.
- Défauts infonuagiques présents dans `llm.py` : `api.openai.com` (si `OPENAI_BASE_URL` absent),
  Gemini, Moonshot, DeepSeek, Anthropic. Le backend est **auto-détecté par les clés d'environnement**
  : l'appeler toujours avec `--backend openai` explicite, `OPENAI_BASE_URL` posé, et jamais
  `GEMINI_API_KEY`/`GOOGLE_API_KEY`/`OPENAI_API_KEY` d'un fournisseur dans l'environnement.
- **Non essayé en vrai** : la clé LiteLLM `rag` n'existe pas encore (PR Infra A) ; aucune autre clé
  n'a été utilisée. D'où « sémantique : maison » provisoire ; refaire l'essai sur 3 notes copiées
  dès que la clé existe, et passer à « sémantique : graphify » s'il réussit.
