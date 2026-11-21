CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE TEXT SEARCH CONFIGURATION fr (COPY = french);
ALTER TEXT SEARCH CONFIGURATION fr
  ALTER MAPPING FOR hword, hword_part, word WITH unaccent, french_stem;

CREATE TABLE sources (
  id serial PRIMARY KEY,
  nom text NOT NULL UNIQUE,
  type text NOT NULL CHECK (type IN ('obsidian', 'code')),
  emplacement text NOT NULL,
  visibilite text NOT NULL DEFAULT 'public-maison' CHECK (visibilite IN ('public-maison', 'prive')),
  branche text,
  revision text,              -- dernier commit indexé (code) ; horodatage du listage (coffre)
  indexe_le timestamptz,
  demande_le timestamptz      -- « réindexer maintenant » (plan 3)
);
CREATE TABLE index_versions (
  id serial PRIMARY KEY,
  empreinte text NOT NULL UNIQUE,
  config jsonb NOT NULL,
  cree_le timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE documents (
  id bigserial PRIMARY KEY,
  source_id int NOT NULL REFERENCES sources ON DELETE CASCADE,
  chemin text NOT NULL,
  titre text NOT NULL,
  titre_norm text NOT NULL,   -- titre + alias, minuscules sans accents (flou pg_trgm)
  alias text[] NOT NULL DEFAULT '{}',
  tags text[] NOT NULL DEFAULT '{}',
  type text,
  origine text,               -- champ `source` du frontmatter (URL, livre, claude…)
  date_doc date,
  frontmatter jsonb NOT NULL DEFAULT '{}',
  liens_sortants text[] NOT NULL DEFAULT '{}',   -- cibles des [[liens]] (graphe, plan 2)
  signature text NOT NULL,    -- etag S3, mtime+taille, ou sha de blob git
  sha256 text NOT NULL,
  modifie_le timestamptz,
  UNIQUE (source_id, chemin)
);
CREATE TABLE morceaux (
  id bigserial PRIMARY KEY,
  document_id bigint NOT NULL REFERENCES documents ON DELETE CASCADE,
  version_id int NOT NULL REFERENCES index_versions ON DELETE CASCADE,
  ordre int NOT NULL,
  fil text NOT NULL,          -- fil d'Ariane « Note > H1 > H2 » ou « dépôt > chemin > symbole »
  texte text NOT NULL,
  parent_id bigint REFERENCES morceaux ON DELETE CASCADE,
  symbole text,
  ligne int,
  embedding halfvec(1024),    -- NULL pour un parent (stratégie parent_enfant) : non cherché
  tsv tsvector,
  UNIQUE (document_id, version_id, ordre)
);
CREATE INDEX ON morceaux USING hnsw (embedding halfvec_cosine_ops);
CREATE INDEX ON morceaux USING gin (tsv);
CREATE INDEX ON morceaux USING gin (lower(symbole) gin_trgm_ops);
CREATE INDEX ON documents USING gin (titre_norm gin_trgm_ops);
CREATE INDEX ON documents USING gin (tags);
CREATE INDEX ON documents (type);
