-- Graphe de connaissances : structure du code (graphify), [[liens]] du coffre, passe sémantique.
CREATE TABLE noeuds (
  id bigserial PRIMARY KEY,
  source_id int NOT NULL REFERENCES sources ON DELETE CASCADE,
  cle text NOT NULL,                 -- identifiant de graphify, chemin de la note, ou « concept:<nom> »
  libelle text NOT NULL,
  libelle_norm text NOT NULL,
  type text,                         -- note | fichier | fonction | classe | concept | …
  communaute int,
  document_id bigint REFERENCES documents ON DELETE SET NULL,
  ligne int,
  UNIQUE (source_id, cle)
);
CREATE TABLE liens (
  id bigserial PRIMARY KEY,
  de bigint NOT NULL REFERENCES noeuds ON DELETE CASCADE,
  vers bigint NOT NULL REFERENCES noeuds ON DELETE CASCADE,
  type text NOT NULL,
  confiance text NOT NULL CHECK (confiance IN ('EXTRAIT', 'INFERE', 'AMBIGU')),
  origine text NOT NULL CHECK (origine IN ('structure', 'wikilien', 'semantique')),
  UNIQUE (de, vers, type, origine)
);
CREATE INDEX ON liens (de);
CREATE INDEX ON liens (vers);
CREATE INDEX ON noeuds (document_id);
CREATE INDEX ON noeuds USING gin (libelle_norm gin_trgm_ops);
ALTER TABLE documents ADD COLUMN semantique_sha text;   -- sha256 du contenu au dernier passage sémantique
ALTER TABLE sources ADD COLUMN graphe_revision text;    -- révision du dernier graphe de structure chargé
