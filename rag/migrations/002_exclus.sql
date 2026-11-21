-- Fichiers exclus après lecture (secret, binaire) : pour ne pas les relire à chaque passe.
CREATE TABLE exclus (
  source_id int NOT NULL REFERENCES sources ON DELETE CASCADE,
  chemin text NOT NULL,
  signature text NOT NULL,
  empreinte text NOT NULL,     -- empreinte d'indexation (donc VERSION_ANALYSEUR) au moment du verdict
  raison text NOT NULL,
  PRIMARY KEY (source_id, chemin)
);
