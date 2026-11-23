-- Sources git gérées depuis l'interface : exclusions, jeton chiffré (Coffret) et marqueurs.
ALTER TABLE sources ADD COLUMN exclure text[] NOT NULL DEFAULT '{}';
ALTER TABLE sources ADD COLUMN jeton_chiffre bytea;
ALTER TABLE sources ADD COLUMN utilisateur text NOT NULL DEFAULT 'x-access-token';
ALTER TABLE sources ADD COLUMN geree boolean NOT NULL DEFAULT false;      -- créée depuis l'interface
ALTER TABLE sources ADD COLUMN sans_garde boolean NOT NULL DEFAULT false;
ALTER TABLE sources ADD COLUMN cree_le timestamptz NOT NULL DEFAULT now();

-- Valeurs propres à l'instance (clé de contrôle du coffret, amorçage fait, etc.).
CREATE TABLE instance (
  cle text PRIMARY KEY,
  valeur text NOT NULL
);
