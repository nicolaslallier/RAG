-- Exclusions décidées à la main (interface) ; le motif suit fnmatch comme `exclure:` de la config.
CREATE TABLE exclusions_manuelles (
  id serial PRIMARY KEY,
  source text NOT NULL,        -- nom de la source (pas l'id : survit à un import, à une base recréée)
  motif text NOT NULL,         -- chemin exact ou motif fnmatch (« dossier/** », « *.csv »)
  raison text,                 -- facultative, saisie par Nicolas
  cree_le timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source, motif)
);

-- exclus devient le registre de tous les fichiers écartés ; lu = verdict pris après lecture du contenu.
ALTER TABLE exclus ADD COLUMN lu boolean NOT NULL DEFAULT true;
