-- Quarantaine : décisions prises à la main sur les fichiers écartés par le filtre (secret, genere).
CREATE TABLE quarantaine_decisions (
  id serial PRIMARY KEY,
  source text NOT NULL,        -- nom de la source (pas l'id), comme exclusions_manuelles
  chemin text NOT NULL,        -- chemin exact, en NFC
  decision text NOT NULL CHECK (decision IN ('accepte', 'refuse')),
  signature text NOT NULL,     -- signature du fichier à la décision ; une acceptation ne vaut que pour elle
  regles text[] NOT NULL DEFAULT '{}',   -- accepte : règles levées ; refuse : règle vue au refus
  contenu_sha256 text,         -- contenu_secret levé : sha256 du contenu accepté (relu à chaque passe)
  cree_le timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source, chemin),
  CHECK (regles <@ ARRAY['nom_secret', 'contenu_secret', 'genere']::text[]),
  CHECK (decision <> 'accepte' OR cardinality(regles) > 0),
  CHECK (decision <> 'accepte' OR NOT ('contenu_secret' = ANY(regles)) OR contenu_sha256 IS NOT NULL)
);

-- Ce qui a déclenché le verdict : règle, motif de nom, lignes et extraits masqués. Jamais de contenu.
ALTER TABLE exclus ADD COLUMN detail jsonb;
-- sha256 du contenu lu pour le verdict (NULL si jamais lu) : une acceptation de `contenu_secret`
-- ne vaut que pour ce contenu-là, pas seulement pour la signature.
ALTER TABLE exclus ADD COLUMN contenu_sha256 text;

-- Les secrets déjà trouvés dans un contenu sont relus à la prochaine passe, pour avoir leur détail.
UPDATE exclus SET empreinte = '' WHERE lu AND raison = 'secret';
