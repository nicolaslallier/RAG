-- Nouveau niveau de visibilité « travail » (visible seulement avec le jeton privé).
ALTER TABLE sources DROP CONSTRAINT sources_visibilite_check;
ALTER TABLE sources ADD CONSTRAINT sources_visibilite_check CHECK (visibilite IN ('public-maison', 'prive', 'travail'));
