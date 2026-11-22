CREATE TABLE passes (
  id bigserial PRIMARY KEY,
  genre text NOT NULL CHECK (genre IN ('incrementale', 'nocturne', 'demandee')),
  debut timestamptz NOT NULL DEFAULT now(),
  fin timestamptz,
  ok boolean,
  bilan jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX ON passes (genre, debut DESC);
