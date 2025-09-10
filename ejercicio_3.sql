CREATE INDEX IF NOT EXISTS idx_nb_primary_name ON imdb.name_basics(primary_name);

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_nb_primary_name_trgm ON imdb.name_basics USING gin (primary_name gin_trgm_ops);

ANALYZE imdb.name_basics;
ANALYZE imdb.title_basics;
ANALYZE imdb.title_ratings;

EXPLAIN (ANALYZE, BUFFERS)
WITH person_movies AS (
  SELECT nb.nconst, tr.average_rating
  FROM imdb.name_basics nb
  CROSS JOIN LATERAL unnest(
    string_to_array(NULLIF(nb.known_for_titles, '\N'), ',')
  ) AS k(tconst)
  JOIN imdb.title_basics tb  ON tb.tconst = k.tconst AND tb.title_type = 'movie'
  JOIN imdb.title_ratings tr ON tr.tconst = tb.tconst
  WHERE nb.primary_profession ~* '\b(actor|actress)\b'
    AND tr.num_votes >= 1000
)
SELECT AVG(average_rating)
FROM person_movies;
