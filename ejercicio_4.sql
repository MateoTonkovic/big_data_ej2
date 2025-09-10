WITH params AS (
  SELECT 2010::INT AS y_from, 2019::INT AS y_to, 2500::INT AS m
),
global_mean AS (  -- C
  SELECT AVG(tr.average_rating)::NUMERIC(4,3) AS c
  FROM imdb.title_ratings tr
  JOIN imdb.title_basics tb ON tb.tconst = tr.tconst
  WHERE tb.title_type = 'movie'
    AND tr.num_votes >= (SELECT m FROM params)
),
base AS (
  SELECT
    tb.tconst, tb.primary_title, tb.start_year,
    tr.average_rating AS r, tr.num_votes AS v,
    string_to_array(tb.genres, ',') AS genres_arr
  FROM imdb.title_basics tb
  JOIN imdb.title_ratings tr ON tr.tconst = tb.tconst
  JOIN params p ON true
  WHERE tb.title_type = 'movie'
    AND tb.start_year BETWEEN p.y_from AND p.y_to
    AND tr.num_votes >= p.m
),
exploded AS (
  SELECT
    b.tconst, b.primary_title, b.start_year, b.r, b.v,
    unnest(b.genres_arr) AS genre
  FROM base b
),
scored AS (
  SELECT
    e.genre, e.primary_title, e.start_year, e.r, e.v,
    ((e.v::NUMERIC/(e.v + p.m)) * e.r) + ((p.m::NUMERIC/(e.v + p.m)) * gm.c) AS wr
  FROM exploded e
  CROSS JOIN params p
  CROSS JOIN global_mean gm
)
SELECT *
FROM (
  SELECT
    genre,
    primary_title,
    start_year,
    r AS rating,
    v AS votes,
    wr,
    ROW_NUMBER() OVER (PARTITION BY genre ORDER BY wr DESC, v DESC) AS rk
  FROM scored
) q
WHERE rk <= 20
ORDER BY genre, rk;
