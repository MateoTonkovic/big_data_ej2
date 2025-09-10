WITH person_movies AS (
  SELECT
    nb.nconst,
    nb.primary_name,
    tr.average_rating
  FROM imdb.name_basics nb
  CROSS JOIN LATERAL unnest(string_to_array(nb.known_for_titles, ',')) AS k(tconst)
  JOIN imdb.title_basics  tb ON tb.tconst = k.tconst AND tb.title_type = 'movie'
  JOIN imdb.title_ratings tr ON tr.tconst = tb.tconst
  WHERE nb.known_for_titles IS NOT NULL
    AND (
      (','||COALESCE(nb.primary_profession,'')||',') ILIKE '%,actor,%'
      OR (','||COALESCE(nb.primary_profession,'')||',') ILIKE '%,actress,%'
    )
    AND tr.num_votes >= 1000
)
SELECT
  primary_name,
  ROUND(AVG(average_rating)::numeric, 3) AS avg_rating,
  COUNT(*) AS films_count
FROM person_movies
GROUP BY primary_name
HAVING COUNT(*) >= 2
ORDER BY avg_rating DESC, films_count DESC, primary_name
LIMIT 10;
