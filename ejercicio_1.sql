SELECT
    nb.primary_name AS actor,
    tb.tconst,
    tb.primary_title AS title,
    tb.start_year,
    tr.average_rating,
    tr.num_votes
FROM
    imdb.name_basics nb
    CROSS JOIN LATERAL unnest (
        string_to_array (NULLIF(nb.known_for_titles, '\N'), ',')
    ) AS k (tconst)
    JOIN imdb.title_basics tb ON tb.tconst = k.tconst
    AND tb.title_type = 'movie'
    LEFT JOIN imdb.title_ratings tr ON tr.tconst = tb.tconst
WHERE
    nb.primary_name ILIKE 'Tom Hanks%'
    AND (
        nb.primary_profession ILIKE '%actor%'
        OR nb.primary_profession ILIKE '%actress%'
    )
ORDER BY
    nb.primary_name,
    tb.start_year NULLS LAST,
    tr.num_votes DESC NULLS LAST;