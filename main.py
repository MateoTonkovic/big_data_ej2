#!/usr/bin/env python3
import argparse
import gzip
import io
import os
import sys
from typing import List, Optional

import psycopg2
from psycopg2 import sql


NAME_COLS = [
    "nconst", "primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles"
]
TITLE_COLS = [
    "tconst", "titleType", "primaryTitle", "originalTitle",
    "isAdult", "startYear", "endYear", "runtimeMinutes", "genres"
]
RATING_COLS = ["tconst", "averageRating", "numVotes"]


DDL_STATEMENTS = [
    # Schema placeholder {schema}
    """
    CREATE SCHEMA IF NOT EXISTS {schema};
    """,
    # Tables (snake_case names, nullable for raw loads; we’ll cast later if needed)
    """
    CREATE TABLE IF NOT EXISTS {schema}.name_basics (
      nconst TEXT PRIMARY KEY,
      primary_name TEXT,
      birth_year INTEGER,
      death_year INTEGER,
      primary_profession TEXT,
      known_for_titles TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS {schema}.title_basics (
      tconst TEXT PRIMARY KEY,
      title_type TEXT,
      primary_title TEXT,
      original_title TEXT,
      is_adult SMALLINT,
      start_year INTEGER,
      end_year INTEGER,
      runtime_minutes INTEGER,
      genres TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS {schema}.title_ratings (
      tconst TEXT PRIMARY KEY,
      average_rating NUMERIC(3,1),
      num_votes INTEGER
      -- You can add FK to title_basics(tconst) after loading if you want
    );
    """,

    "CREATE INDEX IF NOT EXISTS idx_title_basics_type ON {schema}.title_basics(title_type);",
    "CREATE INDEX IF NOT EXISTS idx_title_ratings_votes ON {schema}.title_ratings(num_votes DESC);",
]


def open_textmaybe_gz(path: str):
    """
    Open a TSV or TSV.GZ as a text stream (utf-8).
    """
    if path.endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8", newline="")
    return open(path, "r", encoding="utf-8", newline="")


def run_ddl(conn, schema: str):
    with conn.cursor() as cur:
        for stmt in DDL_STATEMENTS:
            cur.execute(
                sql.SQL(stmt).format(schema=sql.Identifier(schema))
            )
    conn.commit()


def truncate_table(conn, schema: str, table: str):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("TRUNCATE TABLE {}.{};").format(
                sql.Identifier(schema), sql.Identifier(table)
            )
        )
    conn.commit()


def copy_tsv(
    conn,
    schema: str,
    table: str,
    file_path: str,
    table_cols_in_order,
    tsv_header_cols_in_order,
):
    r"""
    COPY FROM STDIN using TEXT mode with tab delimiter.
    We map TSV header order -> our table columns.
    NULLs are '\N' per IMDb.
    """
    mapping = {
        # name.basics
        "nconst": "nconst",
        "primaryName": "primary_name",
        "birthYear": "birth_year",
        "deathYear": "death_year",
        "primaryProfession": "primary_profession",
        "knownForTitles": "known_for_titles",
        # title.basics
        "tconst": "tconst",
        "titleType": "title_type",
        "primaryTitle": "primary_title",
        "originalTitle": "original_title",
        "isAdult": "is_adult",
        "startYear": "start_year",
        "endYear": "end_year",
        "runtimeMinutes": "runtime_minutes",
        "genres": "genres",
        # ratings
        "averageRating": "average_rating",
        "numVotes": "num_votes",
    }

    ordered_table_cols = [mapping[c] for c in tsv_header_cols_in_order]

    copy_sql = sql.SQL(
        "COPY {}.{} ({}) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\t', NULL '\\N')"
    ).format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, ordered_table_cols)),
    )

    total = 0
    with conn.cursor() as cur, open_textmaybe_gz(file_path) as f:
        # Skip header line (TEXT mode has no HEADER option)
        header = f.readline()

        # Optional sanity check (won't stop load if different):
        expected = "\t".join(tsv_header_cols_in_order)
        if header.strip() != expected:
            print(f"⚠️ Header mismatch in {file_path}. Continuing load.")

        cur.copy_expert(copy_sql, f)

        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {}.{};").format(
                sql.Identifier(schema), sql.Identifier(table)
            )
        )
        total = cur.fetchone()[0]
    conn.commit()
    print(f"Loaded {total:,} rows into {schema}.{table} from {os.path.basename(file_path)}")

def main():
    ap = argparse.ArgumentParser(
        description="Load IMDb TSV files (name.basics, title.basics, title.ratings) into PostgreSQL using COPY."
    )
    ap.add_argument(
        "--dsn",
        required=True,
        help="PostgreSQL DSN, e.g. postgresql://user:pass@localhost:5432/imdb",
    )
    ap.add_argument("--schema", default="imdb", help="Target schema (default: imdb)")
    ap.add_argument("--name", required=True, help="Path to name.basics.tsv or .tsv.gz")
    ap.add_argument("--title", required=True, help="Path to title.basics.tsv or .tsv.gz")
    ap.add_argument("--ratings", required=True, help="Path to title.ratings.tsv or .tsv.gz")
    ap.add_argument(
        "--truncate",
        action="store_true",
        help="TRUNCATE tables before load (safe for re-loads).",
    )
    args = ap.parse_args()

    # Connect
    try:
        conn = psycopg2.connect(args.dsn)
    except Exception as e:
        print(f"Failed to connect to Postgres: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        run_ddl(conn, args.schema)

        if args.truncate:
            for t in ("name_basics", "title_basics", "title_ratings"):
                truncate_table(conn, args.schema, t)

        # name.basics.tsv
        copy_tsv(
            conn,
            args.schema,
            "name_basics",
            args.name,
            table_cols_in_order=[
                "nconst",
                "primary_name",
                "birth_year",
                "death_year",
                "primary_profession",
                "known_for_titles",
            ],
            tsv_header_cols_in_order=NAME_COLS,
        )

        # title.basics.tsv
        copy_tsv(
            conn,
            args.schema,
            "title_basics",
            args.title,
            table_cols_in_order=[
                "tconst",
                "title_type",
                "primary_title",
                "original_title",
                "is_adult",
                "start_year",
                "end_year",
                "runtime_minutes",
                "genres",
            ],
            tsv_header_cols_in_order=TITLE_COLS,
        )

        # title.ratings.tsv
        copy_tsv(
            conn,
            args.schema,
            "title_ratings",
            args.ratings,
            table_cols_in_order=["tconst", "average_rating", "num_votes"],
            tsv_header_cols_in_order=RATING_COLS,
        )

        print("✅ Done.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
