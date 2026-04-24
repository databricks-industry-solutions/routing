#!/usr/bin/env python3
"""Refresh the vendored Indianapolis-area address sample from Overture."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


DEFAULT_RELEASE = "2026-04-15.0"
DEFAULT_ROW_LIMIT = 55_000
DEFAULT_OUTPUT = Path(__file__).with_name("overture_indiana_addresses.csv")
DEFAULT_CITIES = [
    "Indianapolis",
    "Carmel",
    "Fishers",
    "Greenwood",
    "Lawrence",
    "Noblesville",
    "Westfield",
    "Zionsville",
    "Plainfield",
    "Brownsburg",
    "Avon",
    "Speedway",
    "Beech Grove",
    "Southport",
    "Cumberland",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download a deterministic Indianapolis-metro address sample from "
            "Overture and write it as a local CSV for the routing accelerator."
        )
    )
    parser.add_argument(
        "--release",
        default=DEFAULT_RELEASE,
        help="Overture release tag to query.",
    )
    parser.add_argument(
        "--row-limit",
        type=int,
        default=DEFAULT_ROW_LIMIT,
        help="Maximum number of deduplicated rows to export.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Destination CSV path.",
    )
    return parser.parse_args()


def escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def build_query(*, release: str, row_limit: int, output_path: Path) -> str:
    city_sql = ", ".join(
        f"'{escape_sql_literal(city.lower())}'" for city in DEFAULT_CITIES
    )
    remote_path = (
        "s3://overturemaps-us-west-2/"
        f"release/{release}/theme=addresses/type=address/*"
    )
    output_sql = escape_sql_literal(str(output_path))

    return f"""
INSTALL httpfs;
LOAD httpfs;
SET s3_region = 'us-west-2';

COPY (
  WITH raw_source AS (
    SELECT
      id AS address_id,
      country,
      street,
      number,
      unit,
      postcode,
      postal_city,
      to_json(address_levels) AS address_levels_json
    FROM read_parquet(
      '{remote_path}',
      filename = true,
      hive_partitioning = 1
    )
    WHERE country = 'US'
      AND street IS NOT NULL
      AND number IS NOT NULL
      AND lower(coalesce(postal_city, '')) IN ({city_sql})
  ),
  source AS (
    SELECT *
    FROM raw_source
    WHERE lower(coalesce(address_levels_json, '')) LIKE '%"value":"in"%'
       OR lower(coalesce(address_levels_json, '')) LIKE '%indiana%'
  ),
  normalized AS (
    SELECT
      *,
      trim(
        regexp_replace(
          concat_ws(
            ', ',
            concat_ws(' ', number, street),
            nullif(unit, ''),
            nullif(postal_city, ''),
            nullif(postcode, ''),
            country
          ),
          '\\s+',
          ' '
        )
      ) AS formatted_address
    FROM source
  ),
  deduped AS (
    SELECT * EXCLUDE (row_num)
    FROM (
      SELECT
        *,
        row_number() OVER (
          PARTITION BY formatted_address
          ORDER BY address_id
        ) AS row_num
      FROM normalized
    )
    WHERE row_num = 1
  )
  SELECT
    address_id,
    country,
    street,
    number,
    coalesce(unit, '') AS unit,
    coalesce(postcode, '') AS postcode,
    coalesce(postal_city, '') AS postal_city,
    coalesce(address_levels_json, '[]') AS address_levels_json,
    formatted_address
  FROM deduped
  ORDER BY hash(address_id)
  LIMIT {row_limit}
) TO '{output_sql}' (HEADER, DELIMITER ',');
"""


def main() -> int:
    try:
        import duckdb
    except ImportError:
        print(
            "duckdb is required for this refresh script. "
            "Install it with `python3 -m pip install duckdb`.",
            file=sys.stderr,
        )
        return 1

    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    query = build_query(
        release=args.release,
        row_limit=args.row_limit,
        output_path=args.output.resolve(),
    )

    print(f"Refreshing sample from Overture release {args.release}")
    print(f"Output file: {args.output.resolve()}")
    con.execute(query)
    row_count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM read_csv_auto('{escape_sql_literal(str(args.output.resolve()))}', header = true)
        """
    ).fetchone()[0]

    print(f"Wrote {row_count} rows to {args.output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
