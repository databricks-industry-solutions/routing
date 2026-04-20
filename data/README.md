# Vendored sample data

`overture_indiana_addresses.csv` is the default local input for the address-first demo.

## Source

- Dataset: Overture open address data
- Release used for the current sample: `2026-04-15.0`
- Access date for the vendored refresh used in this repo: `2026-04-20`
- Primary citation:
  `Overture Maps Foundation, overturemaps.org`

## How this sample is filtered

The refresh script keeps only U.S. address rows that:

- have non-null `street` and `number`
- are in the Indianapolis-metro city list used by this accelerator
- show Indiana in `address_levels`
- are deduplicated by formatted address

The exported CSV is then deterministically shuffled by `address_id` hash so the first
10k or 40k rows still represent a city mix instead of a single town.

## Why the sample is vendored

The runtime demo path should not depend on public Overture downloads. The repo should work
with a local address file plus setup-time Photon / OSRM downloads only.

## Refreshing the sample

Run from the repo root:

```bash
python3 -m pip install duckdb
python3 data/refresh_overture_indiana_addresses.py
```

Optional flags:

```bash
python3 data/refresh_overture_indiana_addresses.py --release 2026-04-15.0 --row-limit 55000
```

## Columns

- `address_id`
- `country`
- `street`
- `number`
- `unit`
- `postcode`
- `postal_city`
- `address_levels_json`
- `formatted_address`

## Attribution notes

Overture's address theme is assembled from multiple permissively licensed data sources,
some of which require attribution. See the Overture attribution page for the source-level
details when adapting this sample to a different geography or redistributing a larger
subset.
