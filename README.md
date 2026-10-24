# Fantasy Football Historical Backfill

Historical backfill workflow for the Phinasty League Sleeper dynasty league.

## League discovery

It starts with:

```text
GET https://api.sleeper.app/v1/user/kmontero3
```

and resolves the Sleeper `user_id`.

It then calls:

```text
GET https://api.sleeper.app/v1/user/{user_id}/leagues/nfl/{season}
```

for the configured historical seasons and selects only the exact league named
`Phinasty`.

The workflow then follows each selected league's `previous_league_id` backward
through the dynasty league chain. This means the unrelated `Foosballs` league
is never included even if `kmontero3` is a member of it.

## Configuration

```text
SLEEPER_USERNAME=kmontero3
TARGET_LEAGUE_NAME=Phinasty League
SEASONS=2023,2024,2025,2026
```

`SLEEPER_USERNAME` and `TARGET_LEAGUE_NAME` can be overridden with environment
variables, while the default is intentionally `kmontero3` / `Phinasty League`.

## Workflow

1. `H01_ingest_global`
2. `H02_ingest_leagues`
3. `H03_ingest_drafts`
4. `H04_ingest_transactions`
5. `H05_ingest_matchups`
6. `H06_ingest_player_stats`
7. `H07_ingest_projections`
8. `H08_build_silver`
9. `H09_build_gold`

The league-aware tasks dynamically use the resolved Phinasty league IDs.

## Bronze

Bronze preserves raw API responses. It does not restrict transactions to
`kmontero3`, because all sides of a transaction are required to reconstruct
asset ownership.

The `kmontero3` user is used to **discover the correct league**, not to throw
away the other managers' transaction records.

## Projection behavior

Projection ingestion is append-only. Every response gets an `ingested_at`
timestamp so future modeling can use point-in-time projection snapshots.

Historical projections cannot be reconstructed merely by querying Sleeper today.
Actual stats and league transaction history are therefore treated separately
from projection snapshots.

## Existing league IDs

The workflow can discover the IDs that correspond to:

```text
2026 Phinasty League
    ↓ previous_league_id
2025 Phinasty League
    ↓ previous_league_id
2024 Phinasty League
    ↓ previous_league_id
2023 Phinasty League
```

The IDs themselves do not need to be hard-coded.

## Important

The Silver/Gold layer should still resolve the `kmontero3` user's roster ID for
each season. Roster IDs can change between leagues/seasons, so never assume a
single roster ID across the entire history.
