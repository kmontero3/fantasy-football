from uuid import uuid4

from pyspark.sql import SparkSession

from config.league_config import (
    CATALOG,
    BRONZE_SCHEMA,
    SLEEPER_USERNAME,
    TARGET_LEAGUE_NAME,
    SEASONS,
)
from client import SleeperClient
from bronze_writer import write_raw


def normalize_name(value):
    return " ".join(str(value or "").strip().lower().split())


def discover_phinasty_leagues(client):
    """Discover only the Phinasty league chain for SLEEPER_USERNAME.

    1. Resolve kmontero3 -> Sleeper user_id.
    2. List that user's NFL leagues for each configured historical season.
    3. Select only the exact league name 'Phinasty League'.
    4. Walk previous_league_id links to preserve the dynasty's historical chain.
    5. Never ingest an unrelated league such as 'Foosballs'.
    """
    user = client.user(SLEEPER_USERNAME)
    user_id = user["user_id"]

    discovered = {}

    for season in SEASONS:
        leagues = client.user_leagues(user_id, season)

        matches = [
            league
            for league in leagues
            if normalize_name(league.get("name")) == normalize_name(TARGET_LEAGUE_NAME)
        ]

        for league in matches:
            discovered[league["league_id"]] = {
                "league_id": league["league_id"],
                "season": int(league.get("season", season)),
                "name": league.get("name"),
                "previous_league_id": league.get("previous_league_id"),
                "user_id": user_id,
            }

    # Also follow previous_league_id from every discovered Phinasty league.
    # This protects the historical chain even if a prior league does not
    # appear in the user's league list for one of the configured seasons.
    queue = list(discovered.values())
    while queue:
        current = queue.pop()
        previous_id = current.get("previous_league_id")

        if not previous_id or previous_id in discovered:
            continue

        previous = client.league(previous_id)
        if normalize_name(previous.get("name")) != normalize_name(TARGET_LEAGUE_NAME):
            raise RuntimeError(
                f"League chain returned unexpected league "
                f"{previous_id}: {previous.get('name')!r}"
            )

        record = {
            "league_id": previous["league_id"],
            "season": int(previous.get("season")),
            "name": previous.get("name"),
            "previous_league_id": previous.get("previous_league_id"),
            "user_id": user_id,
        }
        discovered[previous["league_id"]] = record
        queue.append(record)

    return user, sorted(discovered.values(), key=lambda x: x["season"])


def main():
    spark = SparkSession.builder.getOrCreate()
    client = SleeperClient()
    run_id = str(uuid4())

    user, target_leagues = discover_phinasty_leagues(client)

    # Persist the resolved user and league discovery response. This makes the
    # historical load auditable and documents exactly which league IDs were
    # selected.
    write_raw(
        spark,
        CATALOG,
        BRONZE_SCHEMA,
        "sleeper_user_raw",
        [user],
        endpoint=f"/user/{SLEEPER_USERNAME}",
        run_id=run_id,
    )

    for league_info in target_leagues:
        season = league_info["season"]
        league_id = league_info["league_id"]

        league = client.league(league_id)
        users = client.users(league_id)
        rosters = client.rosters(league_id)
        winners = client.winners_bracket(league_id)
        losers = client.losers_bracket(league_id)
        traded_picks = client.traded_picks(league_id)

        write_raw(
            spark, CATALOG, BRONZE_SCHEMA, "sleeper_league_raw",
            [league], season=season, league_id=league_id,
            endpoint=f"/league/{league_id}", run_id=run_id,
        )
        write_raw(
            spark, CATALOG, BRONZE_SCHEMA, "sleeper_users_raw",
            users, season=season, league_id=league_id,
            endpoint=f"/league/{league_id}/users", run_id=run_id,
        )
        write_raw(
            spark, CATALOG, BRONZE_SCHEMA, "sleeper_rosters_raw",
            rosters, season=season, league_id=league_id,
            endpoint=f"/league/{league_id}/rosters", run_id=run_id,
        )
        write_raw(
            spark, CATALOG, BRONZE_SCHEMA, "sleeper_winners_bracket_raw",
            winners, season=season, league_id=league_id,
            endpoint=f"/league/{league_id}/winners_bracket", run_id=run_id,
        )
        write_raw(
            spark, CATALOG, BRONZE_SCHEMA, "sleeper_losers_bracket_raw",
            losers, season=season, league_id=league_id,
            endpoint=f"/league/{league_id}/losers_bracket", run_id=run_id,
        )
        write_raw(
            spark, CATALOG, BRONZE_SCHEMA, "sleeper_traded_picks_raw",
            traded_picks, season=season, league_id=league_id,
            endpoint=f"/league/{league_id}/traded_picks", run_id=run_id,
        )

    print(
        f"Resolved Sleeper user {user['username']} ({user['user_id']}). "
        f"Selected Phinasty leagues: "
        f"{[(x['season'], x['league_id']) for x in target_leagues]}"
    )


if __name__ == "__main__":
    main()
