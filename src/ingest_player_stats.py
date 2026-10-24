from uuid import uuid4

from pyspark.sql import SparkSession

from config.league_config import CATALOG, BRONZE_SCHEMA, SEASONS, MAX_WEEK
from client import SleeperClient
from bronze_writer import write_raw


def main():
    spark = SparkSession.builder.getOrCreate()
    client = SleeperClient()
    run_id = str(uuid4())

    # Stats are global NFL data, so they only need to be retrieved once per
    # season rather than once per league.
    for season in SEASONS:
        season_stats = client.stats(season)

        # The stats endpoint returns a player-id keyed object rather than a
        # list. Store one raw object per player for easier downstream parsing.
        records = [
            {
                "player_id": player_id,
                "stats": stats,
            }
            for player_id, stats in season_stats.items()
        ]

        write_raw(
            spark,
            CATALOG,
            BRONZE_SCHEMA,
            "sleeper_stats_season_raw",
            records,
            season=season,
            endpoint=f"/stats/nfl/regular/{season}",
            run_id=run_id,
        )

        for week in range(1, MAX_WEEK + 1):
            weekly_stats = client.stats(season, week)

            records = [
                {
                    "player_id": player_id,
                    "stats": stats,
                }
                for player_id, stats in weekly_stats.items()
            ]

            write_raw(
                spark,
                CATALOG,
                BRONZE_SCHEMA,
                "sleeper_stats_weekly_raw",
                records,
                season=season,
                endpoint=f"/stats/nfl/regular/{season}/{week}",
                run_id=run_id,
            )


if __name__ == "__main__":
    main()
