from uuid import uuid4

from pyspark.sql import SparkSession

from config.league_config import CATALOG, BRONZE_SCHEMA, SEASONS, MAX_WEEK
from client import SleeperClient
from bronze_writer import write_raw


def main():
    spark = SparkSession.builder.getOrCreate()
    client = SleeperClient()
    run_id = str(uuid4())

    for season in SEASONS:
        # Season projection snapshot.
        season_projection = client.projections(season)

        write_raw(
            spark,
            CATALOG,
            BRONZE_SCHEMA,
            "sleeper_projections_raw",
            season_projection,
            season=season,
            endpoint=f"/projections/nfl/{season}?season_type=regular",
            run_id=run_id,
        )

        # Weekly projections. These are snapshots of what Sleeper currently
        # projects for each week. Historical point-in-time snapshots only
        # exist if they were ingested at that time.
        for week in range(1, MAX_WEEK + 1):
            weekly_projection = client.projections(season, week)

            write_raw(
                spark,
                CATALOG,
                BRONZE_SCHEMA,
                "sleeper_projections_raw",
                weekly_projection,
                season=season,
                endpoint=(
                    f"/projections/nfl/{season}/{week}"
                    "?season_type=regular"
                ),
                run_id=run_id,
            )


if __name__ == "__main__":
    main()
