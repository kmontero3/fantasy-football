from uuid import uuid4

from pyspark.sql import SparkSession

from config.league_config import CATALOG, BRONZE_SCHEMA, MAX_WEEK
from client import SleeperClient
from league_discovery import discover_phinasty_leagues
from bronze_writer import write_raw


def main():
    spark = SparkSession.builder.getOrCreate()
    client = SleeperClient()
    run_id = str(uuid4())

    _, target_leagues = discover_phinasty_leagues(client)
    for league_info in target_leagues:
        season = league_info['season']
        league_id = league_info['league_id']
        for week in range(1, MAX_WEEK + 1):
            matchups = client.matchups(league_id, week)

            write_raw(
                spark,
                CATALOG,
                BRONZE_SCHEMA,
                "sleeper_matchups_raw",
                matchups,
                season=season,
                league_id=league_id,
                endpoint=f"/league/{league_id}/matchups/{week}",
                run_id=run_id,
            )


if __name__ == "__main__":
    main()
