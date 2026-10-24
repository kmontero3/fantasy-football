from uuid import uuid4

from pyspark.sql import SparkSession

from config.league_config import CATALOG, BRONZE_SCHEMA
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
        drafts = client.drafts(league_id)

        write_raw(
            spark, CATALOG, BRONZE_SCHEMA, "sleeper_drafts_raw",
            drafts, season=season, league_id=league_id,
            endpoint=f"/league/{league_id}/drafts", run_id=run_id,
        )

        for draft in drafts:
            draft_id = draft["draft_id"]

            draft_detail = client.draft(draft_id)
            picks = client.draft_picks(draft_id)
            traded_picks = client.draft_traded_picks(draft_id)

            write_raw(
                spark, CATALOG, BRONZE_SCHEMA, "sleeper_draft_raw",
                [draft_detail], season=season, league_id=league_id,
                endpoint=f"/draft/{draft_id}", run_id=run_id,
            )
            write_raw(
                spark, CATALOG, BRONZE_SCHEMA, "sleeper_draft_picks_raw",
                picks, season=season, league_id=league_id,
                endpoint=f"/draft/{draft_id}/picks", run_id=run_id,
            )
            write_raw(
                spark, CATALOG, BRONZE_SCHEMA,
                "sleeper_draft_traded_picks_raw",
                traded_picks, season=season, league_id=league_id,
                endpoint=f"/draft/{draft_id}/traded_picks",
                run_id=run_id,
            )


if __name__ == "__main__":
    main()
