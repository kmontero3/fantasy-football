from uuid import uuid4

from pyspark.sql import SparkSession

from config.league_config import CATALOG, BRONZE_SCHEMA
from client import SleeperClient
from bronze_writer import write_raw


def main():
    spark = SparkSession.builder.getOrCreate()
    client = SleeperClient()
    run_id = str(uuid4())

    write_raw(
        spark, CATALOG, BRONZE_SCHEMA, "sleeper_state_raw",
        [client.state()], endpoint="/state/nfl", run_id=run_id,
    )

    # Keep the full player response. Do not filter to active players in Bronze
    # because historical players may be required to resolve old transactions.
    players = client.players()
    records = [
        {"player_id": player_id, "player": player}
        for player_id, player in players.items()
    ]

    write_raw(
        spark, CATALOG, BRONZE_SCHEMA, "sleeper_players_raw",
        records, endpoint="/players/nfl", run_id=run_id,
    )


if __name__ == "__main__":
    main()
