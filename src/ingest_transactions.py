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
        seen = set()

        for week in range(0, MAX_WEEK + 1):
            transactions = client.transactions(league_id, week)

            # Preserve the complete API response in Bronze. We also
            # deduplicate identical transaction IDs within this run because
            # Sleeper can expose a transaction through more than one weekly
            # endpoint.
            unique = []
            for transaction in transactions:
                transaction_id = transaction.get("transaction_id")
                if transaction_id in seen:
                    continue
                seen.add(transaction_id)
                unique.append(transaction)

            write_raw(
                spark,
                CATALOG,
                BRONZE_SCHEMA,
                "sleeper_transactions_raw",
                unique,
                season=season,
                league_id=league_id,
                endpoint=f"/league/{league_id}/transactions/{week}",
                run_id=run_id,
            )


if __name__ == "__main__":
    main()
