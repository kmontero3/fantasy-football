"""Build the rebuildable Silver layer from Sleeper Bronze tables.

Silver is intentionally derived only from Bronze.  The job overwrites the
pipeline-owned Silver tables on every run so the layer is deterministic and
rebuildable.

Important modeling rules:
- Bronze remains raw and league-wide.
- Phinasty League is the only league admitted to Silver.
- kmontero3 is NOT used to filter Silver facts.
- Failed/cancelled transactions are retained in fact_transaction but do not
  create effective asset movements.
- Player/pick ownership is reconstructed from completed transaction events,
  draft picks, and the end-of-season roster snapshot.
"""

import re
from functools import reduce

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from config.league_config import (
    CATALOG,
    BRONZE_SCHEMA,
    SILVER_SCHEMA,
    SEASONS,
    TARGET_LEAGUE_NAME,
)


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def full_table(name):
    return f"{CATALOG}.{SILVER_SCHEMA}.{name}"


def bronze_table(name):
    return f"{CATALOG}.{BRONZE_SCHEMA}.{name}"


def read_bronze(spark, name):
    return spark.table(bronze_table(name))


def overwrite(df, table_name):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table(table_name))
    )
    print(f"Wrote {table_name}: {df.count():,} rows")


def parse_endpoint_week(endpoint_col):
    return F.regexp_extract(endpoint_col, r"/matchups/(\d+)$", 1).cast("int")


def parse_transaction_week(endpoint_col):
    return F.regexp_extract(endpoint_col, r"/transactions/(\d+)$", 1).cast("int")


def parse_stats_week(endpoint_col):
    return F.regexp_extract(endpoint_col, r"/regular/\d+/(\d+)$", 1).cast("int")


def epoch_ms_to_timestamp(column):
    return F.to_timestamp(F.from_unixtime((F.col(column) / F.lit(1000)).cast("double")))


def map_to_json(column):
    return F.to_json(F.col(column))


# ---------------------------------------------------------------------------
# Dimensions
# ---------------------------------------------------------------------------


def build_dim_league(spark):
    schema = T.StructType([
        T.StructField("name", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("sport", T.StringType()),
        T.StructField("season", T.StringType()),
        T.StructField("season_type", T.StringType()),
        T.StructField("league_id", T.StringType()),
        T.StructField("previous_league_id", T.StringType()),
        T.StructField("draft_id", T.StringType()),
        T.StructField("total_rosters", T.IntegerType()),
        T.StructField("roster_positions", T.ArrayType(T.StringType())),
        T.StructField("settings", T.MapType(T.StringType(), T.DoubleType())),
        T.StructField("scoring_settings", T.MapType(T.StringType(), T.DoubleType())),
        T.StructField("metadata", T.MapType(T.StringType(), T.StringType())),
    ])

    raw = read_bronze(spark, "sleeper_league_raw")
    parsed = (
        raw
        .withColumn("r", F.from_json("record_json", schema))
        .filter(F.col("r.name") == F.lit(TARGET_LEAGUE_NAME))
        .filter(F.col("season").isin(SEASONS))
    )

    out = parsed.select(
        F.col("r.league_id").alias("league_id"),
        F.col("season").cast("int").alias("season"),
        F.col("r.name").alias("league_name"),
        F.col("r.status").alias("status"),
        F.col("r.sport").alias("sport"),
        F.col("r.season_type").alias("season_type"),
        F.col("r.previous_league_id").alias("previous_league_id"),
        F.col("r.draft_id").alias("draft_id"),
        F.col("r.total_rosters").alias("num_rosters"),
        F.col("r.roster_positions").alias("roster_positions"),
        map_to_json("r.settings").alias("settings_json"),
        map_to_json("r.scoring_settings").alias("scoring_settings_json"),
        map_to_json("r.metadata").alias("metadata_json"),
        F.col("run_id").alias("raw_run_id"),
        F.col("retrieved_at").cast("timestamp").alias("source_retrieved_at"),
    )

    # There may be multiple Bronze snapshots. Keep the latest raw record per
    # league/season so Silver represents the latest authoritative payload.
    w = Window.partitionBy("league_id").orderBy(F.col("source_retrieved_at").desc())
    return out.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


def build_dim_season(spark):
    # Season dates are represented as canonical NFL season labels. Exact
    # regular-season boundaries are derived from league settings downstream.
    rows = [(int(s),) for s in SEASONS]
    return spark.createDataFrame(rows, ["season"])


def build_dim_user(spark):
    schema = T.StructType([
        T.StructField("user_id", T.StringType()),
        T.StructField("display_name", T.StringType()),
        T.StructField("username", T.StringType()),
        T.StructField("avatar", T.StringType()),
        T.StructField("metadata", T.MapType(T.StringType(), T.StringType())),
        T.StructField("is_bot", T.BooleanType()),
        T.StructField("is_owner", T.BooleanType()),
    ])

    raw = read_bronze(spark, "sleeper_users_raw")
    parsed = raw.withColumn("r", F.from_json("record_json", schema))
    out = parsed.select(
        F.col("r.user_id").alias("user_id"),
        F.col("r.username").alias("username"),
        F.col("r.display_name").alias("display_name"),
        F.col("r.avatar").alias("avatar"),
        map_to_json("r.metadata").alias("metadata_json"),
        F.col("r.is_bot").alias("is_bot"),
        F.col("r.is_owner").alias("is_owner"),
        F.col("run_id").alias("raw_run_id"),
        F.col("retrieved_at").cast("timestamp").alias("source_retrieved_at"),
    ).filter(F.col("user_id").isNotNull())

    w = Window.partitionBy("user_id").orderBy(F.col("source_retrieved_at").desc())
    return out.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


def build_dim_roster(spark):
    schema = T.StructType([
        T.StructField("league_id", T.StringType()),
        T.StructField("roster_id", T.IntegerType()),
        T.StructField("owner_id", T.StringType()),
        T.StructField("co_owners", T.ArrayType(T.StringType())),
        T.StructField("players", T.ArrayType(T.StringType())),
        T.StructField("starters", T.ArrayType(T.StringType())),
        T.StructField("taxi", T.ArrayType(T.StringType())),
        T.StructField("reserve", T.ArrayType(T.StringType())),
        T.StructField("settings", T.MapType(T.StringType(), T.DoubleType())),
        T.StructField("metadata", T.MapType(T.StringType(), T.StringType())),
        T.StructField("keepers", T.ArrayType(T.StringType())),
    ])

    raw = read_bronze(spark, "sleeper_rosters_raw")
    parsed = raw.withColumn("r", F.from_json("record_json", schema))
    out = parsed.select(
        F.col("season").cast("int").alias("season"),
        F.col("r.league_id").alias("league_id"),
        F.col("r.roster_id").alias("roster_id"),
        F.col("r.owner_id").alias("owner_user_id"),
        F.col("r.co_owners").alias("co_owner_user_ids"),
        F.col("r.players").alias("players"),
        F.col("r.starters").alias("starters"),
        F.col("r.taxi").alias("taxi"),
        F.col("r.reserve").alias("reserve"),
        F.col("r.keepers").alias("keepers"),
        map_to_json("r.settings").alias("settings_json"),
        map_to_json("r.metadata").alias("metadata_json"),
        F.col("run_id").alias("raw_run_id"),
        F.col("retrieved_at").cast("timestamp").alias("source_retrieved_at"),
    )
    w = Window.partitionBy("league_id", "roster_id").orderBy(F.col("source_retrieved_at").desc())
    return out.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


def build_dim_player(spark):
    # Sleeper's player payload is large and evolves. Keep a typed canonical
    # subset plus the complete raw JSON for fields not currently modeled.
    schema = T.StructType([
        T.StructField("player_id", T.StringType()),
        T.StructField("player", T.StructType([
            T.StructField("gsis_id", T.StringType()),
            T.StructField("full_name", T.StringType()),
            T.StructField("first_name", T.StringType()),
            T.StructField("last_name", T.StringType()),
            T.StructField("position", T.StringType()),
            T.StructField("team", T.StringType()),
            T.StructField("number", T.StringType()),
            T.StructField("status", T.StringType()),
            T.StructField("active", T.BooleanType()),
            T.StructField("years_exp", T.IntegerType()),
            T.StructField("birth_date", T.StringType()),
            T.StructField("height", T.StringType()),
            T.StructField("weight", T.StringType()),
            T.StructField("college", T.StringType()),
            T.StructField("espn_id", T.StringType()),
            T.StructField("yahoo_id", T.StringType()),
            T.StructField("fantasy_data_id", T.StringType()),
            T.StructField("rotowire_id", T.StringType()),
            T.StructField("sportradar_id", T.StringType()),
            T.StructField("stats_id", T.StringType()),
            T.StructField("rotoworld_id", T.StringType()),
            T.StructField("oddsjam_id", T.StringType()),
            T.StructField("metadata", T.MapType(T.StringType(), T.StringType())),
        ])),
    ])

    raw = read_bronze(spark, "sleeper_players_raw")
    parsed = raw.withColumn("r", F.from_json("record_json", schema))
    out = parsed.select(
        F.coalesce(F.col("r.player_id"), F.regexp_extract("record_json", r'"player_id"\s*:\s*"([^"]+)"', 1)).alias("player_id"),
        F.col("r.player.gsis_id").alias("gsis_id"),
        F.col("r.player.full_name").alias("full_name"),
        F.col("r.player.first_name").alias("first_name"),
        F.col("r.player.last_name").alias("last_name"),
        F.col("r.player.position").alias("position"),
        F.col("r.player.team").alias("team"),
        F.col("r.player.number").alias("number"),
        F.col("r.player.status").alias("status"),
        F.col("r.player.active").alias("active"),
        F.col("r.player.years_exp").alias("years_exp"),
        F.col("r.player.birth_date").alias("birth_date"),
        F.col("r.player.height").alias("height"),
        F.col("r.player.weight").alias("weight"),
        F.col("r.player.college").alias("college"),
        F.col("r.player.metadata").getItem("rookie_year").alias("rookie_year"),
        F.col("r.player.espn_id").alias("espn_id"),
        F.col("r.player.yahoo_id").alias("yahoo_id"),
        F.col("r.player.fantasy_data_id").alias("fantasy_data_id"),
        F.col("r.player.rotowire_id").alias("rotowire_id"),
        F.col("r.player.sportradar_id").alias("sportradar_id"),
        F.col("r.player.stats_id").alias("stats_id"),
        F.col("r.player.rotoworld_id").alias("rotoworld_id"),
        F.col("r.player.oddsjam_id").alias("oddsjam_id"),
        "record_json",
        "run_id",
        F.col("retrieved_at").cast("timestamp").alias("source_retrieved_at"),
    ).filter(F.col("player_id").isNotNull())

    w = Window.partitionBy("player_id").orderBy(F.col("source_retrieved_at").desc())
    return out.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


def build_dim_scoring_settings(spark):
    schema = T.StructType([
        T.StructField("league_id", T.StringType()),
        T.StructField("season", T.StringType()),
        T.StructField("scoring_settings", T.MapType(T.StringType(), T.DoubleType())),
    ])
    raw = read_bronze(spark, "sleeper_league_raw")
    parsed = (
        raw.withColumn("r", F.from_json("record_json", schema))
        .filter(F.col("season").isin(SEASONS))
        .filter(F.get_json_object("record_json", "$.name") == TARGET_LEAGUE_NAME)
    )
    return (
        parsed
        .select(
            F.col("r.league_id").alias("league_id"),
            F.col("r.season").cast("int").alias("season"),
            F.explode_outer(F.map_entries("r.scoring_settings")).alias("rule"),
            F.col("run_id").alias("raw_run_id"),
            F.col("retrieved_at").cast("timestamp").alias("source_retrieved_at"),
        )
        .select(
            "league_id", "season",
            F.col("rule.key").alias("scoring_rule"),
            F.col("rule.value").cast("double").alias("scoring_value"),
            "raw_run_id", "source_retrieved_at",
        )
        .dropDuplicates(["league_id", "season", "scoring_rule"])
    )


# ---------------------------------------------------------------------------
# Transactions and assets
# ---------------------------------------------------------------------------


def build_fact_transaction(spark):
    schema = T.StructType([
        T.StructField("transaction_id", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("type", T.StringType()),
        T.StructField("created", T.LongType()),
        T.StructField("status_updated", T.LongType()),
        T.StructField("creator", T.StringType()),
        T.StructField("roster_ids", T.ArrayType(T.IntegerType())),
        T.StructField("consenter_ids", T.ArrayType(T.IntegerType())),
        T.StructField("leg", T.IntegerType()),
        T.StructField("draft_id", T.StringType()),
        T.StructField("waiver_budget", T.ArrayType(T.MapType(T.StringType(), T.DoubleType()))),
        T.StructField("metadata", T.MapType(T.StringType(), T.StringType())),
        T.StructField("settings", T.MapType(T.StringType(), T.DoubleType())),
        T.StructField("adds", T.MapType(T.StringType(), T.IntegerType())),
        T.StructField("drops", T.MapType(T.StringType(), T.IntegerType())),
        T.StructField("draft_picks", T.ArrayType(T.StructType([
            T.StructField("round", T.IntegerType()),
            T.StructField("season", T.StringType()),
            T.StructField("league_id", T.StringType()),
            T.StructField("roster_id", T.IntegerType()),
            T.StructField("owner_id", T.IntegerType()),
            T.StructField("previous_owner_id", T.IntegerType()),
        ]))),
    ])
    raw = read_bronze(spark, "sleeper_transactions_raw")
    parsed = raw.withColumn("r", F.from_json("record_json", schema))
    out = parsed.select(
        F.col("r.transaction_id").alias("transaction_id"),
        F.col("league_id").alias("league_id"),
        F.col("season").cast("int").alias("season"),
        parse_transaction_week(F.col("endpoint")).alias("week"),
        epoch_ms_to_timestamp("r.created").alias("transaction_timestamp"),
        epoch_ms_to_timestamp("r.status_updated").alias("status_updated_timestamp"),
        F.col("r.type").alias("transaction_type"),
        F.col("r.status").alias("status"),
        F.col("r.creator").alias("creator_user_id"),
        F.col("r.roster_ids").alias("roster_ids"),
        F.col("r.consenter_ids").alias("consenter_ids"),
        F.col("r.leg").alias("leg"),
        F.col("r.draft_id").alias("draft_id"),
        map_to_json("r.waiver_budget").alias("waiver_budget_json"),
        map_to_json("r.metadata").alias("metadata_json"),
        map_to_json("r.settings").alias("settings_json"),
        "record_json",
        "run_id",
        F.col("retrieved_at").cast("timestamp").alias("source_retrieved_at"),
    ).filter(F.col("transaction_id").isNotNull())

    # Transaction IDs can occur on multiple week endpoints and across Bronze
    # ingestion runs. Latest retrieved payload wins.
    w = Window.partitionBy("transaction_id").orderBy(F.col("source_retrieved_at").desc())
    return out.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


def build_fact_transaction_asset(spark, fact_transaction):
    raw = read_bronze(spark, "sleeper_transactions_raw")
    schema = T.StructType([
        T.StructField("transaction_id", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("type", T.StringType()),
        T.StructField("adds", T.MapType(T.StringType(), T.IntegerType())),
        T.StructField("drops", T.MapType(T.StringType(), T.IntegerType())),
        T.StructField("draft_picks", T.ArrayType(T.StructType([
            T.StructField("round", T.IntegerType()),
            T.StructField("season", T.StringType()),
            T.StructField("roster_id", T.IntegerType()),
            T.StructField("owner_id", T.IntegerType()),
            T.StructField("previous_owner_id", T.IntegerType()),
        ]))),
    ])
    p = raw.withColumn("r", F.from_json("record_json", schema)).select(
        F.col("r.*"),
        F.col("league_id").alias("league_id"),
        F.col("season").cast("int").alias("season"),
        parse_transaction_week(F.col("endpoint")).alias("week"),
        F.col("retrieved_at").cast("timestamp").alias("source_retrieved_at"),
    )
    w = Window.partitionBy("transaction_id").orderBy(F.col("source_retrieved_at").desc())
    p = p.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

    player_adds = (
        p.select(
            "transaction_id", "league_id", "season", "week", "status", "type",
            F.explode_outer(F.map_entries("adds")).alias("x")
        )
        .filter(F.col("x.key").isNotNull())
        .select(
            "transaction_id", "league_id", "season", "week", "status", "type",
            F.col("x.value").alias("roster_id"),
            F.lit("player").alias("asset_type"),
            F.lit("in").alias("direction"),
            F.col("x.key").alias("player_id"),
            F.lit(None).cast("string").alias("pick_id"),
            F.lit(None).cast("int").alias("pick_season"),
            F.lit(None).cast("int").alias("pick_round"),
            F.lit(None).cast("int").alias("pick_original_roster_id"),
        )
    )

    player_drops = (
        p.select(
            "transaction_id", "league_id", "season", "week", "status", "type",
            F.explode_outer(F.map_entries("drops")).alias("x")
        )
        .filter(F.col("x.key").isNotNull())
        .select(
            "transaction_id", "league_id", "season", "week", "status", "type",
            F.col("x.value").alias("roster_id"),
            F.lit("player").alias("asset_type"),
            F.lit("out").alias("direction"),
            F.col("x.key").alias("player_id"),
            F.lit(None).cast("string").alias("pick_id"),
            F.lit(None).cast("int").alias("pick_season"),
            F.lit(None).cast("int").alias("pick_round"),
            F.lit(None).cast("int").alias("pick_original_roster_id"),
        )
    )

    # draft_picks contains original roster + current/previous owners. Treat
    # previous_owner -> owner as the effective asset transfer.
    pick_events = (
        p.select(
            "transaction_id", "league_id", "season", "week", "status", "type",
            F.explode_outer("draft_picks").alias("pick")
        )
        .filter(F.col("pick").isNotNull())
        .withColumn(
            "pick_id",
            F.concat(
                F.col("pick.season"), F.lit("_R"), F.col("pick.round"),
                F.lit("_ORIGINAL_"), F.col("pick.roster_id")
            )
        )
    )

    pick_out = pick_events.filter(F.col("pick.previous_owner_id").isNotNull()).select(
        "transaction_id", "league_id", "season", "week", "status", "type",
        F.col("pick.previous_owner_id").alias("roster_id"),
        F.lit("pick").alias("asset_type"), F.lit("out").alias("direction"),
        F.lit(None).cast("string").alias("player_id"),
        "pick_id",
        F.col("pick.season").cast("int").alias("pick_season"),
        F.col("pick.round").cast("int").alias("pick_round"),
        F.col("pick.roster_id").alias("pick_original_roster_id"),
    )
    pick_in = pick_events.filter(F.col("pick.owner_id").isNotNull()).select(
        "transaction_id", "league_id", "season", "week", "status", "type",
        F.col("pick.owner_id").alias("roster_id"),
        F.lit("pick").alias("asset_type"), F.lit("in").alias("direction"),
        F.lit(None).cast("string").alias("player_id"),
        "pick_id",
        F.col("pick.season").cast("int").alias("pick_season"),
        F.col("pick.round").cast("int").alias("pick_round"),
        F.col("pick.roster_id").alias("pick_original_roster_id"),
    )

    all_assets = player_adds.unionByName(player_drops).unionByName(pick_out).unionByName(pick_in)
    return (
        all_assets
        .withColumn("is_effective", F.col("status") == F.lit("complete"))
        .select(
            "transaction_id", "league_id", "season", "week", "roster_id",
            "asset_type", "direction", "player_id", "pick_id", "pick_season",
            "pick_round", "pick_original_roster_id", "is_effective"
        )
        .dropDuplicates()
    )


# ---------------------------------------------------------------------------
# Drafts
# ---------------------------------------------------------------------------


def build_dim_draft(spark):
    schema = T.StructType([
        T.StructField("draft_id", T.StringType()),
        T.StructField("league_id", T.StringType()),
        T.StructField("type", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("start_time", T.LongType()),
        T.StructField("last_picked", T.IntegerType()),
        T.StructField("slot_to_roster_id", T.MapType(T.StringType(), T.IntegerType())),
        T.StructField("settings", T.MapType(T.StringType(), T.DoubleType())),
    ])
    raw = read_bronze(spark, "sleeper_draft_raw")
    parsed = raw.withColumn("r", F.from_json("record_json", schema))
    return parsed.select(
        "season",
        F.col("r.draft_id").alias("draft_id"),
        F.col("r.league_id").alias("league_id"),
        F.col("r.type").alias("draft_type"),
        F.col("r.status").alias("status"),
        epoch_ms_to_timestamp("r.start_time").alias("start_time"),
        F.col("r.last_picked").alias("last_picked"),
        map_to_json("r.slot_to_roster_id").alias("slot_to_roster_id_json"),
        map_to_json("r.settings").alias("settings_json"),
        "run_id", F.col("retrieved_at").cast("timestamp").alias("source_retrieved_at"),
    ).dropDuplicates(["draft_id"])


def build_fact_draft_pick(spark):
    schema = T.StructType([
        T.StructField("draft_id", T.StringType()),
        T.StructField("pick_no", T.IntegerType()),
        T.StructField("round", T.IntegerType()),
        T.StructField("draft_slot", T.IntegerType()),
        T.StructField("roster_id", T.IntegerType()),
        T.StructField("picked_by", T.StringType()),
        T.StructField("player_id", T.StringType()),
        T.StructField("is_keeper", T.BooleanType()),
        T.StructField("metadata", T.MapType(T.StringType(), T.StringType())),
    ])
    raw = read_bronze(spark, "sleeper_draft_picks_raw")
    parsed = raw.withColumn("r", F.from_json("record_json", schema))
    return parsed.select(
        F.col("r.draft_id").alias("draft_id"),
        "league_id",
        F.col("season").cast("int").alias("season"),
        F.col("r.pick_no").alias("pick_number"),
        F.col("r.round").alias("round"),
        F.col("r.draft_slot").alias("draft_slot"),
        F.col("r.roster_id").alias("roster_id"),
        F.col("r.picked_by").alias("picked_by_user_id"),
        F.col("r.player_id").alias("player_id"),
        F.col("r.is_keeper").alias("is_keeper"),
        map_to_json("r.metadata").alias("metadata_json"),
        "run_id", F.col("retrieved_at").cast("timestamp").alias("source_retrieved_at"),
    ).filter(F.col("player_id").isNotNull()).dropDuplicates(["draft_id", "pick_number"])


def build_fact_draft_pick_ownership(spark, draft_picks, transaction_assets, fact_transaction):
    traded_schema = T.StructType([
        T.StructField("round", T.IntegerType()),
        T.StructField("season", T.StringType()),
        T.StructField("draft_id", T.StringType()),
        T.StructField("roster_id", T.IntegerType()),
        T.StructField("owner_id", T.IntegerType()),
        T.StructField("previous_owner_id", T.IntegerType()),
    ])
    raw = read_bronze(spark, "sleeper_draft_traded_picks_raw")
    traded = (
        raw.withColumn("r", F.from_json("record_json", traded_schema))
        .select(
            "league_id", F.col("season").cast("int").alias("season"),
            F.col("r.round").alias("pick_round"),
            F.col("r.season").cast("int").alias("pick_season"),
            F.col("r.draft_id").alias("draft_id"),
            F.col("r.roster_id").alias("original_roster_id"),
            F.col("r.owner_id").alias("current_owner_roster_id"),
            F.col("r.previous_owner_id").alias("previous_owner_roster_id"),
            F.lit("draft_traded_picks").alias("ownership_source"),
            F.lit(None).cast("string").alias("source_transaction_id"),
            F.lit(None).cast("timestamp").alias("effective_timestamp"),
            F.lit("transfer_state").alias("ownership_event_type"),
        )
    )

    # Current/last-known league traded-pick state is retained as a state row.
    league_schema = T.StructType([
        T.StructField("round", T.IntegerType()),
        T.StructField("season", T.StringType()),
        T.StructField("roster_id", T.IntegerType()),
        T.StructField("owner_id", T.IntegerType()),
        T.StructField("previous_owner_id", T.IntegerType()),
    ])
    league_raw = read_bronze(spark, "sleeper_traded_picks_raw")
    league_traded = (
        league_raw.withColumn("r", F.from_json("record_json", league_schema))
        .select(
            "league_id", F.col("season").cast("int").alias("season"),
            F.col("r.round").alias("pick_round"),
            F.col("r.season").cast("int").alias("pick_season"),
            F.lit(None).cast("string").alias("draft_id"),
            F.col("r.roster_id").alias("original_roster_id"),
            F.col("r.owner_id").alias("current_owner_roster_id"),
            F.col("r.previous_owner_id").alias("previous_owner_roster_id"),
            F.lit("league_traded_picks").alias("ownership_source"),
            F.lit(None).cast("string").alias("source_transaction_id"),
            F.lit(None).cast("timestamp").alias("effective_timestamp"),
            F.lit("transfer_state").alias("ownership_event_type"),
        )
    )

    # Transaction asset events are the best chronological transfer evidence.
    events = (
        transaction_assets
        .filter((F.col("asset_type") == "pick") & F.col("is_effective"))
        .join(
            fact_transaction.select("transaction_id", "transaction_timestamp"),
            "transaction_id",
            "left",
        )
        .select(
            "league_id",
            "season",
            "pick_round",
            "pick_season",
            F.col("pick_original_roster_id").alias("original_roster_id"),
            F.col("roster_id").alias("current_owner_roster_id"),
            F.lit(None).cast("int").alias("previous_owner_roster_id"),
            F.lit(None).cast("string").alias("draft_id"),
            F.lit("transaction").alias("ownership_source"),
            F.col("transaction_id").alias("source_transaction_id"),
            F.col("transaction_timestamp").alias("effective_timestamp"),
            F.when(
                F.col("direction") == "in",
                "acquisition"
            ).otherwise("disposition").alias("ownership_event_type"),
        )
    )

    return (
        traded.unionByName(league_traded).unionByName(events)
        .withColumn(
            "pick_id",
            F.concat(
                F.col("pick_season"), F.lit("_R"), F.col("pick_round"),
                F.lit("_ORIGINAL_"), F.col("original_roster_id")
            ),
        )
        .select(
            "pick_id", "league_id", "season", "pick_season", "pick_round",
            "original_roster_id", "current_owner_roster_id",
            "previous_owner_roster_id", "draft_id", "effective_timestamp",
            "ownership_event_type", "ownership_source", "source_transaction_id",
        )
        .dropDuplicates()
    )


# ---------------------------------------------------------------------------
# Matchups / roster weekly
# ---------------------------------------------------------------------------


def build_fact_matchup(spark):
    schema = T.StructType([
        T.StructField("points", T.DoubleType()),
        T.StructField("players", T.ArrayType(T.StringType())),
        T.StructField("roster_id", T.IntegerType()),
        T.StructField("matchup_id", T.IntegerType()),
        T.StructField("starters", T.ArrayType(T.StringType())),
        T.StructField("starters_points", T.ArrayType(T.DoubleType())),
        T.StructField("players_points", T.MapType(T.StringType(), T.DoubleType())),
        T.StructField("custom_points", T.DoubleType()),
    ])
    raw = read_bronze(spark, "sleeper_matchups_raw")
    parsed = raw.withColumn("r", F.from_json("record_json", schema))
    base = parsed.select(
        F.col("season").cast("int").alias("season"),
        "league_id",
        parse_endpoint_week(F.col("endpoint")).alias("week"),
        F.col("r.*"),
    ).filter(F.col("roster_id").isNotNull())

    left = base.alias("a")
    right = base.alias("b")
    joined = (
        left.join(
            right,
            (F.col("a.league_id") == F.col("b.league_id"))
            & (F.col("a.season") == F.col("b.season"))
            & (F.col("a.week") == F.col("b.week"))
            & (F.col("a.matchup_id") == F.col("b.matchup_id"))
            & (F.col("a.roster_id") != F.col("b.roster_id")),
            "left",
        )
        .select(
            F.col("a.league_id").alias("league_id"),
            F.col("a.season").alias("season"),
            F.col("a.week").alias("week"),
            F.col("a.matchup_id").alias("matchup_id"),
            F.col("a.roster_id").alias("roster_id"),
            F.col("b.roster_id").alias("opponent_roster_id"),
            F.col("a.points").alias("points"),
            F.col("b.points").alias("opponent_points"),
            F.when(F.col("a.points") > F.col("b.points"), 1).otherwise(0).alias("win_flag"),
            F.when(F.col("a.points") < F.col("b.points"), 1).otherwise(0).alias("loss_flag"),
            F.when(F.col("a.points") == F.col("b.points"), 1).otherwise(0).alias("tie_flag"),
            F.col("a.players").alias("players"),
            F.col("a.starters").alias("starters"),
            F.col("a.starters_points").alias("starters_points"),
            F.col("a.players_points").alias("players_points"),
        )
    )
    return joined.dropDuplicates(["league_id", "season", "week", "roster_id"])


def build_fact_team_week(fact_matchup):
    return fact_matchup.select(
        "league_id", "season", "week", "roster_id",
        "points",
        F.expr("aggregate(starters_points, cast(0D as double), (acc, x) -> acc + coalesce(x, 0D))").alias("starter_points"),
        (
            F.col("points")
            - F.expr("aggregate(starters_points, cast(0D as double), (acc, x) -> acc + coalesce(x, 0D))")
        ).alias("bench_points"),
        "opponent_roster_id", "opponent_points",
        "win_flag", "loss_flag", "tie_flag",
    )


def build_fact_roster_weekly(fact_matchup):
    # Sleeper's historical roster endpoint is a current-state snapshot, not a
    # weekly history. The matchup payload is therefore the authoritative
    # weekly player/starting-lineup snapshot available in Bronze.
    starters = fact_matchup.select(
        "league_id", "season", "week", "roster_id",
        F.posexplode_outer("starters").alias("slot_index", "player_id"),
    ).withColumn("roster_slot", F.lit("starter")).withColumn("is_starter", F.lit(True))

    all_players = fact_matchup.select(
        "league_id", "season", "week", "roster_id",
        F.explode_outer("players").alias("player_id"),
    )
    bench = (
        all_players.join(
            starters.select("league_id", "season", "week", "roster_id", "player_id"),
            ["league_id", "season", "week", "roster_id", "player_id"],
            "left_anti",
        )
        .withColumn("slot_index", F.lit(None).cast("int"))
        .withColumn("roster_slot", F.lit("bench"))
        .withColumn("is_starter", F.lit(False))
    )

    return (
        starters.unionByName(bench)
        .withColumn("is_bench", ~F.col("is_starter"))
        .withColumn("is_taxi", F.lit(False))
        .withColumn("is_reserve", F.lit(False))
        .select(
            "league_id", "season", "week", "roster_id", "player_id",
            "slot_index", "roster_slot", "is_starter", "is_bench", "is_taxi", "is_reserve",
        )
        .dropDuplicates(["league_id", "season", "week", "roster_id", "player_id"])
    )


# ---------------------------------------------------------------------------
# Player statistics and projections
# ---------------------------------------------------------------------------


def build_fact_player_weekly(spark, scoring_settings):
    schema = T.StructType([
        T.StructField("player_id", T.StringType()),
        T.StructField("stats", T.MapType(T.StringType(), T.DoubleType())),
    ])
    raw = read_bronze(spark, "sleeper_stats_weekly_raw")
    base = raw.select(
        F.col("season").cast("int").alias("season"),
        parse_stats_week(F.col("endpoint")).alias("week"),
        F.from_json("record_json", schema).alias("r"),
        "run_id", F.col("retrieved_at").cast("timestamp").alias("source_retrieved_at"),
    ).select(
        "season", "week", F.col("r.player_id").alias("player_id"),
        F.col("r.stats").alias("stats"), "run_id", "source_retrieved_at",
    ).filter(F.col("player_id").isNotNull())

    # Scoring is stored as a map by league/season. Since this league is the
    # only Silver league, join by season. The calculation remains in Silver;
    # Gold can use the resulting fantasy_points without reimplementing rules.
    score_map = (
        scoring_settings.groupBy("season")
        .agg(F.map_from_entries(F.collect_list(F.struct("scoring_rule", "scoring_value"))).alias("scoring"))
    )
    out = base.join(score_map, "season", "left")
    out = out.withColumn(
        "fantasy_points",
        F.expr(
            "aggregate(map_entries(stats), cast(0D as double), "
            "(acc, x) -> acc + coalesce(x.value * element_at(scoring, x.key), 0D))"
        ),
    )
    return out.select(
        "season", "week", "player_id", "stats",
        map_to_json("stats").alias("stats_json"),
        "fantasy_points", "run_id", "source_retrieved_at",
    ).dropDuplicates(["season", "week", "player_id"])


def build_fact_player_season(spark, fact_player_weekly):
    numeric_stats = (
        fact_player_weekly
        .select(
            "season",
            "player_id",
            F.explode_outer("stats").alias("stat_key", "stat_value")
        )
        .groupBy("season", "player_id", "stat_key")
        .agg(
            F.sum("stat_value").alias("value")
        )
    )

    stats_map = (
        numeric_stats
        .groupBy("season", "player_id")
        .agg(
            F.map_from_entries(
                F.collect_list(
                    F.struct(
                        F.col("stat_key"),
                        F.col("value")
                    )
                )
            ).alias("stats")
        )
    )

    points = (
        fact_player_weekly
        .groupBy("season", "player_id")
        .agg(
            F.sum("fantasy_points").alias("fantasy_points"),
            F.countDistinct("week").alias("weeks_with_stats"),
        )
    )

    return stats_map.join(
        points,
        ["season", "player_id"],
        "full"
    )


def build_fact_player_projection(spark):
    schema = T.StructType([
        T.StructField("date", T.StringType()),
        T.StructField("stats", T.MapType(T.StringType(), T.DoubleType())),
        T.StructField("category", T.StringType()),
        T.StructField("week", T.IntegerType()),
        T.StructField("sport", T.StringType()),
        T.StructField("season_type", T.StringType()),
        T.StructField("season", T.StringType()),
        T.StructField("team", T.StringType()),
        T.StructField("player_id", T.StringType()),
        T.StructField("opponent", T.StringType()),
        T.StructField("game_id", T.StringType()),
        T.StructField("company", T.StringType()),
    ])
    raw = read_bronze(spark, "sleeper_projections_raw")
    parsed = raw.select(
        F.col("season").cast("int").alias("ingest_season"),
        "run_id", F.col("retrieved_at").cast("timestamp").alias("source_retrieved_at"),
        F.from_json("record_json", schema).alias("r"),
    )
    out = parsed.select(
        F.col("r.player_id").alias("player_id"),
        F.coalesce(F.col("r.season").cast("int"), F.col("ingest_season")).alias("season"),
        F.col("r.week").alias("week"),
        F.col("r.season_type").alias("season_type"),
        F.col("r.category").alias("category"),
        F.col("r.company").alias("company"),
        F.col("r.date").alias("projection_date"),
        F.col("r.team").alias("team"),
        F.col("r.opponent").alias("opponent"),
        F.col("r.game_id").alias("game_id"),
        F.col("r.stats").alias("stats"),
        map_to_json("r.stats").alias("stats_json"),
        "run_id", "source_retrieved_at",
    ).filter(F.col("player_id").isNotNull())

    w = Window.partitionBy("player_id", "season", "week", "company", "category").orderBy(F.col("source_retrieved_at").desc())
    return out.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


def build_fact_player_projection_snapshot(spark):
    schema = T.StructType([
        T.StructField("date", T.StringType()),
        T.StructField("stats", T.MapType(T.StringType(), T.DoubleType())),
        T.StructField("category", T.StringType()),
        T.StructField("week", T.IntegerType()),
        T.StructField("sport", T.StringType()),
        T.StructField("season_type", T.StringType()),
        T.StructField("season", T.StringType()),
        T.StructField("team", T.StringType()),
        T.StructField("player_id", T.StringType()),
        T.StructField("opponent", T.StringType()),
        T.StructField("game_id", T.StringType()),
        T.StructField("company", T.StringType()),
    ])
    raw = read_bronze(spark, "sleeper_projections_raw")
    return (
        raw.select(
            F.col("season").cast("int").alias("ingest_season"),
            "run_id", F.col("retrieved_at").cast("timestamp").alias("retrieved_at"),
            F.from_json("record_json", schema).alias("r"),
        )
        .select(
            F.col("r.player_id").alias("player_id"),
            F.coalesce(F.col("r.season").cast("int"), F.col("ingest_season")).alias("season"),
            F.col("r.week").alias("week"),
            F.col("r.season_type").alias("season_type"),
            F.col("r.category").alias("category"),
            F.col("r.company").alias("company"),
            F.col("r.date").alias("projection_date"),
            F.col("r.team").alias("team"),
            F.col("r.opponent").alias("opponent"),
            F.col("r.game_id").alias("game_id"),
            F.col("r.stats").alias("stats"),
            map_to_json("r.stats").alias("stats_json"),
            "run_id", "retrieved_at",
        )
        .filter(F.col("player_id").isNotNull())
    )


# ---------------------------------------------------------------------------
# Historical player ownership
# ---------------------------------------------------------------------------


def build_fact_roster_player(spark, dim_roster, transaction_assets, draft_picks, fact_transaction):
    """Reconstruct player ownership intervals.

    Completed player transaction events and draft selections are treated as
    acquisition/disposition events. For players appearing in the final roster
    snapshot with no observed acquisition event, a season-start seed is added.
    This captures inherited players while avoiding falsely resetting players
    that were acquired during the season.
    """
    player_events = (
        transaction_assets
        .filter((F.col("asset_type") == "player") & F.col("is_effective"))
        .join(
            fact_transaction.select("transaction_id", "transaction_timestamp"),
            "transaction_id",
            "left",
        )
        .select(
            "league_id", "season", "roster_id", "player_id",
            F.when(F.col("direction") == "in", "in").otherwise("out").alias("direction"),
            "transaction_id",
            F.lit(None).cast("string").alias("draft_id"),
            F.col("transaction_timestamp").alias("event_timestamp"),
            F.col("week").alias("event_week"),
            F.lit(False).alias("is_seed"),
        )
    )

    # Draft picks are player acquisition events. Draft start_time is not
    # repeated on picks, so use season-level draft metadata when available.
    draft_events = draft_picks.select(
        "league_id", "season", "roster_id", "player_id",
        F.lit("in").alias("direction"),
        F.concat(F.lit("DRAFT_"), F.col("draft_id"), F.lit("_"), F.col("pick_number")).alias("transaction_id"),
        F.col("draft_id"),
        F.lit(None).cast("timestamp").alias("event_timestamp"),
        F.lit(0).cast("int").alias("event_week"),
        F.lit(False).alias("is_seed"),
    )

    events = player_events.unionByName(draft_events)

    # Seed final-roster players only when there is no observed acquisition for
    # that roster/player. The roster endpoint is a current-state snapshot.
    final_players = (
        dim_roster
        .select("league_id", "season", "roster_id", F.explode_outer("players").alias("player_id"))
        .filter(F.col("player_id").isNotNull())
    )
    acquired = events.filter(F.col("direction") == "in").select(
        "league_id", "season", "roster_id", "player_id"
    ).dropDuplicates()
    seeds = (
        final_players.join(acquired, ["league_id", "season", "roster_id", "player_id"], "left_anti")
        .withColumn("direction", F.lit("in"))
        .withColumn("transaction_id", F.lit(None).cast("string"))
        .withColumn("draft_id", F.lit(None).cast("string"))
        .withColumn("event_timestamp", F.lit(None).cast("timestamp"))
        .withColumn("event_week", F.lit(0).cast("int"))
        .withColumn("is_seed", F.lit(True))
    )
    events = events.unionByName(seeds)

    # Event timestamp ordering: transactions have precise timestamps; draft
    # and seed events use deterministic ordering before normal transactions.
    events = events.withColumn(
        "event_order",
        F.when(F.col("event_timestamp").isNotNull(), F.col("event_timestamp"))
         .otherwise(F.to_timestamp(F.concat(F.col("season"), F.lit("-01-01")))),
    )

    # Pair each acquisition with the next disposition for the same asset.
    # The sequence number counts acquisitions, allowing a correlated next-out
    # selection with a window over the event stream.
    w = Window.partitionBy("league_id", "season", "roster_id", "player_id").orderBy(
        F.col("event_order"),
        F.when(F.col("direction") == "out", 0).otherwise(1),
        F.col("transaction_id").asc_nulls_last(),
    )
    ordered = events.withColumn("event_seq", F.row_number().over(w))

    # For a rebuildable and Spark-native interval model, use cumulative counts.
    # Each 'in' increments ownership; each 'out' decrements it. An out with no
    # prior in represents season-start ownership.
    state = ordered.withColumn(
        "prior_balance",
        F.sum(F.when(F.col("direction") == "in", 1).otherwise(-1)).over(
            w.rowsBetween(Window.unboundedPreceding, -1)
        ),
    ).withColumn(
        "next_out_timestamp",
        F.min(F.when(F.col("direction") == "out", F.col("event_order"))).over(
            w.rowsBetween(1, Window.unboundedFollowing)
        ),
    )

    acquisitions = state.filter(F.col("direction") == "in").select(
        "league_id", "season", "roster_id", "player_id",
        F.when(F.col("is_seed"), F.to_timestamp(F.concat(F.col("season"), F.lit("-01-01"))))
         .otherwise(F.col("event_order")).alias("acquired_at"),
        F.col("next_out_timestamp").alias("released_at"),
        F.col("transaction_id").alias("acquisition_transaction_id"),
        F.lit(None).cast("string").alias("release_transaction_id"),
        F.when(F.col("is_seed"), "season_start")
         .when(F.col("draft_id").isNotNull(), "draft")
         .otherwise("transaction").alias("acquisition_type"),
        F.when(F.col("next_out_timestamp").isNotNull(), "transaction").otherwise(None).alias("release_type"),
    )

    return (
        acquisitions
        .dropDuplicates(["league_id", "season", "roster_id", "player_id", "acquired_at", "released_at"])
        .withColumn("is_current", F.col("released_at").isNull())
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

    print("=== BUILDING SILVER ===")

    dim_league = build_dim_league(spark)
    overwrite(dim_league, "dim_league")

    dim_season = build_dim_season(spark)
    overwrite(dim_season, "dim_season")

    dim_user = build_dim_user(spark)
    overwrite(dim_user, "dim_user")

    dim_roster = build_dim_roster(spark)
    overwrite(dim_roster, "dim_roster")

    dim_player = build_dim_player(spark)
    overwrite(dim_player, "dim_player")

    dim_scoring = build_dim_scoring_settings(spark)
    overwrite(dim_scoring, "dim_scoring_settings")

    fact_transaction = build_fact_transaction(spark)
    overwrite(fact_transaction, "fact_transaction")

    fact_transaction_asset = build_fact_transaction_asset(spark, fact_transaction)
    overwrite(fact_transaction_asset, "fact_transaction_asset")

    dim_draft = build_dim_draft(spark)
    overwrite(dim_draft, "dim_draft")

    fact_draft_pick = build_fact_draft_pick(spark)
    overwrite(fact_draft_pick, "fact_draft_pick")

    fact_draft_pick_ownership = build_fact_draft_pick_ownership(
        spark, fact_draft_pick, fact_transaction_asset, fact_transaction
    )
    overwrite(fact_draft_pick_ownership, "fact_draft_pick_ownership")

    fact_matchup = build_fact_matchup(spark)
    overwrite(fact_matchup, "fact_matchup")

    fact_team_week = build_fact_team_week(fact_matchup)
    overwrite(fact_team_week, "fact_team_week")

    fact_roster_weekly = build_fact_roster_weekly(fact_matchup)
    overwrite(fact_roster_weekly, "fact_roster_weekly")

    fact_roster_player = build_fact_roster_player(
        spark, dim_roster, fact_transaction_asset, fact_draft_pick, fact_transaction
    )
    overwrite(fact_roster_player, "fact_roster_player")

    fact_player_weekly = build_fact_player_weekly(spark, dim_scoring)
    overwrite(fact_player_weekly, "fact_player_weekly")

    fact_player_season = build_fact_player_season(spark, fact_player_weekly)
    overwrite(fact_player_season, "fact_player_season")

    fact_player_projection = build_fact_player_projection(spark)
    overwrite(fact_player_projection, "fact_player_projection")

    fact_player_projection_snapshot = build_fact_player_projection_snapshot(spark)
    overwrite(fact_player_projection_snapshot, "fact_player_projection_snapshot")

    print("=== SILVER BUILD COMPLETE ===")


if __name__ == "__main__":
    main()
