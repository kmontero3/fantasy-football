"""
Build the rebuildable Gold layer for the Phinasty League.

Model version: v1.1-revised

Key modeling rules:
- Gold is derived from Silver.
- The valuation universe excludes players with no credible NFL relevance.
- Current-season production is actual points plus remaining projection, not
  actual points plus a full-season projection.
- Season-level projections are preferred to weekly projections when available.
- Future seasons carry forward the prior forecast baseline when projections
  are unavailable.
- Expected fantasy points are never negative.
- VOR is standardized within position before being used for cross-position
  dynasty valuation.
- Trade analysis contains completed trades only.
- Roster composition is one row per league/season/roster/player.
- Draft-pick valuation uses Sleeper's continuous overall pick_number and does
  not calibrate on the 2023 startup draft.
- NFL draft capital is intentionally a stub. It has no effect on the current
  valuation functionality and can be populated later.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from config.league_config import (
    CATALOG,
    SILVER_SCHEMA,
    GOLD_SCHEMA,
    SEASONS,
    SLEEPER_USERNAME,
)

MODEL_VERSION = "v1.1-revised"
TARGET_DISPLAY_NAME = SLEEPER_USERNAME
CURRENT_SEASON = max(SEASONS)
FORECAST_YEARS = 5

RELEVANT_POSITIONS = ["QB", "RB", "WR", "TE", "K", "DEF"]


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def st(name):
    return f"{CATALOG}.{SILVER_SCHEMA}.{name}"


def gt(name):
    return f"{CATALOG}.{GOLD_SCHEMA}.{name}"


def overwrite(df, name):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(gt(name))
    )
    print(f"Wrote gold.{name}: {df.count():,} rows")


def read(spark, name):
    return spark.table(st(name))


def add_model_metadata(df):
    return (
        df.withColumn("model_name", F.lit("fantasy_football_gold"))
        .withColumn("model_version", F.lit(MODEL_VERSION))
        .withColumn("calculated_at", F.current_timestamp())
    )


def stat_value(stats, key):
    return F.coalesce(
        F.element_at(F.col(stats), key),
        F.lit(0.0),
    )


def safe_age(birth_date, season):
    return F.when(
        F.col(birth_date).isNotNull(),
        F.floor(
            F.datediff(
                F.to_date(
                    F.concat(
                        F.col(season).cast("string"),
                        F.lit("-12-31"),
                    )
                ),
                F.to_date(F.col(birth_date)),
            )
            / F.lit(365.25)
        ),
    )


# ---------------------------------------------------------------------------
# NFL draft capital stub
# ---------------------------------------------------------------------------

def build_nfl_draft_capital_stub(spark):
    """
    Stub for future NFL draft-capital modeling.

    There is intentionally no NFL draft-round/pick data in the current Silver
    layer. Returning an empty table means this feature has zero effect on the
    current valuation model.

    Future schema:
        player_id
        nfl_draft_season
        nfl_draft_round
        nfl_draft_pick
        nfl_draft_capital_score
    """
    schema = T.StructType([
        T.StructField("player_id", T.StringType(), True),
        T.StructField("nfl_draft_season", T.IntegerType(), True),
        T.StructField("nfl_draft_round", T.IntegerType(), True),
        T.StructField("nfl_draft_pick", T.IntegerType(), True),
        T.StructField("nfl_draft_capital_score", T.DoubleType(), True),
    ])

    return spark.createDataFrame([], schema)


# ---------------------------------------------------------------------------
# League standings
# ---------------------------------------------------------------------------

def build_league_standings(spark):
    team = read(spark, "fact_team_week")

    roster = (
        read(spark, "dim_roster")
        .select(
            "league_id",
            "season",
            "roster_id",
            "owner_user_id",
        )
    )

    users = (
        read(spark, "dim_user")
        .select(
            "user_id",
            "display_name",
        )
    )

    league = (
        read(spark, "dim_league")
        .select(
            "league_id",
            "season",
            "settings_json",
        )
    )

    # Sleeper league settings contain playoff_week_start. If it is unavailable,
    # use all currently available matchup weeks as regular-season weeks.
    settings = (
        league
        .withColumn(
            "playoff_week_start",
            F.get_json_object(
                "settings_json",
                "$.playoff_week_start",
            ).cast("double").cast("int"),
        )
        .select(
            "league_id",
            "season",
            "playoff_week_start",
        )
    )

    team = (
        team
        .filter(F.col("points").isNotNull())
        .join(
            settings,
            ["league_id", "season"],
            "left",
        )
        .withColumn(
            "is_regular_season",
            F.when(
                F.col("playoff_week_start").isNotNull(),
                F.col("week") < F.col("playoff_week_start"),
            ).otherwise(F.lit(True)),
        )
        .filter(F.col("is_regular_season"))
    )

    # Only completed weeks represented in fact_team_week are counted.
    out = (
        team
        .groupBy(
            "league_id",
            "season",
            "roster_id",
        )
        .agg(
            F.sum("win_flag").alias("wins"),
            F.sum("loss_flag").alias("losses"),
            F.sum("tie_flag").alias("ties"),
            F.sum("points").alias("points_for"),
            F.sum("opponent_points").alias("points_against"),
            F.avg("points").alias("avg_points"),
            F.expr(
                "percentile_approx(points, 0.5)"
            ).alias("median_points"),
            F.max("points").alias("max_points"),
            F.min("points").alias("min_points"),
            F.countDistinct("week").alias("weeks_played"),
        )
        .join(
            roster,
            ["league_id", "season", "roster_id"],
            "left",
        )
        .join(
            users,
            F.col("owner_user_id") == F.col("user_id"),
            "left",
        )
        .drop("user_id")
        .withColumn(
            "games_played",
            F.col("wins") + F.col("losses") + F.col("ties"),
        )
        .withColumn(
            "win_pct",
            F.when(
                F.col("games_played") > 0,
                (
                    F.col("wins")
                    + F.col("ties") * F.lit(0.5)
                )
                / F.col("games_played"),
            ),
        )
    )

    rank_window = (
        Window
        .partitionBy("league_id", "season")
        .orderBy(
            F.col("wins").desc(),
            F.col("points_for").desc(),
            F.col("roster_id").asc(),
        )
    )

    return add_model_metadata(
        out.withColumn(
            "regular_season_rank",
            F.row_number().over(rank_window),
        )
    )


# ---------------------------------------------------------------------------
# Roster composition
# ---------------------------------------------------------------------------

def build_roster_composition(spark):
    rp = read(spark, "fact_roster_player")

    players = (
        read(spark, "dim_player")
        .select(
            "player_id",
            "full_name",
            "position",
            "team",
            "birth_date",
            "years_exp",
            "rookie_year",
            "active",
            "status",
        )
    )

    weekly = read(spark, "fact_roster_weekly")

    starts = (
        weekly
        .groupBy(
            "league_id",
            "season",
            "roster_id",
            "player_id",
        )
        .agg(
            F.sum(
                F.when(F.col("is_starter"), 1).otherwise(0)
            ).alias("starts"),
            F.countDistinct("week").alias(
                "weeks_in_lineup_data"
            ),
        )
    )

    # fact_roster_player is an ownership interval table, so multiple rows can
    # legitimately exist for a player/roster/season. Gold roster composition
    # is intentionally a season-level snapshot/grain.
    composition = (
        rp
        .groupBy(
            "league_id",
            "season",
            "roster_id",
            "player_id",
        )
        .agg(
            F.min("acquired_at").alias("first_acquired_at"),
            F.max("released_at").alias("last_released_at"),
            F.max(
                F.when(F.col("is_current"), 1).otherwise(0)
            ).alias("is_current"),
        )
    )

    out = (
        composition
        .join(players, "player_id", "left")
        .join(
            starts,
            [
                "league_id",
                "season",
                "roster_id",
                "player_id",
            ],
            "left",
        )
        .withColumn(
            "starts",
            F.coalesce(
                F.col("starts"),
                F.lit(0),
            ),
        )
        .withColumn(
            "weeks_in_lineup_data",
            F.coalesce(
                F.col("weeks_in_lineup_data"),
                F.lit(0),
            ),
        )
        .withColumn(
            "age_years",
            safe_age("birth_date", "season"),
        )
    )

    return add_model_metadata(out)


# ---------------------------------------------------------------------------
# Projection consensus
# ---------------------------------------------------------------------------

def build_projection_consensus(spark):
    p = read(spark, "fact_player_projection")
    scoring = (
        read(spark, "dim_scoring_settings")
        .filter(
            F.col("league_id").isin(
                [
                    r["league_id"]
                    for r in (
                        read(spark, "dim_league")
                        .select("league_id")
                        .distinct()
                        .collect()
                    )
                ]
            )
        )
    )

    score_map = (
        scoring
        .groupBy("season")
        .agg(
            F.map_from_entries(
                F.collect_list(
                    F.struct(
                        F.col("scoring_rule"),
                        F.col("scoring_value"),
                    )
                )
            ).alias("scoring")
        )
    )

    per_source = (
        p
        .filter(F.col("season_type") == "regular")
        .join(score_map, "season", "left")
        .withColumn(
            "projected_points",
            F.expr(
                """
                aggregate(
                    map_entries(stats),
                    cast(0D as double),
                    (acc, x) ->
                        acc + coalesce(
                            x.value * element_at(scoring, x.key),
                            0D
                        )
                )
                """
            ),
        )
        .withColumn(
            "projected_points",
            F.greatest(
                F.col("projected_points"),
                F.lit(0.0),
            ),
        )
    )

    # One provider/category projection per player/season/week.
    provider_level = (
        per_source
        .groupBy(
            "player_id",
            "season",
            "week",
            "company",
            "category",
        )
        .agg(
            F.max("projected_points").alias(
                "provider_category_points"
            )
        )
    )

    # Collapse categories within a provider. This prevents multiple category
    # records from multiplying the provider's contribution.
    provider_level = (
        provider_level
        .groupBy(
            "player_id",
            "season",
            "week",
            "company",
        )
        .agg(
            F.avg(
                "provider_category_points"
            ).alias("provider_projected_points")
        )
    )

    consensus = (
        provider_level
        .groupBy(
            "player_id",
            "season",
            "week",
        )
        .agg(
            F.countDistinct("company").alias(
                "company_count"
            ),
            F.avg(
                "provider_projected_points"
            ).alias("mean_projected_points"),
            F.expr(
                "percentile_approx(provider_projected_points, 0.5)"
            ).alias("median_projected_points"),
            F.min(
                "provider_projected_points"
            ).alias("min_projected_points"),
            F.max(
                "provider_projected_points"
            ).alias("max_projected_points"),
            F.stddev(
                "provider_projected_points"
            ).alias("projection_stddev"),
        )
        .withColumn(
            "mean_projected_points",
            F.greatest(
                F.col("mean_projected_points"),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "median_projected_points",
            F.greatest(
                F.col("median_projected_points"),
                F.lit(0.0),
            ),
        )
    )

    return add_model_metadata(consensus)


# ---------------------------------------------------------------------------
# Player valuation universe
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Player valuation universe
# ---------------------------------------------------------------------------

def build_player_valuation_universe(
    spark,
    projection_consensus,
    nfl_draft_capital,
):
    players = read(spark, "dim_player")

    weekly = read(spark, "fact_player_weekly")

    historical = (
        weekly
        .groupBy("player_id")
        .agg(
            F.sum(
                F.greatest(
                    F.coalesce(
                        F.col("fantasy_points"),
                        F.lit(0.0),
                    ),
                    F.lit(0.0),
                )
            ).alias("historical_points"),
            F.countDistinct("season").alias(
                "historical_seasons"
            ),
            F.max("season").alias(
                "last_historical_season"
            ),
        )
        .withColumn(
            "has_historical_production",
            (
                (F.col("historical_points") > 0)
                & (
                    F.col("last_historical_season")
                    >= CURRENT_SEASON - 1
                )
            ),
        )
    )

    current_proj = (
        projection_consensus
        .groupBy("player_id")
        .agg(
            F.max(
                F.when(
                    F.col("season") == CURRENT_SEASON,
                    1,
                ).otherwise(0)
            ).alias("has_current_projection"),
            F.max(
                F.when(
                    F.col("season") > CURRENT_SEASON,
                    1,
                ).otherwise(0)
            ).alias("has_future_projection"),
        )
    )

    # team != null is deliberately not required. An injured/free-agent player
    # can still have historical or projected fantasy value.
    out = (
        players
        .select(
            "player_id",
            "full_name",
            "position",
            "team",
            "birth_date",
            "years_exp",
            "rookie_year",
            "active",
            "status",
        )
        .filter(
            F.col("position").isin(RELEVANT_POSITIONS)
        )
        .join(
            historical,
            "player_id",
            "left",
        )
        .join(
            current_proj,
            "player_id",
            "left",
        )
        .join(
            nfl_draft_capital.select(
                "player_id",
                "nfl_draft_season",
                "nfl_draft_round",
                "nfl_draft_pick",
                "nfl_draft_capital_score",
            ),
            "player_id",
            "left",
        )
        .withColumn(
            "has_current_projection",
            F.coalesce(
                F.col("has_current_projection"),
                F.lit(0),
            ),
        )
        .withColumn(
            "has_future_projection",
            F.coalesce(
                F.col("has_future_projection"),
                F.lit(0),
            ),
        )
        .withColumn(
            "has_historical_production",
            F.coalesce(
                F.col("has_historical_production"),
                F.lit(False),
            ),
        )
        .withColumn(
            "is_on_nfl_roster",
            F.coalesce(
                F.col("team").isNotNull(),
                F.lit(False),
            ),
        )
        .withColumn(
            "has_nfl_draft_signal",
            F.coalesce(
                F.col("nfl_draft_capital_score").isNotNull(),
                F.lit(False),
            ),
        )
        .withColumn(
            "include_in_valuation_universe",
            (
                F.col("is_on_nfl_roster")
                & (
                    True
                    # F.col("has_historical_production")
                    # | (F.col("has_current_projection") == 1)
                    # | (F.col("has_future_projection") == 1)
                    # | F.col("has_nfl_draft_signal")
                )
            ).cast("boolean"),
        )
        .withColumn(
            "exclusion_reason",
            F.when(
                F.col("include_in_valuation_universe"),
                F.lit(None).cast("string"),
            ).otherwise(
                F.lit(
                    # "No NFL roster, meaningful historical production, "
                    # "projection, or NFL draft signal"
                    "Not Currently on an NFL roster"
                )
            ),
        )
    )

    return add_model_metadata(out)


# ---------------------------------------------------------------------------
# Player forecast
# ---------------------------------------------------------------------------

def build_player_forecast(
    spark,
    projection_consensus,
    valuation_universe,
):
    weekly = read(spark, "fact_player_weekly")

    players = (
        valuation_universe
        .filter(F.col("include_in_valuation_universe"))
        .select(
            "player_id",
            "full_name",
            "position",
            "team",
            "birth_date",
            "years_exp",
            "rookie_year",
            "active",
            "status",
        )
    )

    forecast_seasons = spark.createDataFrame(
        [
            (CURRENT_SEASON + i,)
            for i in range(FORECAST_YEARS)
        ],
        ["forecast_season"],
    )

    base = players.crossJoin(forecast_seasons)

    # Historical baseline is calculated only from seasons before the forecast
    # season. This prevents current-season realized points from leaking into
    # the historical component.
    season_points = (
        weekly
        .groupBy("season", "player_id")
        .agg(
            F.sum(
                F.greatest(
                    F.coalesce(
                        F.col("fantasy_points"),
                        F.lit(0.0),
                    ),
                    F.lit(0.0),
                )
            ).alias("season_points"),
            F.countDistinct("week").alias("weeks"),
        )
    )

    hist_rows = (
        base
        .select(
            "player_id",
            "forecast_season",
        )
        .join(
            season_points,
            "player_id",
            "left",
        )
        .filter(
            F.col("season").isNotNull()
            & (
                F.col("season")
                < F.col("forecast_season")
            )
        )
        .withColumn(
            "years_back",
            F.col("forecast_season")
            - F.col("season"),
        )
        .filter(F.col("years_back") <= 3)
        .withColumn(
            "raw_weight",
            F.pow(
                F.lit(0.5),
                F.col("years_back") - 1,
            ),
        )
    )

    hist = (
        hist_rows
        .groupBy(
            "player_id",
            "forecast_season",
        )
        .agg(
            (
                F.sum(
                    F.col("season_points")
                    * F.col("raw_weight")
                )
                / F.sum("raw_weight")
            ).alias(
                "weighted_historical_points"
            )
        )
    )

    # Projection rows are explicitly separated into full-season and weekly
    # projections. If a season-level projection exists, it wins. Otherwise
    # weekly projections are summed.
    season_projection = (
        projection_consensus
        .filter(F.col("week").isNull())
        .groupBy(
            "player_id",
            "season",
        )
        .agg(
            F.max(
                "median_projected_points"
            ).alias(
                "season_projected_points"
            ),
            F.max(
                "company_count"
            ).alias(
                "projection_company_count"
            ),
            F.avg(
                "projection_stddev"
            ).alias(
                "projection_stddev"
            ),
        )
    )

    weekly_projection = (
        projection_consensus
        .filter(F.col("week").isNotNull())
        .groupBy(
            "player_id",
            "season",
        )
        .agg(
            F.sum(
                "median_projected_points"
            ).alias(
                "weekly_projected_points"
            ),
            F.max(
                "company_count"
            ).alias(
                "weekly_projection_company_count"
            ),
            F.avg(
                "projection_stddev"
            ).alias(
                "weekly_projection_stddev"
            ),
        )
    )

    projection_by_season = (
        season_projection
        .join(
            weekly_projection,
            ["player_id", "season"],
            "full",
        )
        .withColumn(
            "projected_points",
            F.coalesce(
                F.col("season_projected_points"),
                F.col("weekly_projected_points"),
            ),
        )
        .withColumn(
            "projection_company_count",
            F.coalesce(
                F.col("projection_company_count"),
                F.col(
                    "weekly_projection_company_count"
                ),
            ),
        )
        .withColumn(
            "projection_stddev",
            F.coalesce(
                F.col("projection_stddev"),
                F.col(
                    "weekly_projection_stddev"
                ),
            ),
        )
        .withColumn(
            "projected_points",
            F.greatest(
                F.coalesce(
                    F.col("projected_points"),
                    F.lit(0.0),
                ),
                F.lit(0.0),
            ),
        )
    )

    joined = (
        base
        .join(
            hist,
            ["player_id", "forecast_season"],
            "left",
        )
        .join(
            projection_by_season,
            (
                base.player_id
                == projection_by_season.player_id
            )
            & (
                base.forecast_season
                == projection_by_season.season
            ),
            "left",
        )
        .drop(projection_by_season.player_id)
        .drop(projection_by_season.season)
    )

    age = safe_age(
        "birth_date",
        "forecast_season",
    )

    position = F.col("position")

    age_factor = (
        F.when(
            position == "RB",
            F.when(age <= 25, 1.00)
            .when(age <= 27, 0.93)
            .when(age <= 29, 0.78)
            .otherwise(0.55),
        )
        .when(
            position == "WR",
            F.when(age <= 25, 1.00)
            .when(age <= 28, 0.97)
            .when(age <= 30, 0.90)
            .when(age <= 32, 0.72)
            .otherwise(0.50),
        )
        .when(
            position == "TE",
            F.when(age <= 26, 1.00)
            .when(age <= 29, 0.97)
            .when(age <= 31, 0.88)
            .when(age <= 33, 0.70)
            .otherwise(0.50),
        )
        .when(
            position == "QB",
            F.when(age <= 29, 1.00)
            .when(age <= 32, 0.98)
            .when(age <= 35, 0.94)
            .when(age <= 38, 0.82)
            .otherwise(0.60),
        )
        .when(
            position == "K",
            F.when(age <= 32, 1.00)
            .when(age <= 36, 0.92)
            .otherwise(0.75),
        )
        .when(
            position == "DEF",
            F.lit(1.0),
        )
        .otherwise(F.lit(0.90))
    )

    # Current season: actual completed production + remaining projection.
    current_actual = (
        weekly
        .filter(
            (F.col("season") == CURRENT_SEASON)
            & F.col("week").isNotNull()
        )
        .groupBy("player_id")
        .agg(
            F.sum(
                F.greatest(
                    F.coalesce(
                        F.col("fantasy_points"),
                        F.lit(0.0),
                    ),
                    F.lit(0.0),
                )
            ).alias("actual_points_y1")
        )
    )

    # Determine the latest completed fantasy week represented in Silver.
    current_week = (
        weekly
        .filter(
            F.col("season") == CURRENT_SEASON
        )
        .agg(F.max("week").alias("max_week"))
        .collect()[0]["max_week"]
    )

    current_week = int(current_week or 0)

    remaining_projection = (
        projection_consensus
        .filter(
            (F.col("season") == CURRENT_SEASON)
            & F.col("week").isNotNull()
            & (F.col("week") > current_week)
        )
        .groupBy("player_id")
        .agg(
            F.sum(
                "median_projected_points"
            ).alias(
                "remaining_projected_points"
            ),
            F.avg(
                "projection_stddev"
            ).alias(
                "remaining_projection_stddev"
            ),
        )
    )

    joined = (
        joined
        .join(
            current_actual,
            "player_id",
            "left",
        )
        .join(
            remaining_projection,
            "player_id",
            "left",
        )
        .withColumn(
            "age",
            age,
        )
        .withColumn(
            "age_factor",
            F.coalesce(
                age_factor,
                F.lit(0.90),
            ),
        )
        .withColumn(
            "actual_points_y1",
            F.coalesce(
                F.col("actual_points_y1"),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "remaining_projected_points",
            F.coalesce(
                F.col(
                    "remaining_projected_points"
                ),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "current_full_season_points",
            F.when(
                F.col("forecast_season")
                == CURRENT_SEASON,
                F.col("actual_points_y1")
                + F.col(
                    "remaining_projected_points"
                ),
            ),
        )
        .withColumn(
            "raw_projection_points",
            F.when(
                F.col("forecast_season")
                == CURRENT_SEASON,
                F.col(
                    "current_full_season_points"
                ),
            ).otherwise(
                F.coalesce(
                    F.col("projected_points"),
                    F.col(
                        "weighted_historical_points"
                    ),
                    F.lit(0.0),
                )
            ),
        )
        .withColumn(
            "raw_projection_points",
            F.greatest(
                F.col("raw_projection_points"),
                F.lit(0.0),
            ),
        )
    )

    # Build a stable forecast trajectory. Future years use their own projection
    # when available; otherwise they carry the prior year's pre-age baseline.
    trajectory = None

    for year in range(
        CURRENT_SEASON,
        CURRENT_SEASON + FORECAST_YEARS,
    ):
        current = joined.filter(
            F.col("forecast_season") == year
        )

        if year == CURRENT_SEASON:
            current = current.withColumn(
                "base_points_before_age",
                F.col("raw_projection_points"),
            )
        else:
            prior = (
                trajectory
                .select(
                    "player_id",
                    F.col(
                        "base_points_before_age"
                    ).alias("prior_base_points"),
                )
            )

            current = (
                current
                .join(
                    prior,
                    "player_id",
                    "left",
                )
                .withColumn(
                    "base_points_before_age",
                    F.coalesce(
                        F.col("raw_projection_points"),
                        F.col("prior_base_points"),
                        F.lit(0.0),
                    ),
                )
            )

        current = current.withColumn(
            "expected_points",
            F.greatest(
                F.coalesce(
                    F.col("base_points_before_age"),
                    F.lit(0.0),
                )
                * F.coalesce(
                    F.col("age_factor"),
                    F.lit(0.0),
                ),
                F.lit(0.0),
            ),
        )

        if trajectory is None:
            trajectory = current
        else:
            trajectory = trajectory.unionByName(
                current.select(
                    trajectory.columns
                )
            )

    out = (
        trajectory
        .withColumn(
            "forecast_floor",
            F.greatest(
                F.col("expected_points")
                * F.lit(0.80),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "forecast_ceiling",
            F.greatest(
                F.col("expected_points")
                * F.lit(1.20),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "role_probability",
            F.col("age_factor"),
        )
        .select(
            "player_id",
            "forecast_season",
            "full_name",
            "position",
            "team",
            "birth_date",
            "age",
            "expected_points",
            "forecast_floor",
            "forecast_ceiling",
            "projected_points",
            "weighted_historical_points",
            "projection_company_count",
            "projection_stddev",
            "age_factor",
            "role_probability",
            "actual_points_y1",
            "remaining_projected_points",
        )
    )

    return add_model_metadata(out)


# ---------------------------------------------------------------------------
# Player value
# ---------------------------------------------------------------------------

def build_player_value(
    spark,
    forecast,
    valuation_universe,
):
    universe = (
        valuation_universe
        .filter(
            F.col(
                "include_in_valuation_universe"
            )
        )
        .select("player_id")
        .dropDuplicates()
    )

    forecast = (
        forecast
        .join(universe, "player_id", "inner")
        .withColumn(
            "expected_points",
            F.greatest(
                F.coalesce(
                    F.col("expected_points"),
                    F.lit(0.0),
                ),
                F.lit(0.0),
            ),
        )
    )

    # Replacement level is calculated only from the relevant valuation
    # universe, preventing inactive/fringe players from depressing it.
    replacement = (
        forecast
        .filter(
            F.col("expected_points") > 0
        )
        .groupBy(
            "forecast_season",
            "position",
        )
        .agg(
            F.expr(
                "percentile_approx(expected_points, 0.55)"
            ).alias("replacement_points")
        )
    )

    out = (
        forecast
        .join(
            replacement,
            [
                "forecast_season",
                "position",
            ],
            "left",
        )
        .withColumn(
            "vor",
            F.greatest(
                F.col("expected_points")
                - F.col("replacement_points"),
                F.lit(0.0),
            ),
        )
    )

    # Position-standardized VOR. Raw VOR remains available, but dynasty value
    # uses a position-specific scale so QB scoring magnitude doesn't dominate.
    position_scale = (
        out
        .groupBy(
            "forecast_season",
            "position",
        )
        .agg(
            F.greatest(
                F.expr(
                    "percentile_approx(vor, 0.75)"
                ),
                F.lit(1.0),
            ).alias("position_vor_scale")
        )
    )

    out = (
        out
        .join(
            position_scale,
            [
                "forecast_season",
                "position",
            ],
            "left",
        )
        .withColumn(
            "standardized_vor",
            F.when(
                F.col("position_vor_scale") > 0,
                F.col("vor")
                / F.col("position_vor_scale"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "standardized_vor",
            F.greatest(
                F.col("standardized_vor"),
                F.lit(0.0),
            ),
        )
    )

    out = (
        out
        .withColumn(
            "year_number",
            F.col("forecast_season")
            - F.lit(CURRENT_SEASON)
            + F.lit(1),
        )
        .withColumn(
            "time_discount",
            F.pow(
                F.lit(0.90),
                F.greatest(
                    F.col("year_number") - 1,
                    F.lit(0),
                ),
            ),
        )
        .withColumn(
            "discounted_vor",
            F.col("standardized_vor")
            * F.col("time_discount"),
        )
    )

    agg = (
        out
        .groupBy("player_id")
        .agg(
            F.first(
                "full_name",
                ignorenulls=True,
            ).alias("full_name"),
            F.first(
                "position",
                ignorenulls=True,
            ).alias("position"),
            F.first(
                "team",
                ignorenulls=True,
            ).alias("team"),

            F.max(
                F.when(
                    F.col("year_number") == 1,
                    F.col("expected_points"),
                )
            ).alias("expected_points_y1"),
            F.max(
                F.when(
                    F.col("year_number") == 2,
                    F.col("expected_points"),
                )
            ).alias("expected_points_y2"),
            F.max(
                F.when(
                    F.col("year_number") == 3,
                    F.col("expected_points"),
                )
            ).alias("expected_points_y3"),
            F.max(
                F.when(
                    F.col("year_number") == 4,
                    F.col("expected_points"),
                )
            ).alias("expected_points_y4"),
            F.max(
                F.when(
                    F.col("year_number") == 5,
                    F.col("expected_points"),
                )
            ).alias("expected_points_y5"),

            F.max(
                F.when(
                    F.col("year_number") == 1,
                    F.col("vor"),
                )
            ).alias("vor_y1"),
            F.max(
                F.when(
                    F.col("year_number") == 2,
                    F.col("vor"),
                )
            ).alias("vor_y2"),
            F.max(
                F.when(
                    F.col("year_number") == 3,
                    F.col("vor"),
                )
            ).alias("vor_y3"),
            F.max(
                F.when(
                    F.col("year_number") == 4,
                    F.col("vor"),
                )
            ).alias("vor_y4"),
            F.max(
                F.when(
                    F.col("year_number") == 5,
                    F.col("vor"),
                )
            ).alias("vor_y5"),

            F.max(
                F.when(
                    F.col("year_number") == 1,
                    F.col("standardized_vor"),
                )
            ).alias(
                "standardized_vor_y1"
            ),
            F.max(
                F.when(
                    F.col("year_number") == 2,
                    F.col("standardized_vor"),
                )
            ).alias(
                "standardized_vor_y2"
            ),
            F.max(
                F.when(
                    F.col("year_number") == 3,
                    F.col("standardized_vor"),
                )
            ).alias(
                "standardized_vor_y3"
            ),
            F.max(
                F.when(
                    F.col("year_number") == 4,
                    F.col("standardized_vor"),
                )
            ).alias(
                "standardized_vor_y4"
            ),
            F.max(
                F.when(
                    F.col("year_number") == 5,
                    F.col("standardized_vor"),
                )
            ).alias(
                "standardized_vor_y5"
            ),

            F.sum(
                "discounted_vor"
            ).alias("raw_dynasty_value"),

            F.avg(
                "projection_stddev"
            ).alias("projection_risk"),

            F.max(
                F.when(
                    F.col("year_number") == 1,
                    F.col("age_factor"),
                )
            ).alias("longevity_value"),
        )
    )

    out = (
        agg
        .withColumn(
            "current_production_value",
            F.coalesce(
                F.col("standardized_vor_y1"),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "future_production_value",
            F.greatest(
                F.col("raw_dynasty_value")
                - F.col(
                    "current_production_value"
                ),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "age_value",
            F.coalesce(
                F.col("longevity_value"),
                F.lit(1.0),
            ),
        )
        .withColumn(
            "positional_value",
            F.lit(1.0),
        )
        .withColumn(
            "scarcity_value",
            F.lit(1.0),
        )
        .withColumn(
            "opportunity_value",
            F.lit(1.0),
        )
        .withColumn(
            "injury_risk",
            F.lit(0.0),
        )
        .withColumn(
            "role_risk",
            1.0
            - F.coalesce(
                F.col("longevity_value"),
                F.lit(1.0),
            ),
        )
        .withColumn(
            "age_risk",
            1.0
            - F.coalesce(
                F.col("longevity_value"),
                F.lit(1.0),
            ),
        )
        .withColumn(
            "projection_risk",
            F.coalesce(
                F.col("projection_risk"),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "career_risk",
            1.0
            - F.coalesce(
                F.col("longevity_value"),
                F.lit(1.0),
            ),
        )
        .withColumn(
            "situation_risk",
            F.lit(0.0),
        )
        .withColumn(
            "overall_risk_adjustment",
            F.greatest(
                F.lit(0.70),
                1.0
                - F.col("role_risk") * F.lit(0.50),
            ),
        )
        .withColumn(
            "risk_adjusted_value",
            F.col("raw_dynasty_value")
            * F.col("overall_risk_adjustment"),
        )
    )

    max_value = (
        out
        .agg(
            F.max(
                "risk_adjusted_value"
            ).alias("max_value")
        )
        .collect()[0]["max_value"]
        or 1.0
    )

    out = (
        out
        .withColumn(
            "dynasty_value",
            F.round(
                F.col("risk_adjusted_value")
                / F.lit(float(max_value))
                * F.lit(1000.0),
                2,
            ),
        )
    )

    return add_model_metadata(out)


# ---------------------------------------------------------------------------
# Draft pick distribution
# ---------------------------------------------------------------------------

def build_draft_pick_distribution(spark):
    picks = read(spark, "fact_draft_pick")

    return add_model_metadata(
        picks
        .groupBy(
            "season",
            "round",
        )
        .agg(
            F.count("*").alias("pick_count"),
            F.avg("pick_number").alias(
                "avg_overall_pick"
            ),
            F.min("pick_number").alias(
                "min_overall_pick"
            ),
            F.max("pick_number").alias(
                "max_overall_pick"
            ),
        )
    )


# ---------------------------------------------------------------------------
# Draft pick value
# ---------------------------------------------------------------------------

def build_draft_pick_value(spark):
    picks = read(spark, "fact_draft_pick")

    # Resolve each seasonal league_id to the root league_id using
    # previous_league_id. The root league represents the original
    # league lineage and therefore contains the startup draft.
    leagues = (
        read(spark, "dim_league")
        .select(
            F.col("league_id").cast("string").alias("league_id"),
            F.col("previous_league_id")
                .cast("string")
                .alias("previous_league_id"),
        )
        .dropDuplicates(["league_id"])
    )

    league_links = {
        row["league_id"]: row["previous_league_id"]
        for row in leagues.collect()
    }

    root_map = []

    for league_id in league_links:
        current = league_id
        visited = set()

        while (
            current in league_links
            and league_links[current] is not None
            and current not in visited
        ):
            visited.add(current)
            current = league_links[current]

        root_map.append(
            (
                league_id,
                current,
            )
        )

    root_df = spark.createDataFrame(
        root_map,
        [
            "league_id",
            "root_league_id",
        ],
    )

    picks = (
        picks
        .withColumn(
            "league_id",
            F.col("league_id").cast("string"),
        )
        .join(
            root_df,
            "league_id",
            "left",
        )
        .withColumn(
            "startup_draft",
            F.col("league_id") == F.col("root_league_id"),
        )
        .withColumn(
            "continuous_pick_number",
            F.col("pick_number"),
        )
    )

    # Exclude the startup draft from calibration.
    # The startup draft is identified from the root league in the
    # Sleeper league lineage.
    calibration_picks = (
        picks
        .filter(
            ~F.col("startup_draft")
        )
    )

    # Sleeper's pick_number is already the continuous overall pick number.
    max_pick = (
        calibration_picks
        .agg(
            F.max("pick_number").alias("max_pick")
        )
        .collect()[0]["max_pick"]
    ) or 1

    out = (
        picks
        .withColumn(
            "expected_pick_value",
            F.when(
                F.col("startup_draft"),
                F.lit(None).cast("double"),
            ).otherwise(
                F.greatest(
                    F.lit(0.0),
                    F.lit(1000.0)
                    * (
                        F.lit(1.0)
                        - (
                            (
                                F.col("pick_number")
                                - F.lit(1.0)
                            )
                            / F.lit(float(max_pick))
                        )
                    ),
                )
            ),
        )
        .withColumn(
            "pick_value_method",
            F.when(
                F.col("startup_draft"),
                F.lit(
                    "startup_draft_no_pick_value"
                ),
            ).otherwise(
                F.lit(
                    "continuous_overall_pick_baseline"
                )
            ),
        )
    )

    return add_model_metadata(out)


# ---------------------------------------------------------------------------
# Team competitive window
# ---------------------------------------------------------------------------

def build_team_competitive_window(
    spark,
    standings,
    player_value,
    roster_composition,
):
    roster_value = (
        roster_composition
        .join(
            player_value.select(
                "player_id",
                "dynasty_value",
                "expected_points_y1",
            ),
            "player_id",
            "left",
        )
        .groupBy(
            "league_id",
            "season",
            "roster_id",
        )
        .agg(
            F.sum(
                F.coalesce(
                    F.col("dynasty_value"),
                    F.lit(0.0),
                )
            ).alias("roster_dynasty_value"),
            F.sum(
                F.coalesce(
                    F.col(
                        "expected_points_y1"
                    ),
                    F.lit(0.0),
                )
            ).alias(
                "roster_expected_points_y1"
            ),
            F.countDistinct(
                "player_id"
            ).alias("roster_player_count"),
        )
    )

    out = (
        standings
        .join(
            roster_value,
            [
                "league_id",
                "season",
                "roster_id",
            ],
            "left",
        )
        .withColumn(
            "roster_dynasty_value",
            F.coalesce(
                F.col("roster_dynasty_value"),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "roster_expected_points_y1",
            F.coalesce(
                F.col(
                    "roster_expected_points_y1"
                ),
                F.lit(0.0),
            ),
        )
    )

    return add_model_metadata(out)


# ---------------------------------------------------------------------------
# Trade analysis
# ---------------------------------------------------------------------------

def build_trade_analysis(
    spark,
    player_value,
):
    assets = (
        read(spark, "fact_transaction_asset")
        .filter(F.col("is_effective") == True)
        .alias("a")
    )

    tx = (
        read(spark, "fact_transaction")
        .filter(
            (F.col("status") == "complete")
            & (
                F.col("transaction_type")
                == "trade"
            )
        )
        .select(
            "transaction_id",
            "league_id",
            "season",
            "week",
            "transaction_type",
            "status",
            "roster_ids",
        )
        .alias("t")
    )

    players = (
        player_value
        .select(
            F.col("player_id").alias(
                "pv_player_id"
            ),
            F.col("dynasty_value"),
        )
        .alias("p")
    )

    valued = (
        assets
        .join(
            tx,
            F.col("a.transaction_id")
            == F.col("t.transaction_id"),
            "inner",
        )
        .join(
            players,
            (
                F.col("a.asset_type")
                == F.lit("player")
            )
            & (
                F.col("a.player_id")
                == F.col("p.pv_player_id")
            ),
            "left",
        )
        .select(
            F.col(
                "a.transaction_id"
            ).alias("transaction_id"),
            F.col(
                "t.league_id"
            ).alias("league_id"),
            F.col(
                "t.season"
            ).alias("season"),
            F.col(
                "t.week"
            ).alias("week"),
            F.col(
                "a.roster_id"
            ).alias("roster_id"),
            F.col(
                "a.asset_type"
            ).alias("asset_type"),
            F.col(
                "a.direction"
            ).alias("direction"),
            F.coalesce(
                F.col("p.dynasty_value"),
                F.lit(0.0),
            ).alias("asset_value"),
        )
    )

    out = (
        valued
        .groupBy(
            "transaction_id",
            "league_id",
            "season",
            "week",
            "roster_id",
        )
        .agg(
            F.sum(
                F.when(
                    F.col("direction") == "in",
                    F.col("asset_value"),
                ).otherwise(F.lit(0.0))
            ).alias("value_received"),
            F.sum(
                F.when(
                    F.col("direction") == "out",
                    F.col("asset_value"),
                ).otherwise(F.lit(0.0))
            ).alias("value_sent"),
        )
        .withColumn(
            "net_value_change",
            F.col("value_received")
            - F.col("value_sent"),
        )
    )

    return add_model_metadata(out)


# ---------------------------------------------------------------------------
# Manager profile
# ---------------------------------------------------------------------------

def build_manager_profile(
    spark,
    standings,
    trade_analysis,
):
    users = (
        read(spark, "dim_user")
        .select(
            F.col("user_id").alias(
                "manager_user_id"
            ),
            F.col("display_name").alias(
                "manager_display_name"
            ),
        )
    )

    trade = (
        trade_analysis
        .groupBy(
            "season",
            "roster_id",
        )
        .agg(
            F.countDistinct(
                "transaction_id"
            ).alias(
                "analyzed_transactions"
            ),
            F.sum(
                "net_value_change"
            ).alias(
                "net_transaction_value"
            ),
        )
    )

    out = (
        standings.alias("s")
        .join(
            users.alias("u"),
            F.col("s.owner_user_id")
            == F.col("u.manager_user_id"),
            "left",
        )
        .join(
            trade.alias("t"),
            (
                F.col("s.season")
                == F.col("t.season")
            )
            & (
                F.col("s.roster_id")
                == F.col("t.roster_id")
            ),
            "left",
        )
        .select(
            F.col("s.*"),
            F.col(
                "u.manager_user_id"
            ),
            F.col(
                "u.manager_display_name"
            ),
            F.coalesce(
                F.col(
                    "t.analyzed_transactions"
                ),
                F.lit(0),
            ).alias(
                "analyzed_transactions"
            ),
            F.coalesce(
                F.col(
                    "t.net_transaction_value"
                ),
                F.lit(0.0),
            ).alias(
                "net_transaction_value"
            ),
        )
    )

    return add_model_metadata(out)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    spark = SparkSession.builder.getOrCreate()

    print("=== GOLD BUILD START ===")

    standings = build_league_standings(spark)
    overwrite(standings, "league_standings")

    roster_composition = build_roster_composition(spark)
    overwrite(
        roster_composition,
        "roster_composition",
    )

    projection_consensus = (
        build_projection_consensus(spark)
    )
    overwrite(
        projection_consensus,
        "player_projection_consensus",
    )

    nfl_draft_capital = (
        build_nfl_draft_capital_stub(spark)
    )

    valuation_universe = (
        build_player_valuation_universe(
            spark,
            projection_consensus,
            nfl_draft_capital,
        )
    )
    overwrite(
        valuation_universe,
        "player_valuation_universe",
    )

    forecast = build_player_forecast(
        spark,
        projection_consensus,
        valuation_universe,
    )
    overwrite(
        forecast,
        "player_forecast",
    )

    player_value = build_player_value(
        spark,
        forecast,
        valuation_universe,
    )
    overwrite(
        player_value,
        "player_value",
    )

    draft_distribution = (
        build_draft_pick_distribution(spark)
    )
    overwrite(
        draft_distribution,
        "draft_pick_distribution",
    )

    draft_pick_value = (
        build_draft_pick_value(spark)
    )
    overwrite(
        draft_pick_value,
        "draft_pick_value",
    )

    team_window = build_team_competitive_window(
        spark,
        standings,
        player_value,
        roster_composition,
    )
    overwrite(
        team_window,
        "team_competitive_window",
    )

    trade = build_trade_analysis(
        spark,
        player_value,
    )
    overwrite(
        trade,
        "trade_analysis",
    )

    manager = build_manager_profile(
        spark,
        standings,
        trade,
    )
    overwrite(
        manager,
        "manager_profile",
    )

    print("=== GOLD BUILD COMPLETE ===")


if __name__ == "__main__":
    main()
