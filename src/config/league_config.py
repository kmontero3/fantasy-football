import os

# The only username used to discover the target league.
SLEEPER_USERNAME = os.getenv("SLEEPER_USERNAME", "kmontero3")

# Exact Sleeper league name to include. This intentionally excludes
# unrelated leagues such as "Foosballs".
TARGET_LEAGUE_NAME = os.getenv("TARGET_LEAGUE_NAME", "Phinasty League")

# Historical seasons to inspect when discovering the league chain.
SEASONS = [2023, 2024, 2025, 2026]

CATALOG = os.getenv("FF_CATALOG", "fantasy_football")
BRONZE_SCHEMA = os.getenv("FF_BRONZE_SCHEMA", "bronze")
SILVER_SCHEMA = os.getenv("FF_SILVER_SCHEMA", "silver")
GOLD_SCHEMA = os.getenv("FF_GOLD_SCHEMA", "gold")

MAX_WEEK = 18
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 4
BACKOFF_SECONDS = 2
