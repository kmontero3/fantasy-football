from config.league_config import SLEEPER_USERNAME, TARGET_LEAGUE_NAME, SEASONS
from client import SleeperClient


def normalize_name(value):
    return " ".join(str(value or "").strip().lower().split())


def discover_phinasty_leagues(client=None):
    client = client or SleeperClient()
    user = client.user(SLEEPER_USERNAME)
    user_id = user["user_id"]

    discovered = {}

    for season in SEASONS:
        for league in client.user_leagues(user_id, season):
            if normalize_name(league.get("name")) == normalize_name(TARGET_LEAGUE_NAME):
                discovered[league["league_id"]] = {
                    "league_id": league["league_id"],
                    "season": int(league.get("season", season)),
                    "name": league.get("name"),
                    "previous_league_id": league.get("previous_league_id"),
                    "user_id": user_id,
                }

    queue = list(discovered.values())
    while queue:
        current = queue.pop()
        previous_id = current.get("previous_league_id")
        if not previous_id or previous_id in discovered:
            continue

        previous = client.league(previous_id)
        if normalize_name(previous.get("name")) != normalize_name(TARGET_LEAGUE_NAME):
            raise RuntimeError(
                f"Unexpected previous league {previous_id}: "
                f"{previous.get('name')!r}"
            )

        record = {
            "league_id": previous["league_id"],
            "season": int(previous["season"]),
            "name": previous.get("name"),
            "previous_league_id": previous.get("previous_league_id"),
            "user_id": user_id,
        }
        discovered[previous_id] = record
        queue.append(record)

    return user, sorted(discovered.values(), key=lambda x: x["season"])
