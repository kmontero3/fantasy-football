import time
import requests

from config.league_config import (
    MAX_RETRIES,
    BACKOFF_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
)


class SleeperClient:
    BASE = "https://api.sleeper.app/v1"
    PROJECTION_BASE = "https://api.sleeper.com/projections/nfl"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "fantasy-football-historical-backfill/1.0"
        })

    def get_json(self, url):
        last_error = None

        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)

                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    sleep_for = (
                        float(retry_after)
                        if retry_after
                        else BACKOFF_SECONDS * (2 ** attempt)
                    )
                    time.sleep(sleep_for)
                    continue

                response.raise_for_status()
                return response.json()

            except requests.RequestException as exc:
                last_error = exc
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(BACKOFF_SECONDS * (2 ** attempt))

        raise last_error

    def user(self, username):
        return self.get_json(f"{self.BASE}/user/{username}")

    def user_leagues(self, user_id, season):
        return self.get_json(
            f"{self.BASE}/user/{user_id}/leagues/nfl/{season}"
        )

    def league(self, league_id):
        return self.get_json(f"{self.BASE}/league/{league_id}")

    def users(self, league_id):
        return self.get_json(f"{self.BASE}/league/{league_id}/users")

    def rosters(self, league_id):
        return self.get_json(f"{self.BASE}/league/{league_id}/rosters")

    def drafts(self, league_id):
        return self.get_json(f"{self.BASE}/league/{league_id}/drafts")

    def draft(self, draft_id):
        return self.get_json(f"{self.BASE}/draft/{draft_id}")

    def draft_picks(self, draft_id):
        return self.get_json(f"{self.BASE}/draft/{draft_id}/picks")

    def draft_traded_picks(self, draft_id):
        return self.get_json(f"{self.BASE}/draft/{draft_id}/traded_picks")

    def traded_picks(self, league_id):
        return self.get_json(f"{self.BASE}/league/{league_id}/traded_picks")

    def transactions(self, league_id, week):
        return self.get_json(
            f"{self.BASE}/league/{league_id}/transactions/{week}"
        )

    def matchups(self, league_id, week):
        return self.get_json(
            f"{self.BASE}/league/{league_id}/matchups/{week}"
        )

    def winners_bracket(self, league_id):
        return self.get_json(f"{self.BASE}/league/{league_id}/winners_bracket")

    def losers_bracket(self, league_id):
        return self.get_json(f"{self.BASE}/league/{league_id}/losers_bracket")

    def players(self):
        return self.get_json(f"{self.BASE}/players/nfl")

    def state(self):
        return self.get_json(f"{self.BASE}/state/nfl")

    def projections(self, season, week=None, season_type="regular"):
        if week is None:
            url = f"{self.PROJECTION_BASE}/{season}"
        else:
            url = f"{self.PROJECTION_BASE}/{season}/{week}"

        return self.get_json(url + f"?season_type={season_type}")

    def stats(self, season, week=None):
        if week is None:
            url = f"{self.BASE}/stats/nfl/regular/{season}"
        else:
            url = f"{self.BASE}/stats/nfl/regular/{season}/{week}"
        return self.get_json(url)
