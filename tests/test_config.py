def test_analysis_user():
    from config.league_config import ANALYSIS_USERNAME
    assert ANALYSIS_USERNAME == "kmontero3"


def test_seasons():
    from config.league_config import SEASONS
    assert SEASONS == [2023, 2024, 2025, 2026]
