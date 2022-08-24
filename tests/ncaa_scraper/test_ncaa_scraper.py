from collegebaseball import ncaa_scraper as ncaa
import pytest


@ pytest.fixture()
def generate_career_stats():
    players = [
        (2347219, 'batting'),  # Sam Kaplan, Cornell
        (2471763, 'batting'),  # Ivan Melendez, Texas
        (2486499, 'fielding'),  # Jake Gelof, Virginia
        (2306475, 'pitching')  # Nate Savino, Virginia
    ]
    generated_data = []
    for i in players:
        data = ncaa.ncaa_career_stats(i[0], i[1])
        generated_data.append(data)
    return generated_data


@ pytest.fixture()
def generate_team_totals():
    teams = [167]
    seasons = [2022]
    generated_data = []
    for team in teams:
        for season in seasons:
            for variant in ['batting', 'pitching', 'fielding']:
                data = ncaa.ncaa_team_totals(team, season, variant)
                generated_data.append(data)
    return generated_data


@ pytest.fixture()
def generate_team_roster():
    teams = [167]
    seasons = [2022]
    generated_data = []
    for team in teams:
        data = ncaa.ncaa_team_roster(team, seasons)
        generated_data.append(data)
    return generated_data


@pytest.fixture()
def generate_team_season_roster():
    tests = [
        ("Cornell", 2022),
        ("Texas", 2018),
        ("Auburn", 2013)
    ]
    generated_data = []
    for i in tests:
        data = ncaa.ncaa_team_season_roster(i[0], i[1])
        generated_data.append(data)
    return generated_data


@ pytest.fixture()
def generate_team_stats():
    teams = [167]
    seasons = [2022]
    generated_data = []
    for team in teams:
        for season in seasons:
            for variant in ['batting', 'pitching', 'fielding']:
                data = ncaa.ncaa_team_stats(
                    team, season, variant, include_advanced=True)
                generated_data.append(data)
    return generated_data


@pytest.fixture()
def generate_team_game_logs():
    teams = [167]
    seasons = [2022]
    generated_data = []
    for team in teams:
        for season in seasons:
            for variant in ['batting', 'pitching', 'fielding']:
                data = ncaa.ncaa_team_game_logs(team, season, variant)
                generated_data.append(data)
    return generated_data


@ pytest.fixture()
def generate_team_results():
    tests = [
        ("Cornell", 2022),
        ("Texas", 2018),
        ("Auburn", 2013)
    ]
    generated_data = []
    for i in tests:
        data = ncaa.ncaa_team_results(i[0], i[1])
        generated_data.append(data)
    return generated_data


@pytest.fixture()
def generate_player_game_logs():
    players = [
        (2347219, 2022, 'batting'),  # Sam Kaplan, Cornell
        (2471763, 2022, 'batting'),  # Ivan Melendez, Texas
        (2486499, 2022, 'fielding'),  # Jake Gelof, Virginia
        (2306475, 2022, 'pitching')  # Nate Savino, Virginia
    ]
    generated_data = []
    for i in players:
        data = ncaa.ncaa_player_game_logs(i[0], i[1], i[2])
        generated_data.append(data)
    return generated_data


def test_career_stats(generate_career_stats):
    for i in generate_career_stats:
        assert i is not None


def test_team_totals(generate_team_totals):
    for i in generate_team_totals:
        assert i is not None


def test_team_stats(generate_team_stats):
    for i in generate_team_stats:
        assert i is not None


def test_team_results(generate_team_results):
    for i in generate_team_results:
        assert i is not None


def test_team_game_logs(generate_team_game_logs):
    for i in generate_team_game_logs:
        assert i is not None


def test_team_roster(generate_team_roster):
    for i in generate_team_roster:
        assert i is not None


def test_player_game_logs(generate_player_game_logs):
    for i in generate_player_game_logs:
        assert i is not None


def test_team_season_roster(generate_team_season_roster):
    for i in generate_team_season_roster:
        assert i is not None
