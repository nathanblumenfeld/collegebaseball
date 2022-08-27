from collegebaseball import ncaa_scraper as ncaa
import pytest
from time import sleep
import random


_TIMEOUT = 1


# @ pytest.fixture()
# def generate_lookup_player_id():
#     players = [1552732]
#     res = []
#     for player in players:
#         res.append(
#             ncaa.lookup_player_id(player,)
#     return res


# @ pytest.fixture()
# def generate_lookup_player_reverse():
#     players = [1552732, 1882863, 2142781, 2014085, 2457024]
#     res = []
#     for player in players:
#         res.append(
#             ncaa.lookup_player_reverse(player,)
#     return res


@ pytest.fixture()
def generate_lookup_school_id():
    schools = ['Cornell', 'Texas', 'Lincoln Memorial']
    res = []
    for school in schools:
        res.append(
            ncaa.lookup_school_id(school))
    return res


@ pytest.fixture()
def generate_lookup_season_id():
    seasons = [x for x in range(2013, 2023)]
    res = []
    for season in seasons:
        res.append(
            ncaa.lookup_season_id(season))
    return res


@ pytest.fixture()
def generate_lookup_season_ids():
    seasons = [x for x in range(2013, 2023)]
    res = []
    for season in seasons:
        res.append(
            ncaa.lookup_season_ids(season))
    return res


@ pytest.fixture()
def generate_lookup_season_reverse():
    season_ids = [15860, 10942, 11620, 12080]
    res = []
    for season_id in season_ids:
        res.append(
            ncaa.lookup_season_reverse(season_id))
    return res


@ pytest.fixture()
def generate_lookup_seasons_played():
    players = []
    res = []
    for player in players:
        res.append(
            ncaa.lookup_seasons_played(player))
    return res


@ pytest.fixture()
def generate_lookup_team_info():
    teams = [167, 746, 'Albright', 'Cornell']
    res = []
    for team in teams:
        res.append(
            ncaa.lookup_team_info(team))
    return res


@ pytest.fixture()
def generate_lookup_season_info():
    seasons = [2022, 2021, 2020]
    res = []
    for season in seasons:
        res.append(ncaa.lookup_season_info(season))
    return res


@ pytest.fixture()
def generate_career_stats():
    players = [
        (2347219, 'batting'),  # Sam Kaplan, Cornell
        (2471763, 'batting'),  # Ivan Melendez, Texas
        (2486499, 'fielding'),  # Jake Gelof, Virginia
        (2306475, 'pitching'),  # Nate Savino, Virginia
        (1559823, 'batting'),
        (1995168, 'pitching'),
        (1101814, 'fielding')
    ]
    generated_data = []
    for i in players:
        data = ncaa.ncaa_career_stats(i[0], i[1])
        generated_data.append(data)
    return generated_data


@ pytest.fixture()
def generate_team_totals():
    teams = [167, 1257, 641]
    seasons = [x for x in range(2013, 2023)]
    batting_splits = [None, 'vs_LHP', 'two_outs']
    pitching_splits = [None, 'vs_RHB', 'two_outs']
    generated_data = []
    for team in teams:
        for season in seasons:
            for variant in ['batting', 'pitching', 'fielding']:
                if variant == 'batting':
                    splits = batting_splits
                elif variant == 'pitching':
                    splits = pitching_splits
                else:
                    splits = [None]
                for split in splits:
                    sleep(random.uniform(0, _TIMEOUT))
                    generated_data.append(ncaa.ncaa_team_totals(
                        team, season, variant, include_advanced=False,
                        split=split))
    return generated_data


@ pytest.fixture()
def generate_team_roster():
    teams = [167, 1257, 641]
    seasons = [x for x in range(2013, 2023)]
    generated_data = []
    for team in teams:
        sleep(random.uniform(0, _TIMEOUT))
        data = ncaa.ncaa_team_roster(team, seasons)
        generated_data.append(data)
    return generated_data


@ pytest.fixture()
def generate_team_season_roster():
    tests = [
        ("Cornell", 2022),
        ("Texas", 2018),
        ("Auburn", 2013)
    ]
    generated_data = []
    for i in tests:
        sleep(random.uniform(0, _TIMEOUT))
        data = ncaa.ncaa_team_season_roster(i[0], i[1])
        generated_data.append(data)
    return generated_data
    pass


@ pytest.fixture()
def generate_team_stats():
    teams = [167, 1257, 641]
    seasons = [x for x in range(2013, 2023)]
    batting_splits = [None, 'vs_LHP', 'vs_RHP', 'two_outs']
    pitching_splits = [None, 'vs_LHB', 'vs_RHB', 'two_outs']
    generated_data = []
    for team in teams:
        for season in seasons:
            for variant in ['batting', 'pitching', 'fielding']:
                if variant == 'batting':
                    splits = batting_splits
                elif variant == 'pitching':
                    splits = pitching_splits
                else:
                    splits = [None]
                for split in splits:
                    sleep(random.uniform(0, _TIMEOUT))
                    generated_data.append(ncaa.ncaa_team_stats(
                        team, season, variant, include_advanced=True,
                        split=split))
    return generated_data


@ pytest.fixture()
def generate_team_game_logs():
    teams = [167, 1257, 641]
    seasons = [x for x in range(2013, 2023)]
    generated_data = []
    for team in teams:
        for season in seasons:
            for variant in ['batting', 'pitching', 'fielding']:
                sleep(random.uniform(0, _TIMEOUT))
                print(str(team)+' | '+str(season)+' | ' + variant)
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
        sleep(random.uniform(0, _TIMEOUT))
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
        sleep(random.uniform(0, _TIMEOUT))
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


def test_lookup_season_reverse(generate_lookup_season_reverse):
    for i in generate_lookup_season_reverse:
        assert i is not None


# def test_lookup_player_id(generate_lookup_player_id):
#     for i in generate_lookup_player_id:
#         assert i is not None


def test_lookup_season_info(generate_lookup_season_info):
    for i in generate_lookup_season_info:
        assert i is not None


def test_lookup_school_id(generate_lookup_school_id):
    for i in generate_lookup_school_id:
        assert i is not None


# def test_lookup_player_reverse(generate_lookup_player_reverse):
#     for i in generate_lookup_player_reverse:
#         assert i is not None


def test_lookup_season_id(generate_lookup_season_id):
    for i in generate_lookup_season_id:
        assert i is not None


def test_lookup_season_ids(generate_lookup_season_ids):
    for i in generate_lookup_season_ids:
        assert i is not None


def test_lookup_seasons_played(generate_lookup_seasons_played):
    for i in generate_lookup_seasons_played:
        assert i is not None


def test_lookup_team_info(generate_lookup_team_info):
    for i in generate_lookup_team_info:
        assert i is not None
