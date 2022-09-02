from collegebaseball import ncaa_scraper as ncaa
from collegebaseball import guts
import pytest
from time import sleep
import random

_TIMEOUT = 1


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
    df = guts.get_schools_table()
    # teams = df.sample(5)['school_id'].unique()
    teams = [167, 641, 9081, 973, 339, 1257, 335]  # 1257
    seasons = [x for x in range(2013, 2022)]
    generated_data = []
    for team in teams:
        for season in seasons:
            for variant in ['batting', 'pitching', 'fielding']:
                sleep(random.uniform(0, _TIMEOUT))
                new = ncaa.ncaa_team_totals(
                    int(team), season, variant, include_advanced=True,
                    split=None)
                new.to_csv('tests/data/test_team_totals_'+str(team)+'_' +
                           str(season)+'_'+str(variant)+'.csv', index=False)
                generated_data.append(new)
    return generated_data


@ pytest.fixture()
def generate_team_roster():
    df = guts.get_schools_table()
    teams = df.sample(5)['school_id'].unique()
    seasons = [x for x in range(2013, 2015)]
    generated_data = []
    for team in teams:
        sleep(random.uniform(0, _TIMEOUT))
        data = ncaa.ncaa_team_roster(int(team), seasons)
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


@ pytest.fixture()
def generate_team_stats():
    df = guts.get_schools_table()
    teams = df.sample(5)['school_id'].unique()
    seasons = [x for x in range(2013, 2023)]
    generated_data = []
    for team in teams:
        for season in seasons:
            for variant in ['batting', 'pitching', 'fielding']:
                sleep(random.uniform(0, _TIMEOUT))
                new = ncaa.ncaa_team_stats(
                    int(team), season, variant, include_advanced=True,
                    split=None)
                new.to_csv('tests/data/test_team_stats_'+str(team)+'_' +
                           str(season)+'_'+str(variant)+'.csv', index=False)
                generated_data.append(new)
    return generated_data


@ pytest.fixture()
def generate_team_game_logs():
    df = guts.get_schools_table()
    teams = df.sample(5)['school_id'].unique()
    # teams = [167, 1257, 641]
    seasons = [x for x in range(2013, 2023)]
    generated_data = []
    for team in teams:
        for season in seasons:
            for variant in ['batting', 'pitching', 'fielding']:
                sleep(random.uniform(0, _TIMEOUT))
                print(str(team)+' | '+str(season)+' | ' + variant)
                data = ncaa.ncaa_team_game_logs(
                    int(team), season, variant, include_advanced=True)
                data.to_csv('tests/data/test_team_game_logs_'+str(team)+'_' +
                            str(season)+'_'+str(variant)+'.csv', index=False)
                generated_data.append(data)
    return generated_data


@ pytest.fixture()
def generate_team_results():
    tests = [
        ("Cornell", 2022),
        ("Texas", 2018),
        ("Auburn", 2013),
        ("Kutztown", 2015),
        ("NYU", 2019)
    ]
    generated_data = []
    for i in tests:
        sleep(random.uniform(0, _TIMEOUT))
        data = ncaa.ncaa_team_results(i[0], i[1])
        generated_data.append(data)
    return generated_data


@ pytest.fixture()
def generate_player_game_logs():
    players = [
        (2347219, 2022, 'batting'),  # Sam Kaplan, Cornell
        (2471763, 2022, 'batting'),  # Ivan Melendez, Texas
        (2486499, 2022, 'fielding'),  # Jake Gelof, Virginia
        (2306475, 2022, 'pitching'),  # Nate Savino, Virginia
        (1997693, 2018, 'pitching'),
        (1997693, 2019, 'pitching')
    ]
    generated_data = []
    for i in players:
        sleep(random.uniform(0, _TIMEOUT))
        data = ncaa.ncaa_player_game_logs(
            i[0], i[1], i[2], include_advanced=True)
        data.to_csv('tests/data/test_player_game_logs_'+str(i[0])+'_' +
                    str(i[1])+'_'+str(i[2])+'.csv', index=False)
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
