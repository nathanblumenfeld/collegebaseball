from collegebaseball import ncaa_scraper as ncaa
from collegebaseball import guts
import pytest
from time import sleep
import random

_TIMEOUT = 1
_SCHOOLS = [736, 703]  # Vanderbilt (736), Texas (703)
_SEASONS = [x for x in range(2021, 2024)]
_PLAYERS = [
    (2112767, 'batting'),  # Marcus Ernst, Ohio St.
    (2112767, 'fielding'),  # Marcus Ernst, Ohio St.
    (2655626, 'pitching'),  # Brady Afthim, UConn
    (2347219, 'batting'),  # Sam Kaplan, Cornell
    (2471763, 'batting'),  # Ivan Melendez, Texas
    (2486499, 'fielding'),  # Jake Gelof, Virginia
    (2306475, 'pitching'),  # Nate Savino, Virginia
]

@ pytest.fixture()
def generate_career_stats():
    generated_data = []
    for i in _PLAYERS:
        data = ncaa.ncaa_career_stats(i[0], i[1])
        generated_data.append(data)
        # data.to_csv('tests/data/test_career_stats'+str(i[0])+'_' +
        #     str(i[1])+'.csv', index=False)
    return generated_data


@ pytest.fixture()
def generate_team_totals():
    generated_data = []
    for team in _SCHOOLS:
        for season in _SEASONS:
            for variant in ['batting', 'pitching', 'fielding']:
                sleep(random.uniform(0, _TIMEOUT))
                if variant != 'fielding':
                    for split in [None, 'runners_on', 'vs_LH', 'two_outs', 'vs_RH']:
                        sleep(random.uniform(0, _TIMEOUT))
                        new = ncaa.ncaa_team_totals(
                            int(team), season, variant, include_advanced=True,
                            split=split)
                        generated_data.append(new)
                else:
                    sleep(random.uniform(0, _TIMEOUT))
                    new = ncaa.ncaa_team_totals(
                        int(team), season, variant, include_advanced=True,
                        split=None)
                    generated_data.append(new)
    return generated_data


@ pytest.fixture()
def generate_team_roster():
    generated_data = []
    for team in _SCHOOLS:
        sleep(random.uniform(0, _TIMEOUT))
        data = ncaa.ncaa_team_roster(int(team), _SEASONS)
        generated_data.append(data)
    return generated_data


@ pytest.fixture()
def generate_team_season_roster():
    tests = [
        ("Cornell", 2023),
        ("Cornell", 2022),
        ("Texas", 2018),
        ("Auburn", 2013)
    ]
    generated_data = []
    for i in tests:
        sleep(random.uniform(0, _TIMEOUT))
        data = ncaa.ncaa_team_season_roster(i[0], i[1])
        generated_data.append(data)
        # data.to_csv('tests/data/test_team_season_roster'+str(tests[0])+'_' +
        #     str(tests[1])+'.csv', index=False)
    return generated_data


@ pytest.fixture()
def generate_team_stats():
    generated_data = []
    for team in _SCHOOLS:
        for season in _SEASONS:
            for variant in ['batting', 'pitching', 'fielding']:
                sleep(random.uniform(0, _TIMEOUT))
                if variant != 'fielding':
                    for split in [None, 'runners_on', 'vs_LH', 'two_outs', 'vs_RH']:
                        sleep(random.uniform(0, _TIMEOUT))
                        new = ncaa.ncaa_team_stats(
                            int(team), season, variant, include_advanced=True,
                            split=split)
                        generated_data.append(new)
                        # new.to_csv('tests/data/test_team_stats_'+str(team)+'_' +
                        #     str(season)+'_'+str(variant)+'_'+str(split)+'.csv', index=False)
                else: 
                    sleep(random.uniform(0, _TIMEOUT))
                    new = ncaa.ncaa_team_stats(
                        int(team), season, variant, include_advanced=True,
                        split=None)
                    generated_data.append(new)
                    # new.to_csv('tests/data/test_team_stats_'+str(team)+'_' +
                    #     str(season)+'_'+str(variant)+'.csv', index=False)
    return generated_data


@ pytest.fixture()
def generate_team_game_logs():
    generated_data = []
    for team in _SCHOOLS:
        for season in _SEASONS:
            for variant in ['batting', 'pitching', 'fielding']:
                sleep(random.uniform(0, _TIMEOUT))
                print(str(team)+' | '+str(season)+' | ' + variant)
                data = ncaa.ncaa_team_game_logs(
                    int(team), season, variant, include_advanced=True)
                # data.to_csv('tests/data/test_team_game_logs_'+str(team)+'_' +
                #             str(season)+'_'+str(variant)+'.csv', index=False)
                generated_data.append(data)
    return generated_data


@ pytest.fixture()
def generate_team_results():
    tests = [
        ("Vanderbilt", 2023),
        ("Texas", 2023),
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
        (2112767, 2023, 'batting'),  # Marcus Ernst, Ohio St.
        (2112767, 2023, 'fielding'),  # Marcus Ernst, Ohio St.
        (2655626, 2023, 'pitching'),  # Brady Afthim, UConn
        (2347219, 2022, 'batting'),  # Sam Kaplan, Cornell
        (2471763, 2022, 'batting'),  # Ivan Melendez, Texas
        (2486499, 2022, 'fielding'),  # Jake Gelof, Virginia
        (2306475, 2022, 'pitching')  # Nate Savino, Virginia
    ]
    generated_data = []
    for i in players:
        sleep(random.uniform(0, _TIMEOUT))
        data = ncaa.ncaa_player_game_logs(
            i[0], i[1], i[2], include_advanced=True)
        # data.to_csv('tests/data/test_player_game_logs_'+str(i[0])+'_' +
        #             str(i[1])+'_'+str(i[2])+'.csv', index=False)
        generated_data.append(data)
    return generated_data


def test_career_stats(generate_career_stats: list):
    for i in generate_career_stats:
        assert i is not None


def test_team_totals(generate_team_totals: list):
    for i in generate_team_totals:
        assert i is not None


def test_team_stats(generate_team_stats: list):
    for i in generate_team_stats:
        assert i is not None


def test_team_results(generate_team_results: list):
    for i in generate_team_results:
        assert i is not None


def test_team_game_logs(generate_team_game_logs: list):
    for i in generate_team_game_logs:
        assert i is not None


def test_team_roster(generate_team_roster: list):
    for i in generate_team_roster:
        assert i is not None


def test_player_game_logs(generate_player_game_logs: list):
    for i in generate_player_game_logs:
        assert i is not None


def test_team_season_roster(generate_team_season_roster: list):
    for i in generate_team_season_roster:
        assert i is not None
