from cgitb import lookup
from collegebaseball import guts, lookup
import pytest


@ pytest.fixture()
def generate_lookup_school_id():
    df = guts.get_schools_table()
    teams = df.sample(5)['ncaa_name'].unique()
    # teams = ['Cornell', 'Texas', 'Lincoln Memorial']
    res = []
    for team in teams:
        res.append(
            lookup.lookup_school(team))
    return res


@ pytest.fixture()
def generate_lookup_season_id():
    seasons = [x for x in range(2013, 2023)]
    res = []
    for season in seasons:
        res.append(
            lookup.lookup_season_id(season))
    return res


@ pytest.fixture()
def generate_lookup_season_ids():
    seasons = [x for x in range(2013, 2023)]
    res = []
    for season in seasons:
        res.append(
            lookup.lookup_season_ids(season))
    return res


@ pytest.fixture()
def generate_lookup_season_reverse():
    season_ids = [15860, 10942, 11620, 12080]
    res = []
    for season_id in season_ids:
        res.append(
            lookup.lookup_season_reverse(season_id))
    return res


@ pytest.fixture()
def generate_lookup_seasons_played():
    df = guts.get_players_history_table()
    players = df.sample(5)['stats_player_seq'].unique()
    res = []
    for player in players:
        res.append(
            lookup.lookup_seasons_played(player))
    return res


@ pytest.fixture()
def generate_lookup_school_info():
    df = guts.get_schools_table()
    teams = df.sample(5)['school_id'].unique()
    # teams = [167, 746, 'Albright', 'Cornell']
    res = []
    for team in teams:
        res.append(
            lookup._lookup_school_info(int(team)))
    return res


@ pytest.fixture()
def generate_lookup_season_info():
    seasons = [x for x in range(2013, 2023)]
    res = []
    for season in seasons:
        res.append(lookup._lookup_season_info(season))
    return res


def test_lookup_season_reverse(generate_lookup_season_reverse):
    for i in generate_lookup_season_reverse:
        assert i is not None


def test_lookup_season_info(generate_lookup_season_info):
    for i in generate_lookup_season_info:
        assert i is not None


def test_lookup_school_id(generate_lookup_school_id):
    for i in generate_lookup_school_id:
        assert i is not None


def test_lookup_season_id(generate_lookup_season_id):
    for i in generate_lookup_season_id:
        assert i is not None


def test_lookup_season_ids(generate_lookup_season_ids):
    for i in generate_lookup_season_ids:
        assert i is not None


def test_lookup_seasons_played(generate_lookup_seasons_played):
    for i in generate_lookup_seasons_played:
        assert i is not None


def test_lookup_school_info(generate_lookup_school_info):
    for i in generate_lookup_school_info:
        assert i is not None
