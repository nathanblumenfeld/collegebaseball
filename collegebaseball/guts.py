"""
guts.py

A module to provide convenient read-access to collegebaseball's internal data

created by Nathan Blumenfeld in Summer 2022
"""
from importlib import resources
import pandas as pd


def get_schools_path():
    """

    """
    with resources.path("collegebaseball.data",
                        "schools.parquet") as f:
        data_file_path = f
    return data_file_path


def get_schools_table(division=None):
    """
    """
    df = pd.read_parquet(get_schools_path())
    if division is not None:
        df = df.loc[df.division == division]
    return df


def get_player_lu_path():
    """

    """
    with resources.path("collegebaseball.data",
                        "player_lookup.csv") as f:
        data_file_path = f
    return data_file_path


def get_player_lu_table():
    """
    """
    return pd.read_csv(get_player_lu_path())


def get_seasons_path():
    """
    """
    with resources.path("collegebaseball.data",
                        "seasons.parquet") as f:
        data_file_path = f
    return data_file_path


def get_seasons_table():
    """
    """
    return pd.read_parquet(get_seasons_path())


def get_rosters_path():
    """
    """
    with resources.path("collegebaseball.data",
                        "rosters_2012_2022_all.csv") as f:
        data_file_path = f
    return data_file_path


def get_rosters_table():
    """
    """
    return pd.read_csv(get_rosters_path())


def get_players_history_path():
    """

    """
    with resources.path("collegebaseball.data",
                        "players_history.csv") as f:
        data_file_path = f
    return data_file_path


def get_players_history_table():
    """
    """
    return pd.read_csv(get_players_history_path())


# provided by Robert Fray
def get_linear_weights_path():
    """

    """
    with resources.path("collegebaseball.data",
                        "linear_weights.csv") as f:
        data_file_path = f
    return data_file_path


def get_linear_weights_table():
    """
    """
    return pd.read_csv(get_linear_weights_path())


def get_season_linear_weights(season: int, division):
    """
    Args:
        season(int, YYYY): valid 2013-2022

    Returns:
        Single-row Dataframe with the following columns:
            season(primary key)
            season_id
            batting_id
            pitching_id
            fielding_id
    """
    df = get_linear_weights_table()
    df = df.loc[df['season'] == int(season)]
    res = df.loc[df['division'] == int(division)]
    return res
