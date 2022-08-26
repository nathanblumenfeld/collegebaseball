"""
update_guts

A module to faciliate the updating of collegebaseball's guts

created by Nathan Blumenfeld in Summer 2022
"""
import pandas as pd
from collegebaseball import db_utils, datasets


_SEASON_ID_LU_PATH = datasets.get_season_lu_table()
_SCHOOL_ID_LU_PATH = datasets.get_school_table()
_PLAYERS_HISTORY_LU_PATH = datasets.get_players_history_table()
_PLAYER_ID_LU_PATH = datasets.get_player_id_lu_table()
_ROSTERS_LU_PATH = datasets.get_rosters_table()

# pre-load lookup tables for performance
_SCHOOL_ID_LU_DF = pd.read_parquet(_SCHOOL_ID_LU_PATH)
_SEASON_LU_DF = pd.read_parquet(_SEASON_ID_LU_PATH)
_PLAYERS_HISTORY_LU_DF = pd.read_parquet(_PLAYERS_HISTORY_LU_PATH)
_PLAYER_ID_LU_DF = pd.read_parquet(_PLAYER_ID_LU_PATH)
_ROSTERS_DF = pd.read_parquet(_ROSTERS_LU_PATH)

# GET request options
_HEADERS = {'User-Agent': 'Mozilla/5.0'}
_TIMEOUT = 1
_LW_FILEPATH = datasets.get_linear_weights_table()


def update_season_ids(season, season_id, batting_id, pitching_id):
    """A function to update and save the season_id lookup table

    Args:
        season (int)
        season_id (int)
        batting_id (int)
        pitching_id (int)
    """
    old = pd.read_parquet(_SEASON_ID_LU_PATH)
    new = pd.DataFrame(
        [[season, season_id, batting_id, pitching_id]], columns=old.columns)
    updated_df = pd.concat([old, new]).reset_index(drop=True)
    updated_df.to_parquet(_SEASON_ID_LU_PATH)
    return updated_df


def update_rosters(season):
    """
    overwrites file
    removes current
    """
    old = _ROSTERS_DF.loc[_ROSTERS_DF.season != season]
    new, failures = db_utils.download_season_rosters(season)
    print(failures)
    res = pd.concat([new, old])
    res.to_parquet(_ROSTERS_LU_PATH, index=False)


# def update_available_stat_ids(new_season_available_stat_ids):
#     """"

#     Args:
#         new_season_available_stat_ids (_type_): _description_
#     """
#     available_stat_id
#     pass
