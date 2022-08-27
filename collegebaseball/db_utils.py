"""
db_utils

database utilities for collegebaseball

Created by Nathan Blumenfeld in Summer 2022
"""
import pandas as pd
from collegebaseball import datasets
from collegebaseball import ncaa_scraper as ncaa
import random
from tqdm import tqdm
from time import sleep


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


def download_rosters(seasons: list[int], divisions: list[int], save=True):
    res = pd.DataFrame()
    failures = []
    for season in seasons:
        for division in divisions:
            sleep(random.uniform(0, _TIMEOUT))
            try:
                new = download_season_rosters(int(season), int(division))
                print('new')
                print(new)
            except:
                print('fail, new')
                continue
            try:
                res = pd.concat([res, new])
                print('concatted')
                print(res)
            except:
                print('fail, concat')
                continue
    print('final')
    print(res)
    if save:
        res.to_parquet(
            'collegebaseball/data/'+str(divisions)+'_'+str(seasons)
            + '_rosters.parquet', index=False)
    return res, failures


def download_season_rosters(season: int, division: int, save=True):
    """
    """
    res = pd.DataFrame()
    failures = []
    school_ids = _SCHOOL_ID_LU_DF.loc[_SCHOOL_ID_LU_DF['division'] == division]
    school_ids = school_ids.school_id.unique()
    for i in school_ids:
        sleep(random.uniform(0, _TIMEOUT))
        try:
            new = ncaa.ncaa_team_season_roster(int(i), int(season))
            res = pd.concat([res, new])
        except:
            failures.append(i)
            continue
    res['season'] = season
    res['season'] = res['season'].astype('int64')
    res['division'] = division
    res['division'] = res['division'].astype('int64')
    if save:
        res.to_parquet('collegebaseball/data/d'+str(division) +
                       '_'+str(season)+'_rosters.parquet', index=False)
    return res


def download_team_results(season: int):
    """
    """
    res = pd.DataFrame()
    failures = []
    for i in tqdm(_SCHOOL_ID_LU_DF.ncaa_name.unique()):
        sleep(random.uniform(0, _TIMEOUT))
        try:
            new = ncaa.ncaa_team_results(int(i), int(season))
            res = pd.concat([res, new])
        except:
            failures.append(i)
            continue
    return res, failures


def download_team_stats(season: int, variant: str):
    """
    """
    res = pd.DataFrame()
    failures = []
    for i in tqdm(_SCHOOL_ID_LU_DF.ncaa_name.unique()):
        sleep(random.uniform(0, _TIMEOUT))
        try:
            new = ncaa.ncaa_team_stats(int(i), int(season), variant)
            new.loc[:, 'school'] = i
            new.loc[:, 'school'] = new.loc[:, 'school'].astype('string')
            res = pd.concat([res, new])
        except:
            failures.append(i)
            continue
    res.loc[:, 'season'] = season
    res.loc[:, 'season'] = res.loc[:, 'season'].astype('int64')
    return res, failures
