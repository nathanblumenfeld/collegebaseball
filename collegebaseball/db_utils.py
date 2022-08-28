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


# GET request options
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
    df = pd.read_parquet(datasets.get_school_path())
    school_ids = df.loc[df['division'] == division]
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
    df = pd.read_parquet(datasets.get_school_path())
    for i in tqdm(df.ncaa_name.unique()):
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
    df = pd.read_parquet(datasets.get_school_path())
    for i in tqdm(df.ncaa_name.unique()):
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
