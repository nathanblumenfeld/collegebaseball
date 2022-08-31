"""
download_utils.py

large-download utilities for collegebaseball

Created by Nathan Blumenfeld in Summer 2022
"""
import pandas as pd
from collegebaseball import guts
from collegebaseball import ncaa_scraper as ncaa
import random
from tqdm import tqdm
from time import sleep
import warnings
warnings.filterwarnings("ignore")

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
    df = guts.get_schools_table()
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
    df = pd.read_parquet(guts.get_school_path())
    for i in tqdm(df.ncaa_name.unique()):
        sleep(random.uniform(0, _TIMEOUT))
        try:
            new = ncaa.ncaa_team_results(int(i), int(season))
            res = pd.concat([res, new])
        except:
            failures.append(i)
            continue
    return res, failures


def download_team_stats(seasons: list[int], variant: str, divisions: list[int], save=True):
    """
    """
    res = pd.DataFrame()
    failures = []
    df = guts.get_schools_table()
    for division in tqdm(divisions):
        schools = df.loc[df.division == division]
        for season in tqdm(seasons):
            for i in tqdm(schools.school_id.unique()):
                sleep(random.uniform(0, _TIMEOUT))
                try:
                    new = ncaa.ncaa_team_stats(int(i), int(season), variant)
                    new['school_id'] = i
                    new['school_id'] = new['school_id'].astype('int32')
                    res = pd.concat([res, new])
                except:
                    failures.append(i)
                    continue
            res['season'] = season
            res['season'] = res['season'].astype('int32')
            res['division'] = division
            res['division'] = res['division'].astype('int8')
            if save:
                res.to_csv('collegebaseball/data/d'+str(division)+'_'+str(season) +
                           '_'+variant+'_stats.csv', index=False)
    return res, failures


def download_player_game_logs(season, division=None, save=True):
    '''
    Gets literally all stats in D1 NCAA Mens Baseball.
    This will take some time to complete.
    '''
    df = guts.get_rosters_table()
    players = df.loc[df.season == season]
    if division is not None:
        players = players.loc[players.division == division]
    total = str(len(players))
    i = 1
    batting_res = pd.DataFrame()
    pitching_res = pd.DataFrame()
    fielding_res = pd.DataFrame()
    for index, player in players.iterrows():
        print(str(i)+'/'+total)
        i += 1
        stats_player_seq = player['stats_player_seq']
        for variant in ['batting', 'pitching', 'fielding']:
            sleep(random.uniform(0, _TIMEOUT))
            try:
                new = ncaa.ncaa_player_game_logs(stats_player_seq,
                                                 season,  variant)
                if variant == 'batting':
                    batting_res = pd.concat([batting_res, new])
                elif variant == 'pitching':
                    pitching_res = pd.concat([pitching_res, new])
                else:
                    fielding_res = pd.concat([fielding_res, new])
            except:
                print('failure: '+str(stats_player_seq) +
                      ' | '+str(season)+' | '+str(variant))
                continue
    if save:
        batting_res.to_csv('collegebaseball/data/batting_player_game_logs_' +
                           str(season)+'.csv', index=False)
        pitching_res.to_csv('collegebaseball/data/pitching_player_game_logs_' +
                            str(season)+'.csv', index=False)
        fielding_res.to_csv('collegebaseball/data/fielding_player_game_logs_' +
                            str(season)+'.csv', index=False)
    return batting_res, pitching_res, fielding_res
