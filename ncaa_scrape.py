"""
A module to scrape and parse colliegate baseball statistics from stats.ncaa.org

created by Nathan Blumenfeld
"""

import pandas as pd
import time
import random
from bs4 import BeautifulSoup
import requests
import numpy as np
import metrics 

#LOOKUP PATHS
SCHOOL_ID_LU_PATH = 'data/schools.parquet'
SEASON_ID_LU_PATH = 'data/seasons.parquet'
PLAYERS_HISTORY_LU_PATH = 'data/players_history.parquet'
PLAYER_LU_PATH = 'data/player_seasons.csv'

def get_roster(school, season, headers = {'User-Agent':'Mozilla/5.0'}):     
    """
    Transmits GET request to stats.ncaa.org, parses roster information into DataFrame

    Inputs
    -----
    school/school_id (int)
    year (int): Valid for 2012 - 2021
    headers (dict): to include with GET request. NCAA is not kind to robots :(
    (default: {'User-Agent':'Mozilla/5.0'})

    Outputs
    -----
    DataFrame indexed by (team, season) containing the following columns:
     -   jersey
     -   stats_player_seq
     -   name
     -   position
     -   class_year
     -   games_played
     -   games_started
     -   height (if from 2019)
    """
    if type(school) is int: 
        school_id = school 
    elif type(school) is str: 
        school_id = lookup_school_id(school)
       
    if len(str(season)) == 4: 
        season_id = lookup_season_id(season)
    # get season_id from lookup table
    elif len(str(season)) == 5: 
        season_id = season
    
    try: 
        # doesn't take regular params, have to build url manually
        r = requests.get(f"""https://stats.ncaa.org/team/{str(school_id)}/roster/{str(season_id)}""", headers = headers)
        soup = BeautifulSoup(r.text, features='lxml')
        res = []
        if (season in [2019, 14781, 2022, 15860]): # records from 2019 season contain an additional field: 'height'
            num_values = 7
            col_names = ['jersey', 'stats_player_seq', 'name', 'position', 'height', 'class_year', 'games_played', 'games_started']
        else:
            num_values = 6
            col_names = ['jersey', 'stats_player_seq', 'name', 'position', 'class_year', 'games_played', 'games_started']
        for index, value in enumerate(soup.find_all('td')):
            if index % num_values == 0: # each player has 6 associated values in table
                details = [] # new list for each players
                res.append(details) # DataFrame deals with extra list for us
            if index % num_values == 1: # need to get stats_player_seq from href tag
                try: # not sure if there is a way to do with w/o catching an error, but it works.
                    details.append((value.contents[0].get('href')[-7:])) # 7-digit id
                    details.append(value.contents[0].string) # player name
                except:
                    details.append(None) # no id found (occurs when players has 0 games played)
                    details.append(value.contents[0]) # player name
            else:
                try:
                    details.append(value.contents[0])
                except:
                    details.append(None)
        df = pd.DataFrame(res)
        df.columns = col_names
        return df
    except:
        return f'''could not retrieve {season} roster for {school}'''

def get_multiyear_roster(school, start, end, request_timeout=0.1):
    """
    Returns: concatenated DataFrame using get_roster across [start, end]
    """
    seasons = pd.read_parquet(SEASON_ID_LU_PATH)
    roster = pd.DataFrame()
    for season in range(start, end+1):
        new = get_roster(school, season) 
        new['season'] = season
        new['school'] = school
        if 'height' in new.columns: 
            new = new.drop(columns = ['height'])
        roster = pd.concat([roster, new])
        time.sleep(random.uniform(0, request_timeout))
        
    roster = pd.merge(roster, seasons, how = 'left', on = 'season')
    try: 
        res = roster.drop(columns=['Unnamed: 0'])
    except: 
        res = roster
    return res
    
def get_career_stats(stats_player_seq, variant, headers = {'User-Agent':'Mozilla/5.0'}):
    """
    Transmits GET request to stats.ncaa.org, parses career stats  into DataFrame

    https://stats.ncaa.org/player/index?id=15204&org_id=746&stats_player_seq=2306451&year_stat_category_id=14761
    
    
    https://stats.ncaa.org/player/index?id=15580&org_id=746&stats_player_seq=2306451&year_stat_category_id=14841

    """
    # craft GET request to NCAA site
    season = lookup_seasons_played(stats_player_seq)[0]
    season_ids = lookup_season_ids(season)
    season_id = season_ids[0]
    
    if variant == 'batting': 
        year_stat_category_id = season_ids[1]
    else: 
        year_stat_category_id = season_ids[2]
    payload = {'id':str(season_id), 'stats_player_seq':str(stats_player_seq), 'year_stat_category_id':str(year_stat_category_id)}
    url = 'https://stats.ncaa.org/player/index'
    # send request
    try:
        r = requests.get(url, params = payload, headers = headers)
    except:
        print('An error occurred with the GET Request')
        if r.status_code == 403:
            print('403 Error: NCAA blocked request')
        return pd.DataFrame()
    
    try: 
        # parse data
        soup = BeautifulSoup(r.text, features = 'lxml')
        table = soup.find_all('table')[2]

        # get table headers
        headers = []
        for val in table.find_all('th'):
            headers.append(val.string.strip())

        # get table data
        rows = []
        row = []
        for val in table.find_all('td'): # TODO: cleanup as much as possible
            # data is also encoded in data-order attr of td elements
            if 'data-order' in val.attrs:
                row.append(val['data-order'])
            elif val.a is not None:
                row.append(val.a.attrs['href'].split('/')[2])
            elif val.string.strip() != 'Career' and 'width' not in val.attrs:
                if row != []:
                    rows.append(row)
                row = []
                row.append(val.string.strip())
            else:
                if val.string.strip() != 'Career':
                    row.append(val.string.strip())

        # Turn into DataFrame
        df = pd.DataFrame(rows)
        df.columns = headers
        df = transform_career_stats(df)
        return df
    except: 
        print('no records found')
        return pd.DataFrame()

def format_names(original):
    """
    A helper function to turn names from 
    
    Ex.
    "Blumenfeld, Nathan" --> "Nathan Blumenfeld"
    """
    try: 
        split = original.split(',')
        split.reverse()
        res = ' '.join(split).strip().title()
    except:
        res = np.nan
    return res

def eliminate_dashes(df):
    """
    Returns: a Dataframe with dashes replaced by 0.00 
    A helper function to replace the weird dashes the NCAA uses with 0.00
    """
    df.replace('None', 0.00, inplace=True)
    df.replace('<NA>', 0.00, inplace=True)
    df.replace('', 0.00, inplace=True)
    df.replace(' ', 0.00, inplace=True)
    df.replace('-', 0.00, inplace=True)
    df.replace('--', 0.00, inplace=True)
    df.replace('---', 0.00, inplace=True)
    df.replace('  -', 0.00, inplace=True)
    df.replace('- -', 0.00, inplace=True)
    df.replace('-  ', 0.00, inplace=True)
    return df 

def transform_career_stats(df):
    """
    A helper function to transform raw data loaded from ncaa with get_career_stats(). Valid 2013-2022
    
    Inputs 
    -----
    DataFrame output from get_career_stats function 
    
    Outputs
    -----
    DataFrame indexed by stats_player_seq, season_id
    """
    #TODO: there's gotta be a cleaner way to do this, but this works for now. 
    df = eliminate_dashes(df)
    df.fillna(value = 0.00, inplace = True)
    cols = df.columns
    if 'Player' in cols: 
            df = df.rename(columns={'Player':'name'})
    if 'name' in cols:     
        df.name = df.name.apply(format_names)
    if 'stats_player_seq' in cols: 
        df.stats_player_seq = df.stats_player_seq.astype('string')
        df.stats_player_seq = df.stats_player_seq.str.replace(r'\D+', '')
    if 'Year' in cols: 
        df['Year'] = df['Year'].astype('string')
        df['season'] = df['Year'].str[:4]
        df.drop(columns=['Year'], inplace=True)
    if 'season' in cols:
        df['season'] = df['season'].astype('int64')
    if 'Team' in cols:
        df = df.rename(columns={'Team':'school_id'}) 
    if 'GP' in cols: 
        df['GP'] = df['GP'].astype('int64')
    if 'GS' in cols: 
        df['GS'] = df['GS'].astype('int64')
    if 'BB' in cols: 
        df['BB'] = df['BB'].astype('int64')
    if 'Jersey' in cols: 
        df['Jersey'] = df['Jersey'].astype('int64')
    if 'DP' in cols: 
        df['DP'] = df['DP'].astype('float64')
    if 'RBI2out' in cols: 
        df['RBI2out'] = df['RBI2out'].astype('int64')       
    if 'G' in cols: 
        df['G'] = df['G'].astype('int64')
    if 'H' in cols: 
        df['H'] = df['H'].astype('int64')
    if 'SO' in cols: 
        df['SO'] = df['SO'].astype('int64')
    if 'TB' in cols: 
        df['TB'] = df['TB'].astype('int64')
    if '2B' in cols: 
        df['2B'] = df['2B'].astype('int64')
    if '3B' in cols: 
        df['3B'] = df['3B'].astype('int64')
    if 'HR' in cols: 
        df['HR'] = df['HR'].astype('int64')
    if 'RBI' in cols: 
        df['RBI'] = df['RBI'].astype('int64')
    if 'R' in cols: 
        df['R'] = df['R'].astype('int64')
    if 'AB' in cols: 
        df['AB'] = df['AB'].astype('int64')
    if 'HBP' in cols: 
        df['HBP'] = df['HBP'].astype('int64')
    if 'SF' in cols: 
        df['SF'] = df['SF'].astype('int64')
    if 'SFA' in cols: 
        df['SFA'] = df['SFA'].astype('int64')
    if 'SHA' in cols: 
        df['SHA'] = df['SHA'].astype('int64')
    if 'K' in cols: 
        df['K'] = df['K'].astype('int64')
    if 'SH' in cols: 
        df['SH'] = df['SH'].astype('int64')
    if 'Picked' in cols: 
        df['Picked'] = df['Picked'].astype('int64')
    if 'SB' in cols: 
        df['SB'] = df['SB'].astype('int64')
    if 'IBB' in cols: 
        df['IBB'] = df['IBB'].astype('int64')
    if 'CS' in cols: 
        df['CS'] = df['CS'].astype('int64')
    if 'OBPct' in cols: 
        df['OBPct'] = df['OBPct'].astype('float64')
    if 'BA' in cols: 
        df['BA'] = df['BA'].astype('float64')
    if 'BF' in cols: 
        df['BF'] = df['BF'].astype('int64') 
    if 'HB' in cols: 
        df['HB'] = df['HB'].astype('int64') 
    if '2B-A' in cols: 
        df['2B-A'] = df['2B-A'].astype('int64')
    if '3B-A' in cols: 
        df['3B-A'] = df['3B-A'].astype('int64')
    if 'HR-A' in cols: 
        df['HR-A'] = df['HR-A'].astype('int64')
    if 'IP' in cols: 
        df['IP'] = df['IP'].astype('float64')
        # df['IP'] = df['IP'].apply(adjust_innings_pitched, axis=1)
    if 'SlgPct' in cols: 
        df['SlgPct'] = df['SlgPct'].astype('float64')
    if 'OPP DP' in cols: 
        df['OPP DP'] = df['OPP DP'].astype('int64')
    if 'Pitches' in cols: 
        df['Pitches'] = df['Pitches'].str.replace(',','')
        df['Pitches'] = df['Pitches'].astype('int64')
    if 'App' in cols: 
        df['App'] = df['App'].astype('int64')
    return df

def get_team_stats(school, season, variant, headers={'User-Agent':'Mozilla/5.0'}):
    """
    Returns: A dataframe of player season totals for a given team in a given season. Takes either school or school_id.
    data from stats.ncaa.org
    
    INPUTS
    ------
    school/school_id
    season/season_id
    variant
    headers
    
    OUTPUTS
    -------
    DataFrame
    
    example url for cornell 2021 batting stats
    https://stats.ncaa.org/team/167/stats?game_sport_year_ctl_id=15580&id=15580&year_stat_category_id=14841 
    """
    if type(school) is int:
        school_id = school
    elif type(school) is str: 
        school_id = lookup_school_id(school)

    if len(str(season)) == 4:
        season_ids = lookup_season_ids(season)
        season_id = season_ids[0]

    elif len(str(season)) == 5: 
        season_id = season
        season_ids = lookup_season_ids_reverse(season_id)

    if variant == 'batting':
        stat_category_id = season_ids[1]
    elif variant == 'pitching':
        stat_category_id = season_ids[2]
       
    url = 'https://stats.ncaa.org/team/'+str(school_id)+'/stats'
    payload = {'game_sport_year_ctl_id':str(season_id), 'id':str(season_id), 'year_stat_category_id':str(stat_category_id)}
        
    try:
        r = requests.get(url, params = payload, headers = headers)
    except:
        print('An error occurred with the GET Request')
        if r.status_code == 403:
            print('403 Error: NCAA blocked request')
        return pd.DataFrame()
    
    
    # try to parse data
    try: 
        soup = BeautifulSoup(r.text, features = 'lxml')
        table = soup.find_all('table')[2]

        # get table headers
        headers = []
        for val in table.find_all('th'):
            headers.append(val.string.strip())

        rows = []
        row = []
        i = 0
        for child in table.find('tr', 'text').find_all_next('td'):
            if i % len(headers) == 0: 
                row = []
                rows.append(row)
            if 'data-order'in child.attrs:
                row.append(str(child['data-order']).strip())
            else:
                row.append(str(child.string))
            i+=1

        df = pd.DataFrame(rows)
        df.columns = headers
        df['season'] = season
        df = df.loc[(df.Player != 'Opponent Totals') & (df.Player != 'Totals')]
        res = transform_team_stats(df, variant= variant)
        return res
    except: 
        print(f'''Could not find {season} {variant} stats for {school}''')
        return pd.DataFrame()

def transform_team_stats(df, variant):
    """
    A helper function to transform raw data loaded from ncaa with get_career_stats
    
    Inputs 
    -----
    DataFrame output from get_career_stats function 
    
    Outputs
    -----
    DataFrame indexed by stats_player_seq, season_id
    """
    df = eliminate_dashes(df)
    df.fillna(value = 0.00, inplace = True)
    cols = df.columns
    if 'Player' in cols: 
        df.rename(columns={'Player':'name'}, inplace=True)
    
    df['name'] = df['name'].apply(format_names)
    df['name'] = df['name'].astype('string')
    df['Pos'] = df['Pos'].astype('string')
    df['Yr'] = df['Yr'].astype('string')

    if 'stats_player_seq' in cols: 
        df.stats_player_seq = df.stats_player_seq.astype('string')
        df.stats_player_seq = df.stats_player_seq.str.replace(r'\D+', '')
    if 'Year' in cols: 
        df['Year'] = df['Year'].astype('string')
        df['season'] = df['Year'].str[:4]
        df.drop(columns=['Year'], inplace=True)
    if 'Pos' in cols:
        df = df.rename(columns={'Pos':'pos'}) 
    if 'season' in cols:
        df['season'] = df['season'].astype('int64')
    if 'Team' in cols:
        df = df.rename(columns={'Team':'school_id'}) 
    if 'GP' in cols: 
        df['GP'] = df['GP'].astype('int64')
    if 'GS' in cols: 
        df['GS'] = df['GS'].astype('int64')
    if 'BB' in cols: 
        df['BB'] = df['BB'].astype('int64')
    if 'Jersey' in cols: 
        df['Jersey'] = df['Jersey'].astype('int64')
    if 'G' in cols: 
        df.drop(columns=['G'], inplace=True)
    if variant == 'batting':
        if 'DP' in cols: 
            df['DP'] = df['DP'].astype('int64')
        if 'RBI2out' in cols: 
            df.drop(columns=['RBI2out'], inplace=True)
        if 'H' in cols: 
            df['H'] = df['H'].astype('int64')
        if 'SO' in cols: 
            df['SO'] = df['SO'].astype('int64')
        if 'TB' in cols: 
            df['TB'] = df['TB'].astype('int64')
        if '2B' in cols: 
            df['2B'] = df['2B'].astype('int64')
        if '3B' in cols: 
            df['3B'] = df['3B'].astype('int64')
        if 'HR' in cols: 
            df['HR'] = df['HR'].astype('int64')
        if 'RBI' in cols: 
            df['RBI'] = df['RBI'].astype('int64')
        if 'R' in cols: 
            df['R'] = df['R'].astype('int64')
        if 'AB' in cols: 
            df['AB'] = df['AB'].astype('int64')
        if 'HBP' in cols: 
            df['HBP'] = df['HBP'].astype('int64')
        if 'SF' in cols: 
            df['SF'] = df['SF'].astype('int64')
        if 'K' in cols: 
            df['K'] = df['K'].astype('int64')
        if 'SH' in cols: 
            df['SH'] = df['SH'].astype('int64')
        if 'Picked' in cols: 
            df['Picked'] = df['Picked'].astype('int64')
        if 'SB' in cols: 
            df['SB'] = df['SB'].astype('int64')
        if 'IBB' in cols: 
            df['IBB'] = df['IBB'].astype('int64')
        if 'CS' in cols: 
            df['CS'] = df['CS'].astype('int64')
        if 'OPP DP' in cols: 
            df['OPP DP'] = df['OPP DP'].astype('int64')
        if 'OBPct' in cols: 
            df.drop(columns=['OBPct'], inplace=False)
        if 'BA' in cols: 
            df.drop(columns=['BA'], inplace=False)
        if 'SlgPct' in cols: 
            df.drop(columns=['SlgPct'], inplace=False)
         
    else:
        if 'ERA' in cols: 
            df.drop(columns=['ERA'], inplace=False)
        if 'IP' in cols: 
            df['IP'] = df['IP'].astype('float64')       
        if 'CG' in cols: 
            df['CG'] = df['CG'].astype('int64')      
        if 'IP' in cols: 
            df['IP'] = df['IP'].astype('float64')             
        if 'H' in cols: 
            df['H'] = df['H'].astype('int64')          
        if 'R' in cols: 
            df['R'] = df['R'].astype('int64')         
        if 'ER' in cols: 
            df['ER'] = df['ER'].astype('int64')          
        if 'BB' in cols: 
            df['BB'] = df['BB'].astype('int64')          
        if 'SO' in cols: 
            df['SO'] = df['SO'].astype('int64')   
        if 'SHO' in cols: 
            df['SHO'] = df['SHO'].astype('int64') 
        if 'BF' in cols: 
            df['BF'] = df['BF'].astype('int64')          
        if 'P-OAB' in cols: 
            df['P-OAB'] = df['P-OAB'].astype('int64')          
        if '2B-A' in cols: 
            df['2B-A'] = df['2B-A'].astype('int64')          
        if '3B-A' in cols: 
            df['3B-A'] = df['3B-A'].astype('int64')          
        if 'Bk' in cols: 
            df['Bk'] = df['Bk'].astype('int64')          
        if 'HR-A' in cols: 
            df['HR-A'] = df['HR-A'].astype('int64')          
        if 'WP' in cols: 
            df['WP'] = df['WP'].astype('int64')          
        if 'IBB' in cols: 
            df['IBB'] = df['IBB'].astype('int64')          
        if 'Inh Run' in cols: 
            df['Inh Run'] = df['Inh Run'].astype('int64')           
        if 'Inh Run Score' in cols: 
            df['Inh Run Score'] = df['Inh Run Score'].astype('int64')          
        if 'SHA' in cols: 
            df['SHA'] = df['SHA'].astype('int64')          
        if 'SFA' in cols: 
            df['SFA'] = df['SFA'].astype('int64')          
        if 'GO' in cols: 
            df['GO'] = df['GO'].astype('int64')          
        if 'FO' in cols: 
            df['FO'] = df['FO'].astype('int64')          
        if 'W' in cols: 
            df['W'] = df['W'].astype('int64')          
        if 'L' in cols: 
            df['L'] = df['L'].astype('int64')          
        if 'HB' in cols: 
            df['HB'] = df['HB'].astype('int64')          
        if 'SV' in cols: 
            df['SV'] = df['SV'].astype('int64')          
        if 'KL' in cols: 
            df['KL'] = df['KL'].astype('int64')       
        if 'pickoffs' in cols: 
            df['pickoffs'] = df['pickoffs'].astype('int64')                      
        if 'Pitches' in cols: 
            df['Pitches'] = df['Pitches'].str.replace(',','')
            df.fillna(value = 0.00, inplace = True)
            df['Pitches'] = df['Pitches'].astype('int64')
        if 'App' in cols: 
            df['App'] = df['App'].astype('int64')
        df = df.loc[df.App > 0]
    return df

def lookup_season_ids(season):
    """
    Returns: tuple of (season_id, batting_id, pitching_id) for desired season
    """
    lu_df = pd.read_parquet(SEASON_ID_LU_PATH)
    season_row = lu_df.loc[lu_df['season'] == season]
    season_id = season_row.values[0][1]
    batting_id = season_row.values[0][2]
    pitching_id = season_row.values[0][3]  
    return (season_id, batting_id, pitching_id)

def lookup_season_ids_reverse(season_id):
    """
    Returns: tuple of (season_id, batting_id, pitching_id) for desired season
    """
    lu_df = pd.read_parquet(SEASON_ID_LU_PATH)
    season_row = lu_df.loc[lu_df['season_id'] == season_id]
    season = season_row['season'].values[0]
    batting_id = season_row['batting_id'].values[0]
    pitching_id = season_row['pitching_id'].values[0]
    return (season, batting_id, pitching_id)

def lookup_season_id(season):
    """
    Returns: tuple of (season_id, batting_id, pitching_id) for desired season
    """
    lu_df = pd.read_parquet(SEASON_ID_LU_PATH)
    season_row = lu_df.loc[lu_df['season'] == season]
    season_id = season_row.values[0][1]
    return season_id

def lookup_seasons_played(stats_player_seq): 
    """
    """
    df = pd.read_parquet('data/players_history.parquet')
    row = df.loc[df.player_id == stats_player_seq]
    return row['debut_season'].values[0], row['last_played_season'].values[0]
    
def lookup_school_id(school, school_lu = SCHOOL_ID_LU_PATH):
    """
    Returns: school_id (int)
    
    valid 2013-2022. In case these change. 
    """
    df = pd.read_parquet(school_lu)
    school_row = df.loc[(df.ncaa_name == school.title())]
    if len(school_row) == 0:
        # try to search against bd_name
        school_row = df.loc[(df.bd_name == school.title())]
    if len(school_row) == 0: 
        return f'''could not find school {school}'''
    else: 
        return int(school_row['school_id'].values[0])
        
def lookup_player_id(name, school, player_lu = PLAYER_LU_PATH):
    """
    Returns: DataFrame containing possible player_ids (int)
    
    """
    df = pd.read_csv(player_lu)
    school_id = lookup_school_id(school)
    if type(school_id) is int:
        player_row = df.loc[df.name == name.title()]
        player_row = player_row.loc[df.school_id == school_id]
        
        if len(player_row) == 0:
            return f'''could not find player {name}'''
        else: 
            return player_row['player_id'].values[0]
    else: 
        return school_id
        
def get_conference_records(conference, show_progress = True, print_interval = 5, request_timeout = 0.1):
    """
    Returns a table of all season records for players in a given conference
    
    Inputs
    ----
    conference (str)
    show_progress (bool): If True, prints progress of function call
    (default: True)
    print_interval (int): If show_progress, the interval between progress updates
    outputs 
    request_timeout (float): length in seconds between successive calls to database
    
    Outputs
    -----
    DataFrame 
    """
    res = pd.DataFrame()
    df = pd.read_pickle(PLAYER_LU_PATH).groupby(by='stats_player_seq').agg('first')
    in_conference =  df.loc[df.conference == conference].reset_index()
    num_calls = len(in_conference)
    for index, row in in_conference.iterrows():
        if index % print_interval == 0: 
            print('progress: '+str(index)+' of '+str(num_calls)+' complete')
        try:
            new = get_career_stats(stats_player_seq = row['stats_player_seq'], season_id= row['season_id'], team_id = row['school_id'])
            new['season_id'] = row['season_id']
            new['school_id'] = row['school_id']
            new['stats_player_seq'] = row['stats_player_seq']
            new['name'] = row['name']
        except:
            print('failure: '+str(row['name'])+' ('+ str(row['stats_player_seq'])+') | season: '+str(row['season'])+' ('+str(row['season_id'])+') | school: '+str(row['school'])+' ('+str(row['school_id'])+')')
            new = pd.DataFrame()
            new['season_id'] = row['season_id']
            new['school_id'] = row['school_id']
            new['stats_player_seq'] = row['stats_player_seq']
            new['name'] = row['name']
        res = pd.concat([res, new])
        time.sleep(random.uniform(0, request_timeout))
    return res.reset_index()

def build_roster_db(school_id_lu_path = SCHOOL_ID_LU_PATH, season_id_lu_path = SEASON_ID_LU_PATH, request_timeout = 0.25, save_as = 'players.csv', status_updates = True):
    """
    Calls get_roster to build a table of all (player, season) records

    Inputs
    -----
    school_id_lu_path (str): filepath of school_id lookup table
    (default: 'data/ncaa/school_lookup.csv')
    request_timeout (int): maximum timeout between requests in seconds, as to not overload NCAA servers
    (default: 0.25)
    save_as (str): filepath to save DataFrame as csv to. To not save, set to None
    (default: 'players.csv')
    status_updates (bool): whether to give updates on progress of db creation
    (default: True)

    Outputs
    -----
    DataFrame indexed by player, season containing stats_player_seq and relevant season_ids

    """
    df = pd.read_parquet(school_id_lu_path).iloc[:, 1:]
    dfs = {}
    i = 0
    # Potential Improvement. Vectorize the following loop. Is there a way to send multiple get requests at once?
    for index, row in df.iterrows():
        dfs[(row.school_id, row.year)] = get_roster(row.school_id, row.year, headers = {'User-Agent':'Mozilla/5.0'})
        # tryna be sneaky
        time.sleep(random.uniform(0, request_timeout))
        if status_updates:
            i+=1
            if i % 50 == 0:
                print(str(i)+'/8437')

    players = pd.DataFrame() # create an empty df
    for key in dfs.keys(): # for each (team, season) dataframe in dictionary
        df_new = dfs[key].copy() # don't write over while iterating through
        if key[1] == 2019:  # for whatever reason 2019 season records contain an additional field
            df_new = df_new.drop(columns=['height']) # get rid of said additional field for 2019 records
        # add columns from data in dictionary
        df_new['school_id'] = key[0]
        df_new['season'] = key[1]
        # append data to result
        players = pd.concat([players, df_new])

    seasons = pd.read_parquet(season_id_lu_path).iloc[:,1:]
    res = players.merge(seasons, how='left', on='season')
    res = res.rename(columns={'id':'season_id'})
    if save_as is not None:
        res.to_csv(save_as, index=False)
    return res

