"""
A module to scrape and parse college baseball statistics from stats.ncaa.org

Created by Nathan Blumenfeld in Spring 2022
"""

import pandas as pd
import time
import random
from bs4 import BeautifulSoup
import requests
import numpy as np

#LOOKUP PATHS
_SCHOOL_ID_LU_PATH = 'collegebaseball/data/schools.parquet'
_SEASON_ID_LU_PATH = 'collegebaseball/data/seasons.parquet'
_PLAYER_HISTORY_LU_PATH = 'collegebaseball/data/players_history_new_3.parquet'
_PLAYER_LU_PATH = 'collegebaseball/data/player_seasons.csv'

#GET request options
_HEADERS = {'User-Agent':'Mozilla/5.0'}
_TIMEOUT = 0.1

def get_roster(school, season):     
    """
    Transmits GET request to stats.ncaa.org, parses roster information

    Args: 
        school: school name (str) or NCAA school_id (int)
        season: season as (int, YYYY) or NCAA season_id (int): 
        valid for 2012 - 2022. 

    Returns: 
        DataFrame containing the following columns:
         -   jersey
         -   stats_player_seq
         -   name
         -   position
         -   class_year
         -   games_played
         -   games_started
         -   height (if 2019)
        
    data from stats.ncaa.org
    """
    # handling the school/school_id input types
    if type(school) is int: 
        school_id = school 
    elif type(school) is str: 
        school_id = lookup_school_id(school)
      
    # handling season/season_id input types
    if len(str(season)) == 4: 
        season_id = lookup_season_id(season)
    # get season_id from lookup table
    elif len(str(season)) == 5: 
        season_id = season
    
    try: 
        # doesn't take regular params, have to build url manually
        request_body = 'https://stats.ncaa.org/team/'
        request_body += f'''{str(school_id)}/roster/{str(season_id)}'''
        r = requests.get(request_body, headers=_HEADERS)
        soup = BeautifulSoup(r.text, features='lxml')
        res = []
        
        # records from 2019, 2022 seasons contain an additional field: 'height'
        if (season in [2019, 14781, 2022, 15860]): 
            num_values = 7
            col_names = ['jersey','stats_player_seq', 'name', 'position', \
                         'height', 'class_year', 'games_played',\
                         'games_started']
        else:
            num_values = 6
            col_names = ['jersey', 'stats_player_seq', 'name', 'position', \
                         'class_year', 'games_played', 'games_started']
            
        for index, value in enumerate(soup.find_all('td')):
            # each player has 6 associated values in table
            if index % num_values == 0: # each player has 6 associated values in table
                details = [] 
                res.append(details) # DataFrame deals with extra list for us
            
            if index % num_values == 1:
                # need to get stats_player_seq from href tag
                try: 
                    details.append((value.contents[0].get('href')[-7:]))
                    details.append(value.contents[0].string)
                except:
                    # no id found (occurs when players has 0 games played)
                    details.append(None)
                    details.append(value.contents[0])
            else:
                try:
                    details.append(value.contents[0])
                except:
                    details.append(None)
                    
        df = pd.DataFrame(res)
        df.columns = col_names
        df.stats_player_seq = df.stats_player_seq.astype('str')
        df.stats_player_seq = df.stats_player_seq.str.replace('=', '')
        df = df.loc[df.stats_player_seq != 'None']
        df.stats_player_seq = df.stats_player_seq.astype('int64')
        return df
    except:
        print(f'''could not retrieve {season} roster for {school}''')
        return pd.DataFrame()    

def get_multiyear_roster(school, start, end):
    """
    Args: 
        school/school_id (str/int): name of school or school_id. 
        end: a season/season_id (int/int), valid for 2012 - 2021.
        end must be <= start
        start: a season/season_id (int/int), valid for 2012 - 2021.
        start must be >= end

    Returns:
        concatenated DataFrame using get_roster across [start, end]
        
    data from stats.ncaa.org
    """
    roster = pd.DataFrame()
    for season in range(start, end+1):
        try: 
            new = get_roster(school, season) 
            new.loc[:, 'season'] = season
            new.loc[:, 'school'] = school
            if 'height' in new.columns: 
                new.drop(columns=['height'], inplace=True)
            roster = pd.concat([roster, new])
            try: 
                roster.drop(columns=['Unnamed: 0'], inplace=True)
            except: 
                continue
        except: 
            continue
        time.sleep(random.uniform(0, _TIMEOUT))
    return roster
    
def get_career_stats(stats_player_seq, variant):
    """
    Transmits GET request to stats.ncaa.org, parses career stats

    Args: 
        stats_player_seq (int): the NCAA player_id
        variant (str): the type of stats, either 'batting' or 'pitching
    Returns: 
        DataFrame 
    
    data from stats.ncaa.org
    """
    # craft GET request to NCAA site
    season = lookup_seasons_played(stats_player_seq)[0]
    season_ids = lookup_season_ids(int(season))
    season_id = season_ids[0]
    
    if variant == 'batting': 
        year_stat_category_id = season_ids[1]
    else: 
        year_stat_category_id = season_ids[2]
    
    payload = {'id':str(season_id), 'stats_player_seq':str(stats_player_seq), \
               'year_stat_category_id':str(year_stat_category_id)}
    url = 'https://stats.ncaa.org/player/index'
    # send request
    try:
        r = requests.get(url, params=payload, headers=_HEADERS)
    except:
        print('An error occurred with the GET Request')
        if r.status_code == 403:
            print('403 Error: NCAA blocked request')
        return pd.DataFrame()
    
    try: 
        # parse data
        soup = BeautifulSoup(r.text, features='lxml')
        table = soup.find_all('table')[2]

        # get table headers
        headers = []
        for val in table.find_all('th'):
            headers.append(val.string.strip())

        # get table data
        rows = []
        row = []
        for val in table.find_all('td'): 
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
        df = _transform_career_stats(df)
        return df
    except: 
        print('no records found')
        return pd.DataFrame()

def _format_names(original):
    """
    A helper function to turn names from "Last, First" to "First Last"
    
    Args: 
        original (str): the name to reformat. must be in form "Last, First"
    Returns: 
        str of format "First Last"
        
    Examples: 
        _format_names("Blumenfeld, Nathan")
        >>> "Nathan Blumenfeld"
    """
    try: 
        split = original.split(',')
        split.reverse()
        res = ' '.join(split).strip().title()
    except:
        res = np.nan
    return res

def _eliminate_dashes(df):
    """
    A helper function to replace the weird dashes the NCAA uses with 0.00

    Args: 
        df (DataFrame): 
    Returns:
        Dataframe with dashes replaced by 0.00 (not a copy!)
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

def _parse_season(year):
    """
    """
    last_two = year[5:]
    res = 2000 + int(last_two)
    return res

def _transform_career_stats(df):
    """
    A helper function to transform raw data loaded from ncaa with 
    get_career_stats().
    
    Args: 
        DataFrame output from get_career_stats function 
    
    Returns: 
        DataFrame indexed by stats_player_seq, season_id
    """
    df = _eliminate_dashes(df)
    df.fillna(value = 0.00, inplace=True)
    cols = df.columns
    if 'Player' in cols: 
        df = df.rename(columns={'Player':'name'})
    if 'name' in cols:     
        df.name = df.name.apply(_format_names)
    if 'stats_player_seq' in cols: 
        df.stats_player_seq = df.stats_player_seq.astype('string')
        df.stats_player_seq = df.stats_player_seq.str.replace(r'\D+', '')
    if 'Year' in cols: 
        df['Year'] = df['Year'].astype('string')
        df['season'] = df.Year.apply(_parse_season)
        df = df.drop(columns=['Year'])
        cols = df.columns
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
    if 'SlgPct' in cols: 
        df['SlgPct'] = df['SlgPct'].astype('float64')
    if 'OPP DP' in cols: 
        df['OPP DP'] = df['OPP DP'].astype('int64')
    if 'Pitches' in cols: 
        try: 
            df['Pitches'] = df['Pitches'].str.replace(',','')
            df['Pitches'] = df['Pitches'].astype('float64')
        except: 
            df['Pitches'] = df['Pitches'].astype('float64')
    if 'App' in cols: 
        df['App'] = df['App'].astype('int64')
    if 'SHO' in cols: 
        df['SHO'] = df['SHO'].astype('int64')
    if 'Bk' in cols: 
        df['Bk'] = df['Bk'].astype('int64')
    if 'WP' in cols: 
        df['WP'] = df['WP'].astype('int64')
    if 'ER' in cols: 
        df['ER'] = df['ER'].astype('int64')
    if 'SV' in cols: 
        df['SV'] = df['SV'].astype('int64')
    if 'ERA' in cols: 
        df['ERA'] = df['ERA'].astype('float64')
    if 'Inh Run' in cols: 
        df['Inh Run'] = df['Inh Run'].astype('int64')
    if 'GO' in cols: 
        df['GO'] = df['GO'].astype('int64')
    if 'FO' in cols: 
        df['FO'] = df['FO'].astype('int64')
    if 'W' in cols: 
        df['W'] = df['W'].astype('int64')
    if 'L' in cols: 
        df['L'] = df['L'].astype('int64')
    if 'KL' in cols: 
        df['KL'] = df['KL'].astype('int64')
    if 'Inh Run Score' in cols: 
        df['Inh Run Score'] = df['Inh Run Score'].astype('int64')
    if 'pickoffs' in cols: 
        df['pickoffs'] = df['pickoffs'].astype('int64')
    return df

def get_team_stats(school, season, variant):
    """
    Obtains single-season batting/pithing stats totals for all players
    in a given team. 
    
    Args:
        school: schools (str) or NCAA school_id (int) 
        season: season (int, YYYY) or NCAA season_id (int)
        variant (str): the type of stats, either 'batting' or 'pitching'
    
    Returns: 
        DataFrame
        
    data from stats.ncaa.org
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
        season = season_ids[0]

    if variant == 'batting':
        stat_category_id = season_ids[1]
    elif variant == 'pitching':
        stat_category_id = season_ids[2]
       
    url = 'https://stats.ncaa.org/team/'+str(school_id)+'/stats'
    payload = {'game_sport_year_ctl_id':str(season_id), 'id':str(season_id),\
               'year_stat_category_id':str(stat_category_id)}
        
    try:
        r = requests.get(url, params=payload, headers=_HEADERS)
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
        res = _transform_team_stats(df, variant= variant)
        return res
    except: 
        print(f'''Could not find {season} {variant} stats for {school}''')
        return pd.DataFrame()

def _transform_team_stats(df, variant):
    """
    A helper function to transform raw data obtained with get_career_stats
    
    Args:
        df: DataFrame output from get_career_stats function 
        variant: type of stats to transform, either 'batting' or 'pitching'
    
    Returns:
        DataFrame indexed by stats_player_seq, season_id
    """
    df = _eliminate_dashes(df)
    df.fillna(value = 0.00, inplace = True)
    cols = df.columns
    if 'Player' in cols: 
        df.rename(columns={'Player':'name'}, inplace=True)
        cols = df.columns
    if 'name' in cols: 
        df['name'] = df['name'].apply(_format_names)
        df['name'] = df['name'].astype('string')
    if 'Pos' in cols: 
        df['Pos'] = df['Pos'].astype('string')
    if 'Yr' in cols: 
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
        cols = df.columns
    if 'season' in cols:
        df['season'] = df['season'].astype('int64')
    if 'Team' in cols:
        df = df.rename(columns={'Team':'school_id'}) 
        cols = df.columns
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
            df.drop(columns=['OBPct'], inplace=True)
        if 'BA' in cols: 
            df.drop(columns=['BA'], inplace=True)
        if 'SlgPct' in cols: 
            df.drop(columns=['SlgPct'], inplace=True)
    else:
        if 'ERA' in cols: 
            df['ERA'] = df['ERA'].astype('float64')
        if 'IP' in cols: 
            df['IP'] = df['IP'].astype('float64')       
        if 'GS' in cols: 
            df.drop(columns=['GS'], inplace=True)
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
    A lookup function for season/batting/pitching ids

    Args: 
        season (int, YYYY)

    Returns:
        tuple of (season_id, batting_id, pitching_id) for desired season
    """
    lu_df = pd.read_parquet(_SEASON_ID_LU_PATH)
    season_row = lu_df.loc[lu_df['season'] == season]
    season_id = season_row.values[0][1]
    batting_id = season_row.values[0][2]
    pitching_id = season_row.values[0][3]  
    return (season_id, batting_id, pitching_id)

def lookup_season_ids_reverse(season_id):
    """
    A lookup function that returns the season and batting/pitching ids
    for a given season
    
    Args: 
        season_id (int): NCAA season_id
    
    Returns:
        tuple of (season_id, batting_id, pitching_id) for desired season
    """
    lu_df = pd.read_parquet(_SEASON_ID_LU_PATH)
    season_row = lu_df.loc[lu_df['season_id'] == season_id]
    season = season_row['season'].values[0]
    batting_id = season_row['batting_id'].values[0]
    pitching_id = season_row['pitching_id'].values[0]
    return (season, batting_id, pitching_id)

def lookup_season_id(season):
    """
    A lookup function that returns the season_id for a given season
    
    Args: 
        season (int, YYYY)
        
    Returns: 
        season_id as an int
    """
    lu_df = pd.read_parquet(_SEASON_ID_LU_PATH)
    season_row = lu_df.loc[lu_df['season'] == season]
    season_id = season_row.values[0][1]
    return season_id

def lookup_seasons_played(stats_player_seq): 
    """
    A lookup function that gives the first and last seasons played by a
    given player
    
    Args: 
        stats_player_seq (int): NCAA player_id
   
    Returns: 
        tuple of ints: (debut season, most recent season)
    
    """
    df = pd.read_parquet(_PLAYER_HISTORY_LU_PATH)
    row = df.loc[df.stats_player_seq == stats_player_seq]
    return row['debut_season'].values[0], row['season_last'].values[0]
         
def lookup_school_id(school):
    """
    A school_id lookup function

    Args: 
        school (str): the name of the school
        
    Returns:
        school_id as int
    
    Examples: 
        lookup_school_id("cornell")
        >>> 167
    """
    df = pd.read_parquet(_SCHOOL_ID_LU_PATH)
    school_row = df.loc[(df.ncaa_name == school)]
    if len(school_row) == 0:
        # try to search against bd_name
        school_row = df.loc[(df.bd_name == school)]
    if len(school_row) == 0: 
        return f'''could not find school {school}'''
    else: 
        return int(school_row['school_id'].values[0])
        
def lookup_player_id(player_name, school):
    """
    A player_id lookup function
    
    Args: 
        player_name (str): name of player to lookup
        school: either the name of the school (str) of the player, 
        or the NCAA school_id (int) of the player's school
        
    Returns: 
        The player_id of the player as an int

    Examples: 
        lookup_player_id("Jake Gelof", "Virginia")
        >>> 
    
    """
    
    if type(school) is str: 
        school_id = lookup_school_id(school)
    else: 
        school_id = school_id
    
    df = pd.read_csv(_PLAYER_LU_PATH)
    player_row = df.loc[df.name == player_name.title()]
    player_row = player_row.loc[df.school_id == school_id]
        
    if len(player_row) == 0:
        return f'''could not find player {player_name}'''
    else: 
        return player_row['player_id'].values[0]
        

