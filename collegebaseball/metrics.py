"""
metrics 

A module to calculate additional baseball statistics based on results
produced with ncaa_scraper module

created by Nathan Blumenfeld in Spring 2022
"""
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# filepath of D1 linear weights, provided by Robert Fray
_LW_FILEPATH = 'collegebaseball/data/d1_linear_weights.parquet'

# number of decimal places to round floats to 
ROUND_TO = 3

def load_linear_weights():
    """    
    Returns: 
        Dataframe with the following columns: 
            wOBA: lgwOBA (calculated from totals)
            wOBAScale
            wBB
            wHB
            w1B
            w2B
            w3B
            wHR
            R/PA
            RPG
            cFIP
            season (primary key)     
            
    Linear Weights provided by Robert Fray
    n.b. 2022 Season calculated as avg. of previous 5 seasons
    """
    res = pd.read_parquet(_LW_FILEPATH)
    return res

# save this as a global to make things faster
_WEIGHTS_DF = load_linear_weights()

def load_season_weights(season):
    """
    Args:
        season (int, YYYY): valid 2013-2022
    
    Returns: 
        Single-row Dataframe with the following columns: 
            wOBA: lgwOBA (calculated from totals)
            wOBAScale
            wBB
            wHB
            w1B
            w2B
            w3B
            wHR
            R/PA
            RPG
            cFIP
            season (primary key)
    """
    _WEIGHTS_DF = load_linear_weights()
    res = _WEIGHTS_DF.loc[_WEIGHTS_DF['season'] == int(season)]
    return res

def _calculate_pa(row):
    """    
    Returns:
        The number of plate appearances by a player as an int
        based on the following formula: 
    
        PA = AB + BB + SF + SH + HBP - IBB
        
    """    
    PA = (row['AB'] + row['BB'] + row['SF'] + row['SH'] + row['HBP'] \
          - row['IBB'])
    return PA

def _calculate_singles(row):
    """    
    Returns:
        The number of singles by a player as an int 
        based on the following formula: 

        1B = H - 2B - 3B - HR
    """
    singles = row['H'] - row['2B'] - row['3B'] - row['HR']
    return singles  

def _calculate_woba(row):
    """            
    """
    if row['PA'] > 0:
        season_weights = load_season_weights(row['season'])
        numerator = (season_weights['wBB'].values[0] * row['BB']
          + season_weights['wHBP'].values[0] * row['HBP']
          + season_weights['w1B'].values[0] * row['1B']
          + season_weights['w2B'].values[0] * row['2B']          
          + season_weights['w3B'].values[0] * row['3B']
          + season_weights['wHR'].values[0] * row['HR'])
        denominator = row['PA'] 
        return round(numerator / denominator, 3)
    else:
        return 0.00
    
def calculate_woba_manual(plate_appearances, walks, hits_by_pitch, singles, \
                          doubles, triples, homeruns, season):
    """
    Calculates wOBA according to the following formula: 
    
    [(wBB * walks) + (wHBP * hits_by_pitch) + (w1B * singles) + (w2B * doubles) \
    (w3B * triples) + (wHR * homeruns)] / plate_appearances
        
    Args: 
        plate_appearances (int)
        walks (int)
        hits_by_pitch (int)
        singles (int)
        doubles (int)
        triples (int)
        homeruns (int)
        season (int) 
        
    Returns: 
        wOBA as a float
    """
    if plate_appearances > 0:
        season_weights = load_season_weights(season)
        numerator = (season_weights['wBB'].values[0] * walks
          + season_weights['wHBP'].values[0] * hits_by_pitch
          + season_weights['w1B'].values[0] * singles
          + season_weights['w2B'].values[0] * doubles          
          + season_weights['w3B'].values[0] * triples
          + season_weights['wHR'].values[0] * homeruns)
        denominator = plate_appearances 
        return round(numerator / denominator, 3)
    else:
        return 0.00
    
def _calculate_wraa(row):
    """
    """
    # get linear weights for given season
    if row['PA'] > 0:
        season_weights = load_season_weights(row['season'])
        return round(((row['wOBA'] - season_weights['wOBA'].values[0]) \
                      / season_weights['wOBAScale'].values[0]) * row['PA'], 3)
    else: 
        return 0.00       
        
def calculate_wraa_manual(plate_appearances, woba,  season):
    """
    Calculates wRAA based on the following formula: 
        wRAA = [(wOBA − leagueWOBA) / wOBAscale] ∗ PA

    Args: 
        plate_appearances (int)
        woba (float)
        season (int)
    
    Returns:
        The weighted runs created above average of a player as a float 
    """
    # get linear weights for given season
    if plate_appearances > 0:
        season_weights = load_season_weights(season)
        return round(((woba - season_weights['wOBA'].values[0]) \
                      / season_weights['wOBAScale'].values[0]) \
                     * plate_appearances, 3)
    else: 
        return 0.00

def _calculate_wrc(row):
    """
    Args:
    
    Returns:
        The weighted runs created of a player as a float
        based on the following formula: 

        wRC = [((wOBA - lgwOBA) / wOBAScale) + (lgR / PA))] * PA
    """
    if row['PA'] > 0:
        season_weights = load_season_weights(row['season'])
        return round((((row['wOBA'] - season_weights['wOBA'].values[0]) \
                       / season_weights['wOBAScale'].values[0]) \
                      + season_weights['R/PA'].values[0]) * row['PA'], 3)
    else: 
        return 0.00
    
def calculate_wrc_manual(plate_appearances, woba, season):
    """
    Calculates wRC based on the following formula: 
    wRC = [((wOBA - lgwOBA) / wOBAScale) + (lgR / PA))] * PA

    Args: 
        plate_appearances (int)
        woba (float)
        season (int)
    
    Returns: 
        The weighted runs created by a player as a float 
    """
    if plate_appearances > 0:
        season_weights = load_season_weights(season)
        return round((((woba - season_weights['wOBA'].values[0]) \
                       / season_weights['wOBAScale'].values[0]) \
                      + season_weights['R/PA'].values[0]) \
                     * plate_appearances, 3)
    else: 
        return 0.00
    
def add_batting_metrics(df):
    """
    Adds the following columns to a given DataFrame:
    
        PA 
        1B 
        OBP
        BA
        SLG
        OPS
        ISO
        PA/HR
        K
        BB
        BABIP
        wOBA
        wRAA
        wRC
    
    Args: 
        df (DataFrame): the DataFrame to append additional stats to
        
    Returns: 
        DataFrame of stats with additional columns
    """
    df.loc[:, 'PA'] = df.apply(_calculate_pa, axis=1)
    df = df.loc[df.PA > 0]
    try: 
        df.loc[:, '1B'] = df.apply(_calculate_singles, axis=1)
        df.loc[:, 'OBP'] = round((df['H'] + df['BB'] + df['IBB'] + df['HBP']) \
                                 /df['PA'], ROUND_TO)
        df.loc[:, 'BA'] = round(df['H'] / df['AB'], ROUND_TO)
        df.loc[:, 'SLG'] = round((1 *df['1B'] + 2 * df['2B']+ 3 * df['3B'] \
                                  + 4 * df['HR']) /df['AB'], ROUND_TO)
        df.loc[:, 'OPS'] = round(df['OBP'] + df['SLG'], ROUND_TO)
        df.loc[:, 'ISO'] = round(df['SLG'] - df['BA'], ROUND_TO)
        df.loc[:, 'HR%'] = round(df['HR'] / df['PA'], ROUND_TO)
        df.loc[:, 'K%'] = round(df['K'] / df['PA'], ROUND_TO)
        # df.loc[:, 'K%'] = round(df['K'] / df['PA'], ROUND_TO)*100
        df.loc[:, 'BB%'] = round(df['BB'] / df['PA'], ROUND_TO)
        # df.loc[:, 'BB%'] = round(df['BB'] / df['PA'], ROUND_TO)*100
        df.loc[:, 'BABIP'] = round((df['H'] - df['HR']) \
                                   / (df['AB'] - df['K'] - df['HR'] \
                                      + df['SF']), ROUND_TO)
        df.loc[:, 'wOBA'] = df.apply(_calculate_woba, axis=1)
        df.loc[:, 'wRAA'] = df.apply(_calculate_wraa, axis=1)
        df.loc[:, 'wRC'] = df.apply(_calculate_wrc, axis=1)
        return df.sort_values(by='wOBA', ascending=False)
    except: 
        print('no records found')
        return pd.DataFrame()

def _adjust_innings_pitched(row):
    """
    Examples: 
        .1 --> 1/3
        .2 --> 2/3 
    """
    as_list = str(row['IP']).split('.')
    if len(as_list) < 2: 
        return row['IP']
    else:
        as_list[-1] = str(int(as_list[-1]) * (1 / 3)).split('.')[-1]
        res = round(float('.'.join(map(str, as_list))), 4)
        return res
    
def _calculate_fip(row):
    """
    """
    if row['App'] > 0:
        season_weights = load_season_weights(row['season'])
        res = round(((13 * row['HR-A'] + 3 * (row['BB'] + row['HB']) - 2 * row['SO']) / row['IP-adj']) + season_weights['cFIP'].values[0], 3)
        return res
    else: 
        print('no records found')
        return 0.00
    
def calculate_fip_manual(HR, BB, HP, K, IP):
    """
    Calculates FIP bases on the following formula: 
        ((HR x 13) + (3 x (BB + HBP)) - (2 x K)) / IP + cFIP

    Args: 
        HR (int)
        BB (int)
        HBP (int)
        K (int)
        IP (float)
        season (int)
        
    Returns: 
        FIP as a float  
    """
    if row['App'] > 0:
        season_weights = load_season_weights(row['season'])
        return round(((13 * row['HR-A'] + 3 * (row['BB'] + row['HB']) - 2 * row['SO']) / row['IP-adj']) + season_weights['cFIP'].values[0], ROUND_TO)
    else: 
        print('no records found')
        return 0.00
       
    
def add_pitching_metrics(df):
    """
    Adds the following columns to a given DataFrame:
        PA 
        1B-A
        OBP-against
        BA-against
        SLG-against
        OPS-against
        K/PA
        K/9
        BB/PA
        BB/9
        BABIP-against
        FIP
        ERA
        WHIP
        IP/App
        PA/HR
        Pitches/PA
        
    Args: 
        df (DataFrame): the DataFrame to append additional stats to
        
    Returns: 
        DataFrame of stats with additional columns
    """
    df = df.loc[df.IP > 0]
    df = df.loc[df.season >= 2013]
    try:
        df.loc[:, 'IP-adj'] = df.apply(_adjust_innings_pitched, axis = 1)
        df.loc[:, '1B-A'] = df['H']-df['HR-A']-df['3B-A']-df['2B-A']
        df.loc[:, 'OBP-against'] = round((df['H'] + df['BB'] + df['IBB'] \
                                          + df['HB']) / df['BF'], ROUND_TO)
        df.loc[:, 'BA-against'] = round(df['H'] / (df['BF'] - df['BB'] \
                                                   - df['SFA'] - df['SHA'] \
                                                   - df['HB'] + df['IBB']), \
                                        ROUND_TO)
        df.loc[:, 'SLG-against'] = round((1 * df['1B-A'] \
                                          + 2 * df['2B-A'] \
                                          + 3 * df['3B-A'] \
                                          + 4 * df['HR-A']) \
                                         / (df['BF'] - df['BB'] - df['SFA'] \
                                            - df['SHA'] - df['HB'] \
                                            + df['IBB']),
                                         ROUND_TO)
        df.loc[:, 'OPS-against'] = round(df['OBP-against'] \
                                         + df['SLG-against'], ROUND_TO)
        df.loc[:, 'K/PA'] = round(df['SO'] / df['BF'], ROUND_TO)
        df.loc[:, 'K/9'] = round((9 * df['SO'] / df['IP-adj']), ROUND_TO)
        df.loc[:, 'BB/PA'] = round(df['BB'] / df['BF'], ROUND_TO)                 
        df.loc[:, 'BB/9'] = round(9* df['BB'] / df['IP-adj'], ROUND_TO)
        df.loc[:, 'BABIP-against'] = round((df['H'] - df['HR-A']) \
                                           / (df['BF'] - df['SO'] \
                                              - df['HR-A'] + df['SFA']), \
                                           ROUND_TO)
        df.loc[:, 'FIP'] = df.apply(_calculate_fip, axis=1)
        df.loc[:, 'WHIP'] = round((df['H']+df['BB']) / df['IP-adj'], ROUND_TO)
        df.loc[:, 'IP/App'] = round(df['IP-adj'] / df['App'], ROUND_TO)
        df.loc[:, 'Pitches/IP'] = round(df['Pitches'] / df['IP-adj'], ROUND_TO)
        df.loc[:, 'HR-A/PA'] = round(df['HR-A'] / df['BF'], ROUND_TO)
        df.loc[:, 'Pitches/PA'] = round(df['Pitches'] / df['BF'], ROUND_TO)
        return df.sort_values(by='FIP', ascending=True)
    except: 
        print('no records found')
        return pd.DataFrame()
        
    
