"""
A module to calculate additional baseball statistics based on results produced with ncaa_scrape module

created by Nathan Blumenfeld
"""
import pandas as pd
import numpy as np
import ncaa_scrape
import warnings

warnings.filterwarnings("ignore")

# filepath of D1 linear weights
LW_FILEPATH = 'data/d1_linear_weights.parquet' # <-- Robert Fray's Linear Weights

# number of decimal places to round floats to 
ROUND_TO = 3

def load_linear_weights(lw_filepath=LW_FILEPATH):
    """
    Returns: Linear Weights DataFrame
    """
    res = pd.read_parquet(lw_filepath)
    return res

WEIGHTS_DF = load_linear_weights()


def calculate_pa(player_row):
    """
    Returns (int): the number of plate appearances by a given player based on the following formula: 
    
    PA = AB + BB + SF + SH + HBP - IBB
    """    
    PA = player_row['AB'] + player_row['BB'] + player_row['SF'] + player_row['SH'] + player_row['HBP'] - player_row['IBB']
    return PA

def calculate_singles(player_row):
    """
    Returns (int): the number of singles by a given player based on the following formula: 

    1B = H - 2B - 3B - HR
    """
    singles = player_row['H'] - player_row['2B'] - player_row['3B'] - player_row['HR']
    return singles


def load_season_weights(season, weights_df = WEIGHTS_DF):
    """
    takes either season or season_id
    """
    input_length = len(str(season))
    if input_length == 4: 
        # season is YYYY
        return weights_df.loc[weights_df['season'] == int(season)]
    else: # season is season_id:
        return weights_df.loc[weights_df['seasonID'] == int(season)]       
        
def calculate_pa_new(AB, BB, SF, SH, HBP, IBB):
    """
    Returns (int): the number of plate appearances by a given player based on the following formula: 
    
    PA = AB + BB + SF + SH + HBP - IBB
    """    
    PA = player_row['AB'] + player_row['BB'] + player_row['SF'] + player_row['SH'] + player_row['HBP'] - player_row['IBB']
    return PA        
        
def calculate_woba_new(plate_appearances, walks, hits_by_pitch, singles, doubles, triples, homeruns, season):
    """
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
    
def calculate_woba_new_1(row):
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
       
def calculate_wraa_new(plate_appearances, woba,  season):
    """
    Returns (float): the weighted runs created above average of a given player based on the following formula: 

    wRAA = [(wOBA − leagueWOBA) / wOBAscale] ∗ PA
    """
    # get linear weights for given season
    if plate_appearances > 0:
        season_weights = load_season_weights(season)
        return round(((woba - season_weights['wOBA'].values[0]) / season_weights['wOBAScale'].values[0]) * plate_appearances, 3)
    else: 
        return 0.00

def calculate_wraa_new_1(row):
    """
    Returns (float): the weighted runs created above average of a given player based on the following formula: 

    wRAA = [(wOBA − leagueWOBA) / wOBAscale] ∗ PA
    """
    # get linear weights for given season
    if row['PA'] > 0:
        season_weights = load_season_weights(row['season'])
        return round(((row['wOBA'] - season_weights['wOBA'].values[0]) / season_weights['wOBAScale'].values[0]) * row['PA'], 3)
    else: 
        return 0.00

def calculate_wrc_new(plate_appearances, woba, season):
    """
    Returns (float): the weighted runs created of a given player based on the following formula: 

    wRC = [((wOBA - lgwOBA) / wOBAScale) + (lgR / PA))] * PA
    """
    if plate_appearances > 0:
        season_weights = load_season_weights(season)
        return round((((woba - season_weights['wOBA'].values[0]) / season_weights['wOBAScale'].values[0]) + season_weights['R/PA'].values[0]) * plate_appearances, 3)
    else: 
        return 0.00
    
def calculate_wrc_new_1(row):
    """
    Returns (float): the weighted runs created of a given player based on the following formula: 

    wRC = [((wOBA - lgwOBA) / wOBAScale) + (lgR / PA))] * PA
    """
    if row['PA'] > 0:
        season_weights = load_season_weights(row['season'])
        return round((((row['wOBA'] - season_weights['wOBA'].values[0]) / season_weights['wOBAScale'].values[0]) + season_weights['R/PA'].values[0]) * row['PA'], 3)
    else: 
        return 0.00
    
def calculate_fip(row):
    """
    Where the "FIP constant" puts FIP onto the same scale as the entire league's ERA: ((HR x 13) + (3 x (BB + HBP)) - (2 x K)) / IP + FIP constant.


    """
    if row['App'] > 0:
        season_weights = load_season_weights(row['season'])
        
        return round(((13 * row['HR-A'] 
                    + 3 * (row['BB'] + row['HB'])
                    - 2 * row['SO']) / row['IP']) + season_weights['cFIP'].values[0], 3)
    else: 
        print('no records found')
        return 0.00
    

def add_batting_metrics(df, lw_filepath = LW_FILEPATH):
    """
    Adds the following columns to a given DataFrame:
  
    PA 
    1B 
    OBP
    BA
    SLG
    OPS
    ISO
    K
    BB
    BABIP
    wOBA
    wRAA
    wRC
    """
    df.loc[:, 'PA'] = df.apply(calculate_pa, axis = 1)
    df = df.loc[df.PA > 0]
    try: 
        df.loc[:, '1B'] = df.apply(calculate_singles, axis = 1)
        df.loc[:, 'OBP'] = round((df['H']+df['BB'] + df['IBB'] + df['HBP'])/df['PA'], ROUND_TO)
        df.loc[:, 'BA'] = round(df['H'] / df['AB'], ROUND_TO)
        df.loc[:, 'SLG'] = round((1*df['1B']+2*df['2B']+3*df['3B']+4*df['HR'])/df['AB'], ROUND_TO)
        df.loc[:, 'OPS'] = round(df['OBP'] + df['SLG'], ROUND_TO)
        df.loc[:, 'ISO'] = round(df['SLG'] - df['BA'], ROUND_TO)
        df.loc[:, 'K%'] = round(df['K'] / df['PA'], ROUND_TO)
        df.loc[:, 'BB%'] = round(df['BB'] / df['PA'], ROUND_TO)
        df.loc[:, 'BABIP'] = round((df['H']-df['HR'])/(df['AB']-df['K']-df['HR']+df['SF']), ROUND_TO)
        df.loc[:, 'wOBA'] = df.apply(calculate_woba_new_1, axis = 1)
        # df.loc[:, 'wOBA'] = df.apply(lambda row: calculate_woba_new_1(row['PA'], row['BB'], row['HBP'], row['1B'], row['2B'], row['3B'], row['HR'], row['season']), axis = 1)
        df.loc[:, 'wRAA'] = df.apply(calculate_wraa_new_1, axis = 1)
        # df.loc[:, 'wRAA'] = df.apply(lambda row: calculate_wraa_new(row['PA'], row['wOBA'], row['season']), axis = 1)
        df.loc[:, 'wRC'] = df.apply(calculate_wrc_new_1, axis = 1)
    #     df.loc[:, 'wRC'] = df.apply(lambda row: calculate_wrc_new(row['PA'], row['wOBA'], row['season']), axis = 1)
        return df.sort_values(by='wOBA', ascending=False)
    except: 
        print('no records found')
        return pd.DataFrame()

def adjust_innings_pitched(row):
    """
    .1 --> 1/3
    .2 --> 2/3 
    """
    split_at_decimal = str(row['IP']).split('.')
    if len(split_at_decimal) < 2: 
        return row['IP']
    else:
        split_at_decimal[-1] = str(int(split_at_decimal[-1])*(1/3)).split('.')[-1]
        res = round(float('.'.join(map(str, split_at_decimal))), 4)
        return res

def add_pitching_metrics(df, lw_filepath = LW_FILEPATH):
    """
    Adds the following columns to a given DataFrame:
  
    PA 
    1B 
    OBP
    BA:    PA - BB - SF - SH - HBP + IBB = AB 

    SLG
    OPS
    ISO
    K
    BB
    BABIP
    """
    df = df.loc[df.IP > 0]
    df.loc[:, 'IP-adj'] = df.apply(adjust_innings_pitched, axis = 1)
    df.loc[:, '1B-A'] = df['H']-df['HR-A']-df['3B-A']-df['2B-A']
    df.loc[:, 'OBP-against'] = round((df['H'] + df['BB'] + df['IBB'] + df['HB']) / df['BF'], ROUND_TO)
    df.loc[:, 'BA-against'] = round(df['H'] / (df['BF'] - df['BB'] - df['SFA'] - df['SHA'] - df['HB'] + df['IBB']), ROUND_TO)
    df.loc[:, 'SLG-against'] = round((1*df['1B-A']+2*df['2B-A']+3*df['3B-A']+4*df['HR-A']) / (df['BF'] - df['BB'] - df['SFA'] - df['SHA'] - df['HB'] + df['IBB']), ROUND_TO)
    df.loc[:, 'OPS-against'] = round(df['OBP-against'] + df['SLG-against'], ROUND_TO)
    df.loc[:, 'K/PA'] = round(df['SO'] / df['BF'], ROUND_TO)
    df.loc[:, 'K/9'] = round((9 * df['SO'] / df['IP-adj']), ROUND_TO)
    df.loc[:, 'BB/PA'] = round(df['BB'] / df['BF'], ROUND_TO)                 
    df.loc[:, 'BB/9'] = round(9* df['BB'] / df['BF'], ROUND_TO)
    df.loc[:, 'BABIP-against'] = round((df['H']-df['HR-A'])/(df['BF']-df['SO']-df['HR-A']+df['SFA']), ROUND_TO)
    df.loc[:, 'FIP'] = df.apply(calculate_fip, axis=1)
    return df.sort_values(by='FIP', ascending=True)
#     except: 
#         print('no records found')
#         return pd.DataFrame()
        
    

def calculate_woba(player_row, weights_df=WEIGHTS_DF, round_to = ROUND_TO):
    """
    Returns (float): the weighted on base average of a given player based on the following formula: 

    wOBA = (wBB×uBB + wHBP×HBP + w1B×1B + w2B×2B + w3B×3B +
    wHR×HR) / PA
    """
    # get linear weights for given season
    season = player_row['season']
    print(season)
    weights_row = weights_df.loc[weights_df['season'] == season]
    # to avoid div by zero
    if not player_row['PA'] == 0:
        woba = ((weights_row['wBB'] * player_row['BB']
                + weights_row['wHBP'] * player_row['HBP']
                + weights_row['w1B'] * player_row['1B']
                + weights_row['w2B'] * player_row['2B']
                + weights_row['w3B'] * player_row['3B']
                + weights_row['wHR'] * player_row['HR'])
                / player_row['PA'])
        try:
            woba = woba.iloc[0]
        except:
            woba = 0.00
    else: 
        woba = 0.00
    return round(woba, round_to)

def calculate_wraa(player_row, weights_df=WEIGHTS_DF, round_to = ROUND_TO):
    """
    Returns (float): the weighted runs created above average of a given player based on the following formula: 

    wRAA = [(wOBA − leagueWOBA) / wOBAscale] ∗ PA
    """
    # get linear weights for given season
    weights_row = weights_df.loc[weights_df.season == player_row['season']]
    if not player_row['PA'] == 0:
        wraa = (((player_row['wOBA'] - weights_row['wOBA']) / weights_row['wOBAScale']) * player_row['PA'])
        try:
            wraa = wraa.iloc[0]
        except: 
            wraa = 0.00
    else:
        wraa = 0.00
    return round(wraa, round_to)

def calculate_wrc(player_row, weights_df=WEIGHTS_DF, round_to = ROUND_TO):
    """
    Returns (float): the weighted runs created of a given player based on the following formula: 

    wRC = [((wOBA - lgwOBA) / wOBAScale) + (lgR / PA))] * PA
    """
    weights_row = weights_df.loc[weights_df.season == player_row['season']]
    if not player_row['PA'] == 0:
        wrc = (((player_row['wOBA'] - weights_row['wOBA']) / weights_row['wOBAScale']) + weights_row['R/PA']) * player_row['PA']
        try:
            wrc = wrc.iloc[0]
        except:
            wrc = 0.00
    else: 
        wrc = 0.00
    return round(wrc, round_to)