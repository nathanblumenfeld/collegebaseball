"""
metrics

A module to calculate additional baseball statistics based on results
produced with ncaa_scraper module

created by Nathan Blumenfeld in Spring 2022
"""
from collegebaseball import datasets
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# filepath of D1 linear weights, provided by Robert Fray
_LW_FILEPATH = datasets.get_linear_weights_table()

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
    PA = (row['AB'] + row['BB'] + row['SF'] + row['SH'] + row['HBP']
          - row['IBB'])
    return PA


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
        return round(numerator / denominator, ROUND_TO)
    else:
        return 0.00


def calculate_woba_manual(plate_appearances, walks, hits_by_pitch, singles,
                          doubles, triples, homeruns, season):
    """
    Calculates wOBA

    Args:
        plate_appearances(int)
        walks(int)
        hits_by_pitch(int)
        singles(int)
        doubles(int)
        triples(int)
        homeruns(int)
        season(int)

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
        return round(numerator / denominator, ROUND_TO)
    else:
        return 0.00


def _calculate_wraa(row):
    """
    """
    if row['PA'] > 0:
        season_weights = load_season_weights(row['season'])
        return round(((row['wOBA'] - season_weights['wOBA'].values[0])
                     / season_weights['wOBAScale'].values[0])
                     * row['PA'], ROUND_TO)
    else:
        return 0.00


def calculate_wraa_manual(plate_appearances, woba,  season):
    """
    Calculates wRAA based on the following formula:
        wRAA = [(wOBA - leagueWOBA) / wOBAscale] * PA

    Args:
        plate_appearances(int)
        woba(float)
        season(int)

    Returns:
        The weighted runs created above average of a player as a float
    """
    if plate_appearances > 0:
        season_weights = load_season_weights(season)
        return round(((woba - season_weights['wOBA'].values[0])
                     / season_weights['wOBAScale'].values[0])
                     * plate_appearances, ROUND_TO)
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
        return round((((row['wOBA'] - season_weights['wOBA'].values[0])
                       / season_weights['wOBAScale'].values[0])
                      + season_weights['R/PA'].values[0])
                     * row['PA'], ROUND_TO)
    else:
        return 0.00


def calculate_wrc_manual(plate_appearances, woba, season):
    """
    Calculates wRC based on the following formula:
    wRC=[((wOBA - lgwOBA) / wOBAScale) + (lgR / PA))] * PA

    Args:
        plate_appearances(int)
        woba(float)
        season(int)

    Returns:
        The weighted runs created by a player as a float
    """
    if plate_appearances > 0:
        season_weights = load_season_weights(season)
        return round((((woba - season_weights['wOBA'].values[0])
                       / season_weights['wOBAScale'].values[0])
                      + season_weights['R/PA'].values[0])
                     * plate_appearances, ROUND_TO)
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
        df(DataFrame): the DataFrame to append additional stats to

    Returns:
        DataFrame of stats with additional columns
    """
    df.loc[:, 'PA'] = df.apply(_calculate_pa, axis=1)
    df = df.loc[df.PA > 0]
    df.loc[:, '1B'] = (df['H'] - df['2B'] - df['3B'] - df['HR'])
    df.loc[:, 'OBP'] = round((df['H'] + df['BB'] + df['IBB'] + df['HBP'])
                             / df['PA'], ROUND_TO)
    df.loc[:, 'BA'] = round(df['H'].divide(
        df['AB']).replace(np.inf, 0), ROUND_TO)
    df.loc[:, 'SLG'] = round((1 * df['1B'] + 2 * df['2B'] + 3 * df['3B']
                             + 4 * df['HR']) / df['AB'], ROUND_TO)
    df.loc[:, 'OPS'] = round(df['OBP'] + df['SLG'], ROUND_TO)
    df.loc[:, 'ISO'] = round(df['SLG'] - df['BA'], ROUND_TO)
    df.loc[:, 'HR/PA'] = round(df['HR'] / df['PA'], ROUND_TO)
    df.loc[:, 'K/PA'] = round(df['K'] / df['PA'], ROUND_TO)
    df.loc[:, 'BB/PA'] = round(df['BB'] / df['PA'], ROUND_TO)
    df.loc[:, 'K/BB'] = round((df['K'].divide(df['BB'])
                               ).replace(np.inf, 0.0), ROUND_TO)
    df.loc[:, 'BABIP'] = round((df['H'] - df['HR'])
                               / (df['AB'] - df['K'] - df['HR']
                               + df['SF']), ROUND_TO)
    df.loc[:, 'wOBA'] = df.apply(_calculate_woba, axis=1)
    df.loc[:, 'wRAA'] = df.apply(_calculate_wraa, axis=1)
    df.loc[:, 'wRC'] = df.apply(_calculate_wrc, axis=1)
    df = _set_metric_dtypes(df, 'batting')
    return df


def _adjust_innings_pitched(df):
    full_innings = int(df['IP'])
    partial_innings = df['IP'] % 1
    adj_innings = round(partial_innings*(10/3), ROUND_TO)
    return float(full_innings + adj_innings)


def _calculate_fip(row):
    """
    """
    if row['IP-adj'] > 0:
        season_weights = load_season_weights(row['season'])
        res = round(((13 * row['HR-A'] + 3 * (row['BB'] + row['HB']) - 2 *
                    row['SO']) / row['IP-adj'])
                    + season_weights['cFIP'].values[0], 3)
        return res
    else:
        return 0.00


def calculate_fip_manual(HR, BB, HB, K, IP, season):
    """
    Calculates FIP bases on the following formula:
        ((HR x 13) + (3 x(BB + HBP)) - (2 x K)) / IP + cFIP

    Args:
        HR(int)
        BB(int)
        HB(int)
        K(int)
        IP(float)
        season(int)

    Returns:
        FIP as a float
    """
    season_weights = load_season_weights(season)
    return round(((13 * HR + 3 * (BB + HB) - 2 * K) / IP)
                 + season_weights['cFIP'].values[0], ROUND_TO)


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
        df(DataFrame): the DataFrame to append additional stats to

    Returns:
        DataFrame of stats with additional columns
    """
    df['IP-adj'] = df.apply(_adjust_innings_pitched, axis=1)
    df['1B-A'] = df['H']-df['HR-A']-df['3B-A']-df['2B-A']
    df.loc[:, 'OBP-against'] = round((df['H'] + df['BB'] + df['IBB']
                                     + df['HB']) /
                                     (df['BF']).replace(np.inf, 0), ROUND_TO)
    df.loc[:, 'BA-against'] = round(df['H'] /
                                    (df['BF'] - df['BB']
                                     - df['SFA'] - df['SHA']
                                     - df['HB'] + df['IBB']), ROUND_TO)
    df.loc[:, 'SLG-against'] = round((1 * df['1B-A']
                                     + 2 * df['2B-A']
                                     + 3 * df['3B-A']
                                     + 4 * df['HR-A'])
                                     / (df['BF'] - df['BB'] - df['SFA']
                                     - df['SHA'] - df['HB']
                                     + df['IBB']), ROUND_TO)
    df.loc[:, 'OPS-against'] = round(df['OBP-against'] +
                                     df['SLG-against'], ROUND_TO)
    df.loc[:, 'K/PA'] = round(df['SO']
                              / (df['BF']).replace(np.inf, 0), ROUND_TO)
    df.loc[:, 'K/9'] = round((9 * df['SO']
                              / (df['IP-adj']).replace(np.inf, 0)), ROUND_TO)
    df.loc[:, 'BB/PA'] = round(df['BB']
                               / (df['BF']).replace(np.inf, 0), ROUND_TO)
    df.loc[:, 'BB/9'] = round(9 * (df['BB'] /
                              (df['IP-adj']).replace(np.inf, 0)), ROUND_TO)
    df.loc[:, 'BABIP-against'] = round((df['H'] - df['HR-A'])
                                       / (df['BF'] - df['SO']
                                       - df['HR-A'] + df['SFA']), ROUND_TO)
    df.loc[:, 'FIP'] = df.apply(_calculate_fip, axis=1)
    df.loc[:, 'WHIP'] = round(
        ((df['H']+df['BB']) / (df['IP-adj'])).replace(np.inf, 0), ROUND_TO)
    df.loc[:, 'Pitches/IP'] = round(
        (df['pitches'] / (df['IP-adj'])).replace(np.inf, 0), ROUND_TO)
    if 'App' in df.columns:
        df.loc[:, 'IP/App'] = round((df['IP-adj'].divide(df['App'])
                                     ).replace(np.inf, 0), ROUND_TO)
        df.loc[:, 'Pitches/App'] = round(
            (df['pitches'].divide(df['App'])).replace(np.inf, 0), ROUND_TO)
        if len(df.loc[df['Pitches/App'] > 0]) < 1:
            df = df.drop(columns=['Pitches/App'], inplace=False)
    df.loc[:, 'Pitches/PA'] = round((df['pitches'] / (df['BF'])
                                     ).replace(np.inf, 0), ROUND_TO)
    df.loc[:, 'HR-A/PA'] = round((df['HR-A'] / (df['BF'])
                                  ).replace(np.inf, 0), ROUND_TO)
    df.loc[:, 'GO/FO'] = round((df['GO'] / (df['FO'])
                                ).replace(np.inf, 0), ROUND_TO)
    if len(df.loc[df['Pitches/IP'] > 0]) < 1:
        df = df.drop(columns=['Pitches/IP'], inplace=False)
    if len(df.loc[df['Pitches/PA'] > 0]) < 1:
        df = df.drop(columns=['Pitches/PA'], inplace=False)
    df = _set_metric_dtypes(df, 'pitching')

    return df


def _set_metric_dtypes(df, variant):
    """
    """
    cols = df.columns
    if variant == 'pitching':
        data_types = {
            'float32': ['WHIP', 'FIP', 'IP/App', 'Pitches/IP', 'Pitches/App',
                        'HR-A/PA', 'Pitches/PA', 'BABIP-against', 'BB/9',
                        'BB/PA', 'K/9', 'K/PA', 'OPS-against', 'OBP-against',
                        'IP-adj'],
            'int32': ['1B-A']
        }
    elif variant == 'batting':
        data_types = {
            'float32': ['OBP', 'BA', 'SLG', 'OPS', 'HR/PA', 'K/PA', 'BB/PA',
                        'BABIP', 'wOBA', 'wRAA', 'wRC'],
            'int16': ['PA', '1B']
        }

    for i in data_types.keys():
        for j in data_types[i]:
            if j in cols:
                df[j] = df[j].astype(i)

    df.fillna(value=0.00, inplace=True)
    return df
