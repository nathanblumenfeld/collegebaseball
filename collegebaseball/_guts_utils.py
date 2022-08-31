"""
guts_utils.py

A module to faciliate the updating of collegebaseball's guts

created by Nathan Blumenfeld in Summer 2022
"""
import pandas as pd
from collegebaseball import download_utils, guts


def _update_season_ids(season, season_id, batting_id, pitching_id):
    """A function to update and save the season_id lookup table

    Args:
        season (int)
        season_id (int)
        batting_id (int)
        pitching_id (int)
    """
    old = pd.read_parquet(guts.get_season_lu_path())
    new = pd.DataFrame(
        [[season, season_id, batting_id, pitching_id]], columns=old.columns)
    updated_df = pd.concat([old, new]).reset_index(drop=True)
    updated_df.to_parquet(guts.get_season_lu_path())
    return updated_df


def _update_rosters(season, division):
    """
    overwrites current seasons rosters with fresh scrape
    """
    df = pd.read_parquet(guts.get_rosters_path())
    old = df.loc[df.season != season]
    new, failures = download_utils.download_season_rosters(season, division)
    print(failures)
    res = pd.concat([new, old])
    res.to_parquet(guts.get_rosters_path(), index=False)


def _remove_school(school):
    df = pd.read_parquet('collegebaseball/data/schools.parquet')
    df = df.loc[df.ncaa_name != school]
    df.to_csv('collegebaseball/data/schools.csv', index=False)
    df.to_parquet('collegebaseball/data/schools.parquet', index=False)
