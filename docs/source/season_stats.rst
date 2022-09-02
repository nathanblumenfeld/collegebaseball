============
Season Stats
============


Player-level Season Stats
-------------------------
.. py:function:: ncaa_scraper.ncaa_team_stats(school, season, variant, include_advanced=True, split=None)

    Obtains player-level single-season aggregate stats for all players from a given school, from stats.ncaa.org

   :school: ncaa_name (str) or school_id (int)
   :season: season (int, YYYY) or NCAA season_id (int), valid 2013-2022
   :variant (str): 'batting', 'pitching', or 'fielding'
   :include_advanced (bool, optional): whether to
      automatically calcuate advanced metrics, Defaults to True
   :split (str, optional): 'vs_LH', 'vs_RH', 'runners_on', 'bases_empty',
        'bases_loaded', 'with_RISP', 'two_outs'
   :return (pd.DataFrame):

.. py:function:: ncaa_scraper.ncaa_career_stats(school, season, variant, include_advanced=True)

    Obtains season-aggregate stats for all seasons in a given player's collegiate career, from stats.ncaa.org 

   :school: ncaa_name (str) or school_id (int)
   :season: season (int, YYYY) or NCAA season_id (int), valid 2013-2022
   :variant (str): 'batting', 'pitching', or 'fielding'
   :include_advanced (bool, optional): whether to
      automatically calcuate advanced metrics, Defaults to True
   :return (pd.DataFrame):



Team-level Season Stats
-----------------------
.. py:function:: ncaa_scraper.ncaa_team_totals(school, season, variant, include_advanced=True, split=None)

   Obtains team-level aggregate single-season stats for a given team, from stats.ncaa.org

   :school: ncaa_name (str) or school_id (int)
   :season: season (int, YYYY) or NCAA season_id (int), valid 2013-2022
   :variant (str): 'batting', 'pitching', or 'fielding'
   :include_advanced (bool, optional): whether to
      automatically calcuate advanced metrics, Defaults to True
   :split (str, optional): 'vs_LH', 'vs_RH', 'runners_on', 'bases_empty',
        'bases_loaded', 'with_RISP', 'two_outs'
   :return (pd.DataFrame):
