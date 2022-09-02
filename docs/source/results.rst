Game Results
============


Team Results
------------
.. py:function:: ncaa_scraper.ncaa_team_results(school, season):
   
   Obtains the results of games for a given school in a given season, from stats.ncaa.org

   :school: school name (str) or NCAA school_id (int)
   :season: season (int, YYYY) or NCAA season_id (int), valid 2013-2022
   :return (pd.DataFrame):

.. py:function:: boydsworld_scraper.boydsworld_team_results(school, start, end=None, vs="all", parse_dates=True):

   A function to scrape Division I game results, from boydsworld.com

   :school (str): team whose games to select
   :start (int): the start year of games, 1992 <= x <= 2021
   :end (int, optional):  the end season of games, 1992 <= x <= 2021
   :vs (str, optional): school to filter games against. default: 'all'
   :parse_dates (bool, optional): whether to parse data into datetime64
   :return (pd.DataFrame): of all games played for a given team inclusive of start & end

.. py:function:: win_pct.calculate_actual_win_pct(games):

   A function to calculate the winning percentage as
   # games won / # games plated

   :games (pd.DataFrame): from boydsworld_team_results()
   :return (tuple): of actual winning percentage (float), wins (int), ties (int), losses (int)


.. py:function:: win_pct.calculate_pythagenpat_win_pct(games):

   A function to calculate the the PythagenPat expectated winning percentage.
   Pythagenpat Expectation formula (developed by David Smyth and Patriot):

   W% = R^x/(R^x + RA^x)
   where x = (RPG)^.287
   Developed by David Smyth and Patriot

   :games (pd.DataFrame): from boydsworld_team_results()
   :return (tuple): of expected winning percentage as a float, total run differential as int