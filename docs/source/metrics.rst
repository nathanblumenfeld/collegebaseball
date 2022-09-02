=======
Metrics
=======

batting
-------
.. py:function:: calculate_wrc_manual(plate_appearances, woba, season):

    Calculate wRC with manually inputted data

    :plate_appearances (int):
    :woba (float): 
    :season (int):
    :return (float):

.. py:function:: calculate_wraa_manual(plate_appearances, woba,  season):

    Calculate wRAA with manually inputted data

    :plate_appearances (int):
    :woba (float): 
    :season (int):

    :return (float):

.. py:function:: calculate_woba_manual(plate_appearances, walks, hits_by_pitch, singles,
                          doubles, triples, homeruns, season):

    Calculate wOBA with manually inputted data

    :plate_appearances (int):
    :walks (float): 
    :hits_by_pitch (int):
    :singles (int):
    :doubles (int):
    :triples (int):
    :homeruns (int):
    :season (int):

    :return (float):

.. py:function:: add_batting_metrics(df):
    
    Adds all available additional batting metrics 

    :return (pd.DataFrame):

pitching
--------
.. py:function:: calculate_fip_manual(homeruns, walks, hit_batters, strikeouts, innings_pitched, season):
    
    Calculate wOBA with manually inputted data
    
    :homeruns (int):
    :walks (int):
    :hit_batters (int):
    :strikeouts (int):
    :innings_pitched (int):
    :season (int):

    :returns (float):
    
.. py:function:: add_pitching_metrics(df):

    Adds all available additional pitching metrics 

    :df (pd.DataFrame):
    :return (pd.DataFrame):

                          