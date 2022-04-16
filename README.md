# 

# `collegebaseball` <a href='https://github.com/nathanblumenfeld/collegebaseball'/>

<!-- badges: start -->

[![Twitter Follow](https://img.shields.io/twitter/follow/blumenfeldnate?color=blue&label=%40blumenfeldnate&logo=twitter&style=for-the-badge)](https://twitter.com/blumenfeldnate)

<!-- badges: end -->

`collegebaseball` is Python package for college baseball data acquisition and analysis.
 
## **Installation** 
`collegebaseball` is available to download from github
```
# install with the following:
pip install git+https://github.com/nathanblumenfeld/collegebaseball
```
## **Functionality**

You can use **get_team_stats** to retrieve single-season batting or pitching statistics for a school.   
It works for the 2013 - 2022 seasons.

Just give it a school, a season, and the kind of stats you would like ('batting' or 'pitching'). Acceptable school names are available in collegebaseball/data/schools.parquet.

```python
from collegebaseball import ncaa_scraper as ncaa
ncaa.get_team_stats("Cornell", 2019, "pitching").head()
```

<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Jersey</th>
      <th>name</th>
      <th>Yr</th>
      <th>pos</th>
      <th>GP</th>
      <th>GS</th>
      <th>App</th>
      <th>GS</th>
      <th>ERA</th>
      <th>IP</th>
      <th>...</th>
      <th>SHA</th>
      <th>SFA</th>
      <th>Pitches</th>
      <th>GO</th>
      <th>FO</th>
      <th>W</th>
      <th>L</th>
      <th>SV</th>
      <th>KL</th>
      <th>season</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>10</th>
      <td>34</td>
      <td>John Natoli</td>
      <td>Jr</td>
      <td>P</td>
      <td>19</td>
      <td>1</td>
      <td>19</td>
      <td>1</td>
      <td>1.73</td>
      <td>36.1</td>
      <td>...</td>
      <td>1</td>
      <td>1</td>
      <td>389</td>
      <td>32</td>
      <td>29</td>
      <td>5</td>
      <td>2</td>
      <td>7</td>
      <td>12</td>
      <td>2019</td>
    </tr>
    <tr>
      <th>11</th>
      <td>1</td>
      <td>Nikolas Lillios</td>
      <td>Fr</td>
      <td>P</td>
      <td>15</td>
      <td>5</td>
      <td>9</td>
      <td>0</td>
      <td>3.27</td>
      <td>11.0</td>
      <td>...</td>
      <td>0</td>
      <td>2</td>
      <td>57</td>
      <td>13</td>
      <td>13</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>2</td>
      <td>2019</td>
    </tr>
    <tr>
      <th>12</th>
      <td>26</td>
      <td>Trevor Daniel Davis</td>
      <td>So</td>
      <td>P</td>
      <td>14</td>
      <td>0</td>
      <td>14</td>
      <td>0</td>
      <td>4.96</td>
      <td>16.1</td>
      <td>...</td>
      <td>2</td>
      <td>2</td>
      <td>161</td>
      <td>14</td>
      <td>15</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>6</td>
      <td>2019</td>
    </tr>
    <tr>
      <th>13</th>
      <td>32</td>
      <td>Jon Zacharias</td>
      <td>Fr</td>
      <td>P</td>
      <td>13</td>
      <td>8</td>
      <td>13</td>
      <td>8</td>
      <td>2.83</td>
      <td>41.1</td>
      <td>...</td>
      <td>3</td>
      <td>3</td>
      <td>407</td>
      <td>45</td>
      <td>42</td>
      <td>1</td>
      <td>3</td>
      <td>0</td>
      <td>10</td>
      <td>2019</td>
    </tr>
    <tr>
      <th>14</th>
      <td>12</td>
      <td>Luke Yacinich</td>
      <td>Fr</td>
      <td>P</td>
      <td>13</td>
      <td>4</td>
      <td>13</td>
      <td>4</td>
      <td>8.07</td>
      <td>32.1</td>
      <td>...</td>
      <td>2</td>
      <td>0</td>
      <td>268</td>
      <td>33</td>
      <td>39</td>
      <td>2</td>
      <td>4</td>
      <td>0</td>
      <td>5</td>
      <td>2019</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 38 columns</p>
</div>


```python
school_id = ncaa.lookup_school_id('Cornell')
season_id = ncaa.lookup_season_id(2019)

ncaa.get_team_stats(school_id, season_id, "batting").head()
```

<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Jersey</th>
      <th>name</th>
      <th>Yr</th>
      <th>pos</th>
      <th>GP</th>
      <th>GS</th>
      <th>BA</th>
      <th>OBPct</th>
      <th>SlgPct</th>
      <th>R</th>
      <th>...</th>
      <th>HBP</th>
      <th>SF</th>
      <th>SH</th>
      <th>K</th>
      <th>DP</th>
      <th>CS</th>
      <th>Picked</th>
      <th>SB</th>
      <th>IBB</th>
      <th>season</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>15</td>
      <td>Matt Collins</td>
      <td>Jr</td>
      <td>INF</td>
      <td>38</td>
      <td>36</td>
      <td>0.217</td>
      <td>0.314</td>
      <td>0.325</td>
      <td>13</td>
      <td>...</td>
      <td>2</td>
      <td>0</td>
      <td>1</td>
      <td>42</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>10</td>
      <td>0</td>
      <td>14781</td>
    </tr>
    <tr>
      <th>1</th>
      <td>33</td>
      <td>William Simoneit</td>
      <td>Sr</td>
      <td>C</td>
      <td>38</td>
      <td>37</td>
      <td>0.299</td>
      <td>0.357</td>
      <td>0.493</td>
      <td>17</td>
      <td>...</td>
      <td>5</td>
      <td>0</td>
      <td>0</td>
      <td>13</td>
      <td>6</td>
      <td>0</td>
      <td>0</td>
      <td>6</td>
      <td>2</td>
      <td>14781</td>
    </tr>
    <tr>
      <th>2</th>
      <td>6</td>
      <td>Josh Arndt</td>
      <td>Sr</td>
      <td>INF</td>
      <td>38</td>
      <td>38</td>
      <td>0.214</td>
      <td>0.285</td>
      <td>0.303</td>
      <td>16</td>
      <td>...</td>
      <td>2</td>
      <td>4</td>
      <td>0</td>
      <td>32</td>
      <td>7</td>
      <td>3</td>
      <td>0</td>
      <td>6</td>
      <td>0</td>
      <td>14781</td>
    </tr>
    <tr>
      <th>3</th>
      <td>21</td>
      <td>Ramon Garza</td>
      <td>So</td>
      <td>INF</td>
      <td>37</td>
      <td>37</td>
      <td>0.219</td>
      <td>0.272</td>
      <td>0.299</td>
      <td>13</td>
      <td>...</td>
      <td>2</td>
      <td>3</td>
      <td>1</td>
      <td>23</td>
      <td>4</td>
      <td>2</td>
      <td>0</td>
      <td>7</td>
      <td>0</td>
      <td>14781</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2</td>
      <td>Nicholas Binnie</td>
      <td>So</td>
      <td>P</td>
      <td>34</td>
      <td>30</td>
      <td>0.257</td>
      <td>0.316</td>
      <td>0.305</td>
      <td>13</td>
      <td>...</td>
      <td>1</td>
      <td>0</td>
      <td>3</td>
      <td>26</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>6</td>
      <td>0</td>
      <td>14781</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 28 columns</p>
</div>



You can also get a players single-season stats for their entire college career, even if they moved teams.   

Just give **get_career_stats** a player_id and the kind of stats you want.   


```python
ncaa.get_career_stats(2111707, 'pitching')
```


<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>school_id</th>
      <th>GP</th>
      <th>G</th>
      <th>App</th>
      <th>GS</th>
      <th>ERA</th>
      <th>IP</th>
      <th>CG</th>
      <th>H</th>
      <th>R</th>
      <th>...</th>
      <th>SHA</th>
      <th>SFA</th>
      <th>Pitches</th>
      <th>GO</th>
      <th>FO</th>
      <th>W</th>
      <th>L</th>
      <th>SV</th>
      <th>KL</th>
      <th>season</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>167</td>
      <td>15</td>
      <td>15</td>
      <td>9</td>
      <td>0</td>
      <td>3.27</td>
      <td>11.0</td>
      <td>0.0</td>
      <td>12</td>
      <td>5</td>
      <td>...</td>
      <td>0</td>
      <td>2</td>
      <td>57</td>
      <td>13</td>
      <td>13</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>2</td>
      <td>2018</td>
    </tr>
    <tr>
      <th>1</th>
      <td>167</td>
      <td>5</td>
      <td>5</td>
      <td>5</td>
      <td>0</td>
      <td>1.69</td>
      <td>5.1</td>
      <td>0.0</td>
      <td>7</td>
      <td>1</td>
      <td>...</td>
      <td>1</td>
      <td>0</td>
      <td>76</td>
      <td>6</td>
      <td>6</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1</td>
      <td>2019</td>
    </tr>
  </tbody>
</table>
<p>2 rows × 35 columns</p>
</div>



n.b. get_career_stats cannot take a player name due to potential ambiguities. No worries though, as getting a   
player_id is easy with **lookup_player_id(player_name, school_name)**. 


```python
player_id = ncaa.lookup_player_id('William Simoneit', 'Cornell')
ncaa.get_career_stats(player_id, 'batting')
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>school_id</th>
      <th>GP</th>
      <th>G</th>
      <th>BA</th>
      <th>OBPct</th>
      <th>SlgPct</th>
      <th>R</th>
      <th>AB</th>
      <th>H</th>
      <th>2B</th>
      <th>...</th>
      <th>SF</th>
      <th>SH</th>
      <th>K</th>
      <th>DP</th>
      <th>CS</th>
      <th>Picked</th>
      <th>SB</th>
      <th>RBI2out</th>
      <th>IBB</th>
      <th>season</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>167</td>
      <td>24</td>
      <td>0</td>
      <td>0.317</td>
      <td>0.391</td>
      <td>0.610</td>
      <td>16</td>
      <td>82</td>
      <td>26</td>
      <td>9</td>
      <td>...</td>
      <td>0</td>
      <td>0</td>
      <td>14</td>
      <td>3.0</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>2016</td>
    </tr>
    <tr>
      <th>1</th>
      <td>167</td>
      <td>37</td>
      <td>0</td>
      <td>0.308</td>
      <td>0.380</td>
      <td>0.406</td>
      <td>17</td>
      <td>143</td>
      <td>44</td>
      <td>8</td>
      <td>...</td>
      <td>4</td>
      <td>0</td>
      <td>23</td>
      <td>0.0</td>
      <td>2</td>
      <td>0</td>
      <td>7</td>
      <td>0</td>
      <td>0</td>
      <td>2017</td>
    </tr>
    <tr>
      <th>2</th>
      <td>167</td>
      <td>38</td>
      <td>0</td>
      <td>0.299</td>
      <td>0.357</td>
      <td>0.493</td>
      <td>17</td>
      <td>144</td>
      <td>43</td>
      <td>10</td>
      <td>...</td>
      <td>0</td>
      <td>0</td>
      <td>13</td>
      <td>6.0</td>
      <td>0</td>
      <td>0</td>
      <td>6</td>
      <td>0</td>
      <td>2</td>
      <td>2018</td>
    </tr>
    <tr>
      <th>3</th>
      <td>749</td>
      <td>17</td>
      <td>0</td>
      <td>0.377</td>
      <td>0.462</td>
      <td>0.642</td>
      <td>13</td>
      <td>53</td>
      <td>20</td>
      <td>5</td>
      <td>...</td>
      <td>2</td>
      <td>0</td>
      <td>14</td>
      <td>2.0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>2019</td>
    </tr>
  </tbody>
</table>
<p>4 rows × 26 columns</p>
</div>



Calculating advanced metrics from these stats is made simple with **add_pitching_metrics** and **add_batting_metrics**. 

Just pass any DataFrame obtained from **get_team_stats** or **get_career_stats**.


```python
from collegebaseball import metrics
metrics.add_pitching_metrics(ncaa.get_team_stats("Cornell", 2019, "pitching")).head()
```


<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Jersey</th>
      <th>name</th>
      <th>Yr</th>
      <th>pos</th>
      <th>GP</th>
      <th>GS</th>
      <th>App</th>
      <th>GS</th>
      <th>ERA</th>
      <th>IP</th>
      <th>...</th>
      <th>OBP-against</th>
      <th>BA-against</th>
      <th>SLG-against</th>
      <th>OPS-against</th>
      <th>K/PA</th>
      <th>K/9</th>
      <th>BB/PA</th>
      <th>BB/9</th>
      <th>BABIP-against</th>
      <th>FIP</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>10</th>
      <td>34</td>
      <td>John Natoli</td>
      <td>Jr</td>
      <td>P</td>
      <td>19</td>
      <td>1</td>
      <td>19</td>
      <td>1</td>
      <td>1.73</td>
      <td>36.1</td>
      <td>...</td>
      <td>0.248</td>
      <td>0.176</td>
      <td>0.199</td>
      <td>0.447</td>
      <td>0.309</td>
      <td>11.395</td>
      <td>0.060</td>
      <td>2.229</td>
      <td>0.231</td>
      <td>2.410</td>
    </tr>
    <tr>
      <th>11</th>
      <td>1</td>
      <td>Nikolas Lillios</td>
      <td>Fr</td>
      <td>P</td>
      <td>15</td>
      <td>5</td>
      <td>9</td>
      <td>0</td>
      <td>3.27</td>
      <td>11.0</td>
      <td>...</td>
      <td>0.333</td>
      <td>0.300</td>
      <td>0.375</td>
      <td>0.708</td>
      <td>0.089</td>
      <td>3.273</td>
      <td>0.067</td>
      <td>2.455</td>
      <td>0.279</td>
      <td>4.042</td>
    </tr>
    <tr>
      <th>13</th>
      <td>32</td>
      <td>Jon Zacharias</td>
      <td>Fr</td>
      <td>P</td>
      <td>13</td>
      <td>8</td>
      <td>13</td>
      <td>8</td>
      <td>2.83</td>
      <td>41.1</td>
      <td>...</td>
      <td>0.316</td>
      <td>0.247</td>
      <td>0.313</td>
      <td>0.629</td>
      <td>0.172</td>
      <td>6.532</td>
      <td>0.080</td>
      <td>3.048</td>
      <td>0.247</td>
      <td>4.120</td>
    </tr>
    <tr>
      <th>18</th>
      <td>8</td>
      <td>Kevin Cushing</td>
      <td>Fr</td>
      <td>P</td>
      <td>11</td>
      <td>0</td>
      <td>11</td>
      <td>0</td>
      <td>4.50</td>
      <td>12.0</td>
      <td>...</td>
      <td>0.404</td>
      <td>0.333</td>
      <td>0.400</td>
      <td>0.804</td>
      <td>0.135</td>
      <td>5.250</td>
      <td>0.115</td>
      <td>4.500</td>
      <td>0.326</td>
      <td>4.284</td>
    </tr>
    <tr>
      <th>15</th>
      <td>31</td>
      <td>Colby Wyatt</td>
      <td>Jr</td>
      <td>P</td>
      <td>13</td>
      <td>10</td>
      <td>13</td>
      <td>10</td>
      <td>3.68</td>
      <td>63.2</td>
      <td>...</td>
      <td>0.340</td>
      <td>0.290</td>
      <td>0.405</td>
      <td>0.745</td>
      <td>0.124</td>
      <td>4.948</td>
      <td>0.064</td>
      <td>2.545</td>
      <td>0.285</td>
      <td>4.548</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 50 columns</p>
</div>




```python
metrics.add_pitching_metrics(ncaa.get_career_stats(2111716, 'pitching')).head()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>school_id</th>
      <th>GP</th>
      <th>G</th>
      <th>App</th>
      <th>GS</th>
      <th>ERA</th>
      <th>IP</th>
      <th>CG</th>
      <th>H</th>
      <th>R</th>
      <th>...</th>
      <th>OBP-against</th>
      <th>BA-against</th>
      <th>SLG-against</th>
      <th>OPS-against</th>
      <th>K/PA</th>
      <th>K/9</th>
      <th>BB/PA</th>
      <th>BB/9</th>
      <th>BABIP-against</th>
      <th>FIP</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>167</td>
      <td>11</td>
      <td>11</td>
      <td>11</td>
      <td>0</td>
      <td>4.50</td>
      <td>12.0</td>
      <td>0.0</td>
      <td>15</td>
      <td>6</td>
      <td>...</td>
      <td>0.404</td>
      <td>0.333</td>
      <td>0.400</td>
      <td>0.804</td>
      <td>0.135</td>
      <td>5.250</td>
      <td>0.115</td>
      <td>4.500</td>
      <td>0.326</td>
      <td>4.174</td>
    </tr>
    <tr>
      <th>1</th>
      <td>167</td>
      <td>3</td>
      <td>3</td>
      <td>3</td>
      <td>0</td>
      <td>7.94</td>
      <td>5.2</td>
      <td>0.0</td>
      <td>7</td>
      <td>6</td>
      <td>...</td>
      <td>0.385</td>
      <td>0.304</td>
      <td>0.522</td>
      <td>0.907</td>
      <td>0.308</td>
      <td>12.706</td>
      <td>0.115</td>
      <td>4.765</td>
      <td>0.353</td>
      <td>5.010</td>
    </tr>
    <tr>
      <th>2</th>
      <td>167</td>
      <td>3</td>
      <td>3</td>
      <td>3</td>
      <td>2</td>
      <td>20.57</td>
      <td>7.0</td>
      <td>0.0</td>
      <td>13</td>
      <td>17</td>
      <td>...</td>
      <td>0.574</td>
      <td>0.394</td>
      <td>0.758</td>
      <td>1.332</td>
      <td>0.064</td>
      <td>3.857</td>
      <td>0.191</td>
      <td>11.571</td>
      <td>0.244</td>
      <td>14.195</td>
    </tr>
  </tbody>
</table>
<p>3 rows × 47 columns</p>
</div>




```python
metrics.add_batting_metrics(ncaa.get_team_stats(167, 2019, 'batting')).head()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Jersey</th>
      <th>name</th>
      <th>Yr</th>
      <th>pos</th>
      <th>GP</th>
      <th>GS</th>
      <th>BA</th>
      <th>OBPct</th>
      <th>SlgPct</th>
      <th>R</th>
      <th>...</th>
      <th>OBP</th>
      <th>SLG</th>
      <th>OPS</th>
      <th>ISO</th>
      <th>K%</th>
      <th>BB%</th>
      <th>BABIP</th>
      <th>wOBA</th>
      <th>wRAA</th>
      <th>wRC</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>9</th>
      <td>13</td>
      <td>Adam Saks</td>
      <td>Sr</td>
      <td>P</td>
      <td>26</td>
      <td>26</td>
      <td>0.337</td>
      <td>0.414</td>
      <td>0.515</td>
      <td>13</td>
      <td>...</td>
      <td>0.412</td>
      <td>0.515</td>
      <td>0.927</td>
      <td>0.178</td>
      <td>0.092</td>
      <td>0.118</td>
      <td>0.367</td>
      <td>0.416</td>
      <td>6.946</td>
      <td>24.201</td>
    </tr>
    <tr>
      <th>1</th>
      <td>33</td>
      <td>William Simoneit</td>
      <td>Sr</td>
      <td>C</td>
      <td>38</td>
      <td>37</td>
      <td>0.299</td>
      <td>0.357</td>
      <td>0.493</td>
      <td>17</td>
      <td>...</td>
      <td>0.374</td>
      <td>0.493</td>
      <td>0.867</td>
      <td>0.194</td>
      <td>0.084</td>
      <td>0.052</td>
      <td>0.296</td>
      <td>0.390</td>
      <td>5.191</td>
      <td>27.666</td>
    </tr>
    <tr>
      <th>0</th>
      <td>15</td>
      <td>Matt Collins</td>
      <td>Jr</td>
      <td>INF</td>
      <td>38</td>
      <td>36</td>
      <td>0.217</td>
      <td>0.314</td>
      <td>0.325</td>
      <td>13</td>
      <td>...</td>
      <td>0.312</td>
      <td>0.325</td>
      <td>0.637</td>
      <td>0.108</td>
      <td>0.304</td>
      <td>0.109</td>
      <td>0.316</td>
      <td>0.309</td>
      <td>-6.075</td>
      <td>13.935</td>
    </tr>
    <tr>
      <th>7</th>
      <td>25</td>
      <td>Jason Apostle</td>
      <td>So</td>
      <td>OF</td>
      <td>28</td>
      <td>23</td>
      <td>0.233</td>
      <td>0.333</td>
      <td>0.315</td>
      <td>6</td>
      <td>...</td>
      <td>0.318</td>
      <td>0.315</td>
      <td>0.633</td>
      <td>0.082</td>
      <td>0.273</td>
      <td>0.091</td>
      <td>0.347</td>
      <td>0.308</td>
      <td>-3.958</td>
      <td>8.802</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2</td>
      <td>Nicholas Binnie</td>
      <td>So</td>
      <td>P</td>
      <td>34</td>
      <td>30</td>
      <td>0.257</td>
      <td>0.316</td>
      <td>0.305</td>
      <td>13</td>
      <td>...</td>
      <td>0.308</td>
      <td>0.305</td>
      <td>0.613</td>
      <td>0.048</td>
      <td>0.222</td>
      <td>0.068</td>
      <td>0.342</td>
      <td>0.295</td>
      <td>-6.718</td>
      <td>10.247</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 40 columns</p>
</div>


```python
metrics.add_batting_metrics(ncaa.get_career_stats(1779078, 'batting')).head()
```

<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>school_id</th>
      <th>GP</th>
      <th>G</th>
      <th>BA</th>
      <th>OBPct</th>
      <th>SlgPct</th>
      <th>R</th>
      <th>AB</th>
      <th>H</th>
      <th>2B</th>
      <th>...</th>
      <th>OBP</th>
      <th>SLG</th>
      <th>OPS</th>
      <th>ISO</th>
      <th>K%</th>
      <th>BB%</th>
      <th>BABIP</th>
      <th>wOBA</th>
      <th>wRAA</th>
      <th>wRC</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2</th>
      <td>167</td>
      <td>32</td>
      <td>0</td>
      <td>0.231</td>
      <td>0.272</td>
      <td>0.368</td>
      <td>14</td>
      <td>117</td>
      <td>27</td>
      <td>8</td>
      <td>...</td>
      <td>0.272</td>
      <td>0.368</td>
      <td>0.640</td>
      <td>0.137</td>
      <td>0.216</td>
      <td>0.048</td>
      <td>0.281</td>
      <td>0.293</td>
      <td>-6.639</td>
      <td>11.236</td>
    </tr>
    <tr>
      <th>3</th>
      <td>167</td>
      <td>38</td>
      <td>0</td>
      <td>0.214</td>
      <td>0.285</td>
      <td>0.303</td>
      <td>16</td>
      <td>145</td>
      <td>31</td>
      <td>8</td>
      <td>...</td>
      <td>0.285</td>
      <td>0.303</td>
      <td>0.588</td>
      <td>0.089</td>
      <td>0.194</td>
      <td>0.085</td>
      <td>0.259</td>
      <td>0.284</td>
      <td>-10.744</td>
      <td>12.521</td>
    </tr>
    <tr>
      <th>0</th>
      <td>167</td>
      <td>28</td>
      <td>0</td>
      <td>0.229</td>
      <td>0.289</td>
      <td>0.277</td>
      <td>7</td>
      <td>83</td>
      <td>19</td>
      <td>4</td>
      <td>...</td>
      <td>0.286</td>
      <td>0.277</td>
      <td>0.563</td>
      <td>0.048</td>
      <td>0.110</td>
      <td>0.066</td>
      <td>0.260</td>
      <td>0.277</td>
      <td>-5.929</td>
      <td>6.538</td>
    </tr>
    <tr>
      <th>1</th>
      <td>167</td>
      <td>28</td>
      <td>0</td>
      <td>0.187</td>
      <td>0.256</td>
      <td>0.280</td>
      <td>5</td>
      <td>75</td>
      <td>14</td>
      <td>7</td>
      <td>...</td>
      <td>0.253</td>
      <td>0.280</td>
      <td>0.533</td>
      <td>0.093</td>
      <td>0.241</td>
      <td>0.084</td>
      <td>0.255</td>
      <td>0.257</td>
      <td>-6.854</td>
      <td>4.683</td>
    </tr>
  </tbody>
</table>
<p>4 rows × 38 columns</p>
</div>


You can get a school's roster with **get_roster**, which takes either school_name/school_id and season/season_id.  
**get_roster** also gives the column stats_player_seq (i.e. player_id), which can be quite useful.


```python
ncaa.get_roster('Cornell', 2018).head()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>jersey</th>
      <th>stats_player_seq</th>
      <th>name</th>
      <th>position</th>
      <th>class_year</th>
      <th>games_played</th>
      <th>games_started</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>25</td>
      <td>1997329</td>
      <td>Apostle, Jason</td>
      <td>OF</td>
      <td>Fr</td>
      <td>24</td>
      <td>19</td>
    </tr>
    <tr>
      <th>1</th>
      <td>6</td>
      <td>1779078</td>
      <td>Arndt, Josh</td>
      <td>INF</td>
      <td>Jr</td>
      <td>32</td>
      <td>27</td>
    </tr>
    <tr>
      <th>2</th>
      <td>16</td>
      <td>1779080</td>
      <td>Arnold, Austin</td>
      <td>P</td>
      <td>Jr</td>
      <td>3</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>29</td>
      <td>1997331</td>
      <td>Bailey, Garrett</td>
      <td>P</td>
      <td>Fr</td>
      <td>4</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>35</td>
      <td>1652419</td>
      <td>Baur, Trey</td>
      <td>INF</td>
      <td>Sr</td>
      <td>26</td>
      <td>22</td>
    </tr>
  </tbody>
</table>
</div>



If you want a team's roster over multiple years, you can use **get_multiyear_roster**, 
which returns a concatenated (not aggregated!) DataFrame. 

It also adds on season_ids, which can be handy. 


```python
ncaa.get_multiyear_roster('Cornell', 2015, 2018).head()
```

<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>jersey</th>
      <th>stats_player_seq</th>
      <th>name</th>
      <th>position</th>
      <th>class_year</th>
      <th>games_played</th>
      <th>games_started</th>
      <th>season</th>
      <th>school</th>
      <th>season_id</th>
      <th>batting_id</th>
      <th>pitching_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>20</td>
      <td>1546998</td>
      <td>Balestrieri, Paul</td>
      <td>P</td>
      <td>So</td>
      <td>15</td>
      <td>3</td>
      <td>2015</td>
      <td>Cornell</td>
      <td>12080</td>
      <td>10780</td>
      <td>10781</td>
    </tr>
    <tr>
      <th>1</th>
      <td>35</td>
      <td>1652419</td>
      <td>Baur, Trey</td>
      <td>INF</td>
      <td>Fr</td>
      <td>9</td>
      <td>5</td>
      <td>2015</td>
      <td>Cornell</td>
      <td>12080</td>
      <td>10780</td>
      <td>10781</td>
    </tr>
    <tr>
      <th>2</th>
      <td>24</td>
      <td>1652397</td>
      <td>Bitar, Ellis</td>
      <td>C</td>
      <td>Fr</td>
      <td>21</td>
      <td>18</td>
      <td>2015</td>
      <td>Cornell</td>
      <td>12080</td>
      <td>10780</td>
      <td>10781</td>
    </tr>
    <tr>
      <th>3</th>
      <td>37</td>
      <td>1547001</td>
      <td>Brewer, Ray</td>
      <td>P</td>
      <td>So</td>
      <td>5</td>
      <td>0</td>
      <td>2015</td>
      <td>Cornell</td>
      <td>12080</td>
      <td>10780</td>
      <td>10781</td>
    </tr>
    <tr>
      <th>4</th>
      <td>32</td>
      <td>1324535</td>
      <td>Busto, Nick</td>
      <td>P</td>
      <td>Sr</td>
      <td>11</td>
      <td>6</td>
      <td>2015</td>
      <td>Cornell</td>
      <td>12080</td>
      <td>10780</td>
      <td>10781</td>
    </tr>
  </tbody>
</table>
</div>




```python
ncaa.get_multiyear_roster('Cornell', 2015, 2018).tail()
```



<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>jersey</th>
      <th>stats_player_seq</th>
      <th>name</th>
      <th>position</th>
      <th>class_year</th>
      <th>games_played</th>
      <th>games_started</th>
      <th>season</th>
      <th>school</th>
      <th>season_id</th>
      <th>batting_id</th>
      <th>pitching_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>132</th>
      <td>20</td>
      <td>1997324</td>
      <td>Urbon, Seth</td>
      <td>P</td>
      <td>So</td>
      <td>12</td>
      <td>10</td>
      <td>2018</td>
      <td>Cornell</td>
      <td>12973</td>
      <td>11953</td>
      <td>11954</td>
    </tr>
    <tr>
      <th>133</th>
      <td>11</td>
      <td>1783016</td>
      <td>Wahl, Austin</td>
      <td>P</td>
      <td>Sr</td>
      <td>16</td>
      <td>1</td>
      <td>2018</td>
      <td>Cornell</td>
      <td>12973</td>
      <td>11953</td>
      <td>11954</td>
    </tr>
    <tr>
      <th>134</th>
      <td>5</td>
      <td>1652396</td>
      <td>Wickham, Dale</td>
      <td>OF</td>
      <td>Sr</td>
      <td>36</td>
      <td>35</td>
      <td>2018</td>
      <td>Cornell</td>
      <td>12973</td>
      <td>11953</td>
      <td>11954</td>
    </tr>
    <tr>
      <th>135</th>
      <td>36</td>
      <td>1547000</td>
      <td>Willittes, Tim</td>
      <td>P</td>
      <td>Sr</td>
      <td>11</td>
      <td>11</td>
      <td>2018</td>
      <td>Cornell</td>
      <td>12973</td>
      <td>11953</td>
      <td>11954</td>
    </tr>
    <tr>
      <th>136</th>
      <td>31</td>
      <td>1884389</td>
      <td>Wyatt, Colby</td>
      <td>P</td>
      <td>So</td>
      <td>19</td>
      <td>0</td>
      <td>2018</td>
      <td>Cornell</td>
      <td>12973</td>
      <td>11953</td>
      <td>11954</td>
    </tr>
  </tbody>
</table>
</div>


There are also some lookup functions included to make life easier.    

You can get a player_id from their name and school with **lookup_player_id**


```python
ncaa.lookup_player_id("Jake Gelof", "Virginia")
>>> 2486499
```

the season_id, batting_id, and pitching_id of a given season with **lookup_season_ids**


```python
ncaa.lookup_season_ids(2019)
>>> (14781, 14643, 14644)
```

or the season, batting_id, and pitching_id for a given season_id with **lookup_season_ids_reverse**


```python
ncaa.lookup_season_ids_reverse(14781)
>>> (2019, 14643, 14644)
```

sometimes, you just need a single season_id. **lookup_season_id** has you covered. 


```python
ncaa.lookup_season_id(2019)
>>> 14781
```


You can also find the debut and most recent seasons in which a given player has made and appearance in.   
Just pass a player_id into **lookup_seasons_played**. 


```python
ncaa.lookup_seasons_played(2486499)
>>> (2021, 2021)
```

You can get a school_id by giving **lookup_school_id** the correct (from data/schools.parquet or data/schools/csv) school name

```python
ncaa.lookup_school_id("Cornell")
>>> 167
```


collegebaseball also includes a scraper for the [Boyds World Game Score Database](http://boydsworld.com/data/scores.html) through the **get_games** function.

```python
from collegebaseball import boydsworld_scraper as bws
bws.get_games('Cornell', 2015, 2018)
```

<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>field</th>
      <th>runs_scored</th>
      <th>runs_allowed</th>
      <th>opponent</th>
      <th>run_difference</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2015-02-21</td>
      <td>@Gardner-Webb</td>
      <td>1</td>
      <td>2</td>
      <td>Gardner-Webb</td>
      <td>-1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2015-02-21</td>
      <td>@Gardner-Webb</td>
      <td>2</td>
      <td>3</td>
      <td>Gardner-Webb</td>
      <td>-1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2015-02-22</td>
      <td>@Gardner-Webb</td>
      <td>1</td>
      <td>2</td>
      <td>Gardner-Webb</td>
      <td>-1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2015-02-27</td>
      <td>@neutral</td>
      <td>0</td>
      <td>3</td>
      <td>Seton Hall</td>
      <td>-3</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2015-02-28</td>
      <td>@neutral</td>
      <td>4</td>
      <td>3</td>
      <td>Hartford</td>
      <td>1</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>146</th>
      <td>2018-05-05</td>
      <td>@Brown</td>
      <td>6</td>
      <td>7</td>
      <td>Brown</td>
      <td>-1</td>
    </tr>
    <tr>
      <th>148</th>
      <td>2018-05-06</td>
      <td>@Brown</td>
      <td>4</td>
      <td>2</td>
      <td>Brown</td>
      <td>2</td>
    </tr>
    <tr>
      <th>150</th>
      <td>2018-05-12</td>
      <td>@Cornell</td>
      <td>14</td>
      <td>2</td>
      <td>Columbia</td>
      <td>12</td>
    </tr>
    <tr>
      <th>149</th>
      <td>2018-05-12</td>
      <td>@Cornell</td>
      <td>2</td>
      <td>3</td>
      <td>Columbia</td>
      <td>-1</td>
    </tr>
    <tr>
      <th>151</th>
      <td>2018-05-13</td>
      <td>@Cornell</td>
      <td>2</td>
      <td>3</td>
      <td>Columbia</td>
      <td>-1</td>
    </tr>
  </tbody>
</table>
<p>152 rows × 6 columns</p>
</div>

The **win_pct** module includes functions to calculate the actual:  
```python
from collegebaseball import win_pct
win_pct.calculate_actual_win_pct('Cornell', bws.get_games('Cornell', 2015, 2018))
# win_pct, wins, ties, losses
>>> (0.408, 62, 0, 90)
```

and pythagenpat expecting winning percentages from the results of **get_games**. 

```python
win_pct.calculate_pythagenpat_win_pct('Cornell', bws.get_games('Cornell', 2015, 2018))
# expected_win_pct, run differential
>>> (0.43, -144)
```

## Planned Feature Additions

- [ ] Team Schedules
- [ ] Play-by-Play Data
- [ ] Linear Weight Calculation Tools
- [ ] SIERA
- [ ] Park Factor Calculation Tools
- [ ] WRC+

If you have more ideas or if you are interested in contributing, please reach out to @blumenfeldnate on twitter or email nathanblumenfeld@gmail.com

