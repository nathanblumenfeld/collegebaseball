import pytest
from collegebaseball import boydsworld_scraper


@ pytest.fixture()
def generate_boydsworld_games():
    teams = [
        ('Cornell', 2002, 2018)
    ]
    generated_data = []
    for i in teams:
        data = boydsworld_scraper.get_games(i[0], i[1], i[2])
        generated_data.append(data)
    return generated_data


def test_boydsworld_games(generate_boydsworld_games):
    for i in generate_boydsworld_games:
        assert i is not None
