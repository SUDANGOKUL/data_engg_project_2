import requests
from bs4 import BeautifulSoup

r = requests.get('https://en.wikipedia.org/wiki/Car_colour_popularity')
soup = BeautifulSoup(r.text, 'html.parser')

color_table = soup.find('table', class_ = 'sortable wikitable jquery-tablesorter')

for team in color_table.find_all('tbody'):
    rows = team.find_all('tr')
    for row in rows:
        color = row.find('td', class_ ='legend').text.strip()
        points = row.find_all('td')[8].text
        print(color, points)