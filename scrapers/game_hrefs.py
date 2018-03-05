from wranglers.soupify import soupify
import csv
import pandas as pd
from collections import OrderedDict

school_divs = pd.read_csv('csv\\school_divs.csv', header = 0)

try:
    game_hrefs = pd.read_csv('csv\\game_hrefs.csv', header = 0)
except IOError as error:
    if str(error) == 'File csv\game_hrefs.csv does not exist':
        with open('csv\\game_hrefs.csv', 'wb') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
            csvwriter.writerow(['season_href','opp_id','game_href'])
        game_hrefs = pd.read_csv('csv\\game_hrefs.csv', header = 0)
    else:
        raise error

season_hrefs = set(school_divs.loc[school_divs.season > 2009].season_href) - set(game_hrefs.season_href)

for season_href, i in zip(season_hrefs, range(len(season_hrefs))):
    soup = soupify('http://stats.ncaa.org' + season_href)
    
    try:    
        tbody = soup.find('td', text = 'Schedule/Results\n           ').parent.parent
    except:
        continue
    games = [x.find_all('td') for x in tbody.find_all('tr') if (x.get('class') == None) and (len(x.find_all('td')) == 3)]
    
    data = OrderedDict()
    
    data['season_href'] = [season_href]*len(games)
    data['opp_id'] = [int(x[1].find('a').get('href').split('/')[2]) if x[1].find('a') != None else None for x in games]
    data['game_href'] = [x[2].find('a').get('href').split('?')[0] if x[2].find('a') != None else None for x in games]
    
    games_df = pd.DataFrame(data)
    games_df = games_df.loc[pd.isnull(games_df.game_href) == False]
    
    if len(games_df) == 0:
        games_df.loc[0] = [season_href,None,None]
    
    with open('csv\\game_hrefs.csv', 'ab') as hrefscsv:
        games_df.to_csv(hrefscsv, header = False, index = False)
    print(season_href + ' ' + str(i) + '/' + str(len(season_hrefs)))