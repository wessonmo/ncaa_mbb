from wranglers.soupify import soupify
import csv
import pandas as pd
from collections import OrderedDict
import re

game_hrefs = pd.read_csv('csv\\game_hrefs.csv', header = 0)

try:
    starters = pd.read_csv('csv\\starters.csv', header = 0)
except IOError as error:
    if str(error) == 'File csv\starters.csv does not exist':
        with open('csv\\starters.csv', 'wb') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
            csvwriter.writerow(['game_href','period','school_id','player_href','player_name'])
        starters = pd.read_csv('csv\\starters.csv', header = 0)
    else:
        raise error
        
game_hrefs_needed = set(game_hrefs.loc[pd.isnull(game_hrefs.game_href) == False].game_href) - set(starters.game_href)

i = 1
for game_href in game_hrefs_needed:
    gameid = re.compile('(?<=index\/)[0-9]{1,}').search(game_href).group(0)
    
    for period in [1,2]:
        soup = soupify('http://stats.ncaa.org/game/box_score/' + gameid + '?period_no=' + str(period))
        
        school_ids = [int(x.get('href').split('/')[2]) for x in soup.find_all('a', {'href': re.compile('\/team\/[0-9]+')})]
        
        roster_soups = [x.parent.parent.find_all('tr', {'class': 'smtext'}) for x in soup.find_all('th', text = 'Player')]
        
        try:
            roster_soups[0]
        except IndexError:
            with open('csv\\starters.csv', 'ab') as hrefscsv:
                hrefwriter = csv.writer(hrefscsv, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
                hrefwriter.writerow([game_href,period,None,None,None])
            break
        
        for roster_soup, school_id in zip(roster_soups, school_ids):
            starters = [x.find_all('td')[0] for x in roster_soup[:5]]
            
            data = OrderedDict()
            
            data['game_href'] = [game_href]*5
            data['period'] = [period]*5
            data['school_id'] = [school_id]*5
            data['player_href'] = [None if x.find('a') == None else x.find('a').get('href') for x in starters]
            data['player_name'] = [re.sub('[^\x00-\x7F]+','',x.text.strip().encode('utf8')) if x.has_attr('a') == False
                                        else x.find('a').text for x in starters]
            
            start5 = pd.DataFrame(data)
            
            with open('csv\\starters.csv', 'ab') as hrefscsv:
                start5.to_csv(hrefscsv, header = False, index = False)
    
    print(game_href + '\t\t' + str(i) + '/' + str(len(game_hrefs_needed)))
    i += 1