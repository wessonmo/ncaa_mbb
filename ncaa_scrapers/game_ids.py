from ncaa_scrapers.functions.soupify import soupify
import csv
import pandas as pd
from collections import OrderedDict
from time import time

school_divs = pd.read_csv('ncaa_scrapers\\csv\\school_divs.csv', header = 0)

try:
    game_ids = pd.read_csv('ncaa_scrapers\\csv\\game_ids.csv', header = 0)
except IOError as error:
    if str(error) == 'File ncaa_scrapers\csv\game_ids.csv does not exist':
        with open('ncaa_scrapers\\csv\\game_ids.csv', 'wb') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
            csvwriter.writerow(['season_href','opp_id','game_href'])
        game_ids = pd.read_csv('ncaa_scrapers\\csv\\game_ids.csv', header = 0)
    else:
        raise error

needed = pd.merge(school_divs, game_ids, how = 'left', on = ['season_id','school_id'], indicator = 'exist')
num_needed = len(needed.loc[needed.exist == 'left_only'])


i, start = 1, time()
for index, row in needed.loc[needed.exist == 'left_only'].sort_values(['season','school_id']).iterrows():
    
    soup = soupify('http://stats.ncaa.org/team/' + str(row.school_id) + '/' + str(row.season_id))
    
    try:    
        tbody = soup.find('td', text = 'Schedule/Results\n           ').parent.parent
    except AttributeError:
        with open('ncaa_scrapers\\csv\\game_ids.csv', 'ab') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
            csvwriter.writerow([row.season_id,row.school_id,None,None])
        continue
    
    games = [x.find_all('td') for x in tbody.find_all('tr') if (x.get('class') == None) and (len(x.find_all('td')) == 3)]
    
    data = OrderedDict()
    
    data['season_id'] = [row.season_id]*len(games)
    data['school_id'] = [row.school_id]*len(games)
    data['opp_id'] = [int(x[1].find('a').get('href').split('/')[2]) if x[1].find('a') != None else None for x in games]
    data['game_id'] = [int(x[2].find('a').get('href').split('?')[0].split('/')[-1]) if x[2].find('a') != None else None for x in games]
    
    games_df = pd.DataFrame(data)
    games_df = games_df.loc[pd.isnull(games_df.game_id) == False]
    
    if len(games_df) == 0:
        games_df.loc[0] = [row.season_id,row.school_id,None,None]
    
    with open('ncaa_scrapers\\csv\\game_ids.csv', 'ab') as hrefscsv:
        games_df.to_csv(hrefscsv, header = False, index = False)
        
    print(row.season_id, row.school_id)
        
    if i % 100 == 0:
        avg_time = (time() - start)/100
        start = time()
        print('Time Remaining: ' + str(avg_time*(num_needed - i)/60) + ' min')
    
    i += 1