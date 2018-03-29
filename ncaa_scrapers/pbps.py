from ncaa_scrapers.functions.soupify import soupify
import csv
import pandas as pd
from collections import OrderedDict
from time import time
import re

school_divs = pd.read_csv('ncaa_scrapers\\csv\\school_divs.csv', header = 0)

for season in range(2012,school_divs.season.max() + 1):
    
    all_game_ids = pd.read_csv('ncaa_scrapers\\csv\\game_ids_' + str(season) + '.csv', header = 0)
    all_game_ids = set(x for x in set(all_game_ids.loc[pd.isnull(all_game_ids.game_id) == False].game_id))
    
    try:
        scraped_pbp = set(pd.read_csv('ncaa_scrapers\\csv\\pbp_' + str(season) + '.csv', header = 0).game_id)
    except IOError as error:
        if str(error) == 'File ncaa_scrapers\\csv\\pbp_' + str(season) + '.csv does not exist':
            with open('ncaa_scrapers\\csv\\pbp_' + str(season) + '.csv', 'wb') as csvfile:
                csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
                csvwriter.writerow(['game_id','period','time','away_event','score','home_event'])
            scraped_pbp = set()
        else:
            raise error
            
    try:
        scraped_official = set(pd.read_csv('ncaa_scrapers\\csv\\officials_' + str(season) + '.csv', header = 0).game_id)
    except IOError as error:
        if str(error) == 'File ncaa_scrapers\\csv\\officials_' + str(season) + '.csv does not exist':
            with open('ncaa_scrapers\\csv\\officials_' + str(season) + '.csv', 'wb') as csvfile:
                csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
                csvwriter.writerow(['game_id','official'])
            scraped_official = set()
        else:
            raise error
            
    game_ids_needed = sorted(all_game_ids - (scraped_pbp & scraped_official))
    num_needed = len(game_ids_needed)
    
    i, start = 1, time()
    for game_id in game_ids_needed:
        
        soup = soupify('http://stats.ncaa.org/game/play_by_play/' + str(int(game_id)))
        
        if game_id not in scraped_official:
            try:
                officials = re.sub(', Jr.',' Jr',soup.find('td', text = 'Officials:').find_next_sibling('td').text.strip()).split(', ')
            except AttributeError:
                officials = [None]
                
            for official in officials:
                with open('ncaa_scrapers\\csv\\officials_' + str(season) + '.csv', 'ab') as csvfile:
                    csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
                    csvwriter.writerow([game_id,official])
        
        if game_id not in scraped_pbp:
            period = 1
            while True:
                try:
                    events = soup.find('a',{'id': 'pd' + str(period)}).find_parent('table').find_next_sibling('table')\
                                .find_all('tr', class_ = lambda x: x != 'grey_heading')
                except AttributeError:
                    if period == 1:
                        with open('ncaa_scrapers\\csv\\pbp_' + str(season) + '.csv', 'wb') as csvfile:
                            csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
                            csvwriter.writerow([game_id,None,None,None,None,None])
                    break
                
                data = OrderedDict()
                    
                data['game_id'] = [game_id]*len(events)
                data['period'] = [period]*len(events)
                data['time'] = [x.find_all('td')[0].text for x in events]
                data['away_event'] = [re.sub(r'[^\x00-\x7F]+','',x.find_all('td')[1].text)
                                        if x.find_all('td')[1].text != '' else None for x in events]
                data['score'] = [x.find_all('td')[2].text for x in events]
                data['home_event'] = [re.sub(r'[^\x00-\x7F]+','',x.find_all('td')[3].text)
                                        if x.find_all('td')[3].text != '' else None for x in events]
                
                pbp = pd.DataFrame(data)
                    
                with open('ncaa_scrapers\\csv\\pbp_' + str(season) + '.csv', 'ab') as hrefscsv:
                    pbp.to_csv(hrefscsv, header = False, index = False)            
                
                period += 1
            
        if i % 100 == 0:
            avg_time = (time() - start)/i
            print(str(season) + ' pbp Remaining: ' + str(num_needed - i) + ' ' + str(avg_time*(num_needed - i)/60) + ' min')
        
        i += 1