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
        scraped_pbp = set(pd.read_csv('ncaa_scrapers\\csv\\pbps_' + str(season) + '.csv', header = 0).game_id)
    except IOError as error:
        if str(error) == 'File ncaa_scrapers\\csv\\pbps_' + str(season) + '.csv does not exist':
            with open('ncaa_scrapers\\csv\\pbps_' + str(season) + '.csv', 'wb') as csvfile:
                csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
                csvwriter.writerow(['game_id','period','time','school1_id','school1_event','score','school2_id','school2_event'])
            scraped_pbp = set()
        else:
            raise error
            
    try:
        scraped_official = set(pd.read_csv('ncaa_scrapers\\csv\\officials_' + str(season) + '.csv', header = 0).game_id)
    except IOError as error:
        if str(error) == 'File ncaa_scrapers\\csv\\officials_' + str(season) + '.csv does not exist':
            with open('ncaa_scrapers\\csv\\officials_' + str(season) + '.csv', 'wb') as csvfile:
                csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
                csvwriter.writerow(['game_id','officials'])
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
                officials = soup.find('td', text = 'Officials:').find_next_sibling('td').text.strip()
            except AttributeError:
                officials = None
                
            with open('ncaa_scrapers\\csv\\officials_' + str(season) + '.csv', 'ab') as csvfile:
                csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
                csvwriter.writerow([game_id,officials])
        
        if game_id not in scraped_pbp:
            period = 1
            while True:
                try:
                    events = soup.find('a',{'id': 'pd' + str(period)}).find_parent('table').find_next_sibling('table')\
                                .find_all('tr', class_ = lambda x: x != 'grey_heading')
                                
                    teams = soup.find('td', text = 'Total').find_parent('table').find_all('tr')[1:]
                    team_ids = [int(re.compile('(?<=team\/)[0-9]+(?=\/)').search(x.find('a').get('href')).group(0))
                         if x.find('a') != None else None for x in teams]
                    
                    if team_ids == [None,None]:
                        raise AttributeError
                        
                except AttributeError:
                    if period == 1:
                        with open('ncaa_scrapers\\csv\\pbps_' + str(season) + '.csv', 'ab') as csvfile:
                            csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
                            csvwriter.writerow([game_id,None,None,None,None,None])
                    break
                
                data = OrderedDict()
                    
                data['game_id'] = [game_id]*len(events)
                data['period'] = [period]*len(events)
                data['time'] = [x.find_all('td')[0].text for x in events]
                data['school1_id'] = [team_ids[0]]*len(events)
                data['school1_event'] = [re.sub(r'[^\x00-\x7F]+','',x.find_all('td')[1].text)
                                        if x.find_all('td')[1].text != '' else None for x in events]
                data['score'] = [x.find_all('td')[2].text for x in events]
                data['school2_id'] = [team_ids[1]]*len(events)
                data['school2_event'] = [re.sub(r'[^\x00-\x7F]+','',x.find_all('td')[3].text)
                                        if x.find_all('td')[3].text != '' else None for x in events]
                
                pbp = pd.DataFrame(data)
                    
                with open('ncaa_scrapers\\csv\\pbps_' + str(season) + '.csv', 'ab') as hrefscsv:
                    pbp.to_csv(hrefscsv, header = False, index = False)            
                
                period += 1
            
        if (i % 100 == 0) or (i == 1):
            avg_time = (time() - start)/i
            print(str(season) + ' pbp Remaining: ' + str(num_needed - i) + ' ' + str(int(avg_time*(num_needed - i)/60)) + ' min')
        
        i += 1