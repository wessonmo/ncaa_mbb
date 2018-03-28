from ncaa_scrapers.functions.soupify import soupify
import csv
import pandas as pd
from collections import OrderedDict
from time import time

school_divs = pd.read_csv('ncaa_scrapers\\csv\\school_divs.csv', header = 0)

for season in range(2012,school_divs.season.max() + 1):
    
    try:
        rosters = pd.read_csv('ncaa_scrapers\\csv\\rosters_' + str(season) + '.csv', header = 0)
    except IOError as error:
        if str(error) == 'File ncaa_scrapers\\csv\\rosters_' + str(season) + '.csv does not exist':
            with open('ncaa_scrapers\\csv\\rosters_' + str(season) + '.csv', 'wb') as csvfile:
                csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
                csvwriter.writerow(['season','school_id','jersey','player_id','player_name','pos','height','class'])
            rosters = pd.read_csv('ncaa_scrapers\\csv\\rosters_' + str(season) + '.csv', header = 0)
        else:
            raise error
            
    needed = pd.merge(school_divs.loc[school_divs.season == season], rosters, how = 'left', on = ['season','school_id'],
                      indicator = 'exist')
    num_needed = len(needed.loc[needed.exist == 'left_only'])
    
    i, start = 1, time()
    for index, row in needed.loc[needed.exist == 'left_only'].sort_values('school_id').iterrows():
        
        soup = soupify('http://stats.ncaa.org/team/' + str(row.school_id) + '/roster/' + str(row.season_id))
        
        players = soup.find('table', {'id':'stat_grid'}).find('tbody').find_all('tr')
        
        data = OrderedDict()
        
        data['season'] = [row.season]*len(players)
        data['school_id'] = [row.school_id]*len(players)
        data['jersey'] = [int(x.find_all('td')[0].text) if x.find_all('td')[0].text != '' else None for x in players]
        data['player_id'] = [int(x.find_all('td')[1].find('a').get('href').split('=')[-1]) if x.find_all('td')[1].find('a') else None
                                for x in players]
        data['player_name'] = [x.find_all('td')[1].text for x in players]
        data['pos'] = [x.find_all('td')[2].text if x.find_all('td')[2].text != '' else None for x in players]
        data['height'] = [x.find_all('td')[3].text if x.find_all('td')[3].text != '' else None  for x in players]
        data['class'] = [x.find_all('td')[4].text if x.find_all('td')[4].text != '' else None  for x in players]
        
        roster = pd.DataFrame(data)
        
        with open('ncaa_scrapers\\csv\\rosters_' + str(season) + '.csv', 'ab') as hrefscsv:
            roster.to_csv(hrefscsv, header = False, index = False)
            
        print(row.season, row.school_id)
            
        if i % 100 == 0:
            avg_time = (time() - start)/100
            start = time()
            print('rosters Time Remaining: ' + str(avg_time*(num_needed - i)/60) + ' min')
        
        i += 1