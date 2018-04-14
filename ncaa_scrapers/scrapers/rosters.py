#from __future__ import print_function
from functions.soupify import soupify
import pandas as pd
from collections import OrderedDict
import re
import os
#import sys
#import time

def scraper():
    
    needed = pd.read_csv('csv\\school_index.csv', header = 0)[['season_id','school_id']]
    needed = set((row.season_id,row.school_id) for index, row in needed[['season_id','school_id']].iterrows())
    
    
    file_loc = 'csv\\rosters.csv'
    
    exist = os.path.isfile(file_loc)
    
    scraped = set() if not exist else set((row.season_id,row.school_id) for index, row in
                  pd.read_csv(file_loc, header = 0)[['season_id','school_id']].drop_duplicates().iterrows())
    
    left = needed - scraped
    
    
#    completed, newline = len(scraped), True
    for season_id, school_id in left:
        
#        percent_complete = '%5.2f'%(float(completed)/len(needed)*100)
#        
#        sys.stdout.flush()
#        print('{0}Rosters: {1}% Complete'.format('\n' if newline else '', percent_complete), end = '\r')
#        newline = False
        
        url = 'http://stats.ncaa.org/team/{0}/roster/{1}'.format(school_id, season_id)
        
        soup = soupify(url)
        
        players = soup.find('th', text = re.compile('Roster')).find_parent('table').find('tbody').find_all('tr')
        players = [[y.contents[0] if y.contents else '' for y in x.find_all('td')] for x in players]
        
        data = OrderedDict()
            
        data['season_id'] = [season_id]*len(players)
        data['school_id'] = [school_id]*len(players)
        data['jersey'] = [int(x[0]) if re.compile('^[0-9]+$').search(x[0]) else None for x in players]
        data['player_id'] = [int(x[1].get('href').split('=')[-1]) if x[1].name == 'a' else None for x in players]
        data['player_name'] = [re.sub(r'[^\x00-\x7F]+','',x[1].text) if x[1].name == 'a'
            else re.sub(r'[^\x00-\x7F]+','',x[1]) for x in players]
        data['pos'] = [x[2] if x[2] != '' else None for x in players]
        data['height'] = [x[3] if x[3] not in ['','-'] else None for x in players]
        data['class'] = [x[4] if x[4] not in ['','N/A'] else None for x in players]
        
        data = pd.DataFrame(data)
        
        if len(data) == 0:
            raise ValueError('no roster: {0}'.format(url))
        
        with open(file_loc, 'ab' if exist else 'wb') as csvfile:
            data.to_csv(csvfile, header = not exist, index = False)
        
        exist = True
        
        
#        completed += 1
    
#    sys.stdout.flush()
#    print('\nRosters: 100.00% Complete')
    
    
if __name__ == '__main__':
    
    scraper()