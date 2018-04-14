#from __future__ import print_function
from functions.soupify import soupify
import pandas as pd
from collections import OrderedDict
import re
import os
#import sys


def scraper():
    
    needed = pd.read_csv('csv\\school_index.csv', header = 0)[['season_id','school_id']]
    needed = set((row.season_id,row.school_id) for index, row in needed[['season_id','school_id']].iterrows())
    
    
    file_loc = 'csv\\game_ids.csv'
    
    exist = os.path.isfile(file_loc)
    
    scraped = set() if not exist else set((row.season_id,row.school_id) for index, row in
                  pd.read_csv(file_loc, header = 0)[['season_id','school_id']].drop_duplicates().iterrows())
    
    
    left = needed - scraped
    
#    completed = len(scraped)
    for season_id, school_id in left:
        
#        percent_complete = '%5.2f'%(float(completed)/len(needed)*100)
#        
#        sys.stdout.flush()
#        print('Game IDs: {0}% Complete'.format(percent_complete), end = '\r')
        
        
        url = 'http://stats.ncaa.org/team/{0}/{1}'.format(school_id, season_id)
        
        soup = soupify(url)
        
        
        if soup.find('td', text = re.compile('Schedule\/Results')) == None:
            
            games_df = pd.DataFrame([[season_id, school_id]], columns = ['season_id','school_id'])
            
        else:
            
            games = [x for x in soup.find('td', text = re.compile('Schedule\/Results')).find_parent('table')\
                .find_all('tr', {'class': None}) if len(x.find_all('td')) > 1]
            
            data = OrderedDict()
                
            data['season_id'] = [season_id]*len(games)
            data['school_id'] = [school_id]*len(games)
            data['game_date'] = [x.find('td').text for x in games]
            data['opp_id'] = [int(x.find_all('td')[1].find('a').get('href').split('/')[2])
                if x.find_all('td')[1].find('a') != None else None for x in games]
            data['game_id'] = [int(x.find_all('td')[2].find('a').get('href').split('?')[0].split('/')[-1])
                if x.find_all('td')[2].find('a') != None else None for x in games]
            
            games_df = pd.DataFrame(data)
            games_df.loc[:,'game_date'] = pd.to_datetime(games_df.loc[:,'game_date'])
            games_df = games_df.loc[~pd.isnull(games_df.game_id)]
            
            if len(games_df) == 0:
                raise ValueError('no games: {0}'.format(url))
                
            
        with open(file_loc, 'ab' if exist else 'wb') as csvfile:
            games_df.to_csv(csvfile, header = not exist, index = False)
        
        exist = True
        
        
#        completed += 1
#        
#    sys.stdout.flush()
#    print('Game IDs: 100.00% Complete')
    
    
if __name__ == '__main__':
    
    scraper()