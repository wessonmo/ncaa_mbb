from functions.soupify import soupify
import pandas as pd
from collections import OrderedDict
import sys
import os.path
import re

seasons = range(int(sys.argv[1]), int(sys.argv[2]) + 1)
    
divisions = range(int(sys.argv[3]), int(sys.argv[4]) + 1)

needed = set(zip(seasons,divisions*len(seasons)))


file_loc = 'csv\\school_index.csv'

exist = os.path.isfile(file_loc)

scraped = set() if not exist else set((row.season,row.division) for index, row in
              pd.read_csv(file_loc, header = 0)[['season','division']].drop_duplicates().iterrows())


needed = needed - scraped

for season, division in needed:
    
    url = 'http://stats.ncaa.org/team/inst_team_list?academic_year={0}&conf_id=-1&division={1}&sport_code=MBB'\
        .format(season, division)
    
    soup = soupify(url)
    
    schools = soup.find_all('a', href = re.compile('\/team\/[0-9]+\/[0-9]+$'))
    
    data = OrderedDict()
    
    data['season'] = [season]*len(schools)
    data['season_id'] = [int(x.get('href').split('/')[-1]) for x in schools]
    data['school_id'] = [int(x.get('href').split('/')[2]) for x in schools]
    data['school_name'] = [x.text for x in schools]
    data['division'] = [division]*len(schools)
    
    data = pd.DataFrame(data)
    
    with open(file_loc, 'ab' if exist else 'wb') as csvfile:
        data.to_csv(csvfile, index = False, header = not exist)
        
    exist = True
    
print('School Index: 100.00% Complete')