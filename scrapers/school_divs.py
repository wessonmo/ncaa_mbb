from wranglers.soupify import soupify
import pandas as pd
import csv
from datetime import date
from collections import OrderedDict

first_season = 2009
last_season = date.today().year if date.today().timetuple().tm_yday >= 72 else date.today().year - 1

try:
    school_divs = pd.read_csv('ncaa_scrapers\\csv\\school_divs.csv', header = 0)
except IOError as error:
    if str(error) == 'File ncaa_scrapers\\csv\\school_divs.csv does not exist':
        with open('ncaa_scrapers\\csv\\school_divs.csv', 'wb') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
            csvwriter.writerow(['school_id','school_name','season','division'])
        school_info = pd.read_csv('ncaa_scrapers\\csv\\school_divs.csv', header = 0)
    else:
        raise error

seasons = [item for item in range(first_season, last_season + 1) for i in range(3)]
seasons_to_scrape = set(zip(seasons,[1,2,3]*len(seasons))) - set(zip(school_divs.season,school_divs.division))

prev_season = None
for season, division in sorted(seasons_to_scrape):
    
    if prev_season != season:
        print('\t' + str(season))
        prev_season = season
        
    soup = soupify('http://stats.ncaa.org/team/inst_team_list?' +
                   'academic_year=' + str(season) +
                   '&conf_id=-1&division=' + str(division) +
                   '&sport_code=' + 'MBB')
    
    atags = soup.find('body').find('div', {'style': 'display:block'}).find_all('a')
    
    data = OrderedDict()
    
    data['school_id'] = [int(x.get('href').split('/')[2]) for x in atags]
    data['school_name'] = [x.text for x in atags]
    data['season'] = [season]*len(atags)
    data['division'] = [division]*len(atags)
    
    team_data = pd.DataFrame(data)
    
    with open('ncaa_scrapers\\csv\\school_divs.csv', 'ab') as csvfile:
        team_data.to_csv(csvfile, index = False, header = False)
        
    print('\t\t' + str(division))