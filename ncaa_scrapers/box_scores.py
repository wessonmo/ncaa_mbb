from ncaa_scrapers.functions.soupify import soupify
import csv
import pandas as pd
from collections import OrderedDict
import re
from time import time

box_score_var = ['FGM','FGA','3FG','3FGA','FT','FTA','Pts','ORB','DRB','TRB','AST','TO','STL','BLK','FL']

school_divs = pd.read_csv('ncaa_scrapers\\csv\\school_divs.csv', header = 0)

for season in range(2012,school_divs.season.max() + 1):
    
    all_game_ids = pd.read_csv('ncaa_scrapers\\csv\\game_ids_' + str(season) + '.csv', header = 0)
    all_game_ids = set(x for x in set(all_game_ids.loc[pd.isnull(all_game_ids.game_id) == False].game_id))
    
    try:
        scraped_game_ids = set(pd.read_csv('ncaa_scrapers\\csv\\box_scores_' + str(season) + '.csv', header = 0).game_id)
    except IOError as error:
        if str(error) == 'File ncaa_scrapers\\csv\\box_scores_' + str(season) + '.csv does not exist':
            with open('ncaa_scrapers\\csv\\box_scores_' + str(season) + '.csv', 'wb') as csvfile:
                csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
                csvwriter.writerow(['game_id','period','school_id','player_order','player_href','player_name','pos','min']
                    + box_score_var)
            scraped_game_ids = set()
        else:
            raise error
            
    game_ids_needed = sorted(all_game_ids - scraped_game_ids)
    num_needed = len(game_ids_needed)
    
    i, start = 1, time()
    for game_id in game_ids_needed:
        
        for period in [1,2]:
            soup = soupify('http://stats.ncaa.org/game/box_score/' + str(int(game_id)) + '?period_no=' + str(period))
            
            school_ids = [int(x.get('href').split('/')[2]) for x in soup.find_all('a', {'href': re.compile('\/team\/[0-9]+')})]
            
            box_scores = [x.parent.parent.find_all('tr', {'class': 'smtext'}) for x in soup.find_all('th', text = 'Player')]
            
            try:
                box_scores[0]
            except IndexError:
                with open('ncaa_scrapers\\csv\\box_scores_' + str(season) + '.csv', 'ab') as hrefscsv:
                    hrefwriter = csv.writer(hrefscsv, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
                    hrefwriter.writerow([game_id,period] + [None]*20)
                break
            
            for box_score_soup, school_id in zip(box_scores, school_ids):
                players = [x.find_all('td') for x in box_score_soup]
                
                if len(players) < 5:
                    with open('ncaa_scrapers\\csv\\box_scores_' + str(season) + '.csv', 'ab') as hrefscsv:
                        hrefwriter = csv.writer(hrefscsv, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
                        hrefwriter.writerow([game_id,period] + [None]*20)
                    continue
                
                data = OrderedDict()
                
                data['game_id'] = [game_id]*len(players)
                data['period'] = [period]*len(players)
                data['school_id'] = [school_id]*len(players)
                data['order'] = range(len(players))
                data['player_href'] = [None if x[0].find('a') == None else x[0].find('a').get('href') for x in players]
                data['player_name'] = [re.sub('[^\x00-\x7F]+','',x[0].text.strip().encode('utf8')) if x[0].has_attr('a') == False
                                            else x[0].find('a').text for x in players]
                data['pos'] = [None if x[1].text == '' else x[1].text for x in players]
                data['min'] = [0 if x[2].text.strip() == '' else 
                         int(x[2].text.strip().split(':')[0]) + float(x[2].text.strip().split(':')[1])/60 for x in players]
    
                for var, j in zip(box_score_var,range(3,18)):
                    data[var] = [0 if x[j].text.strip() == '' else int(x[j].text.strip()) for x in players]
                
                box = pd.DataFrame(data)
                
                with open('ncaa_scrapers\\csv\\box_scores_' + str(season) + '.csv', 'ab') as hrefscsv:
                    box.to_csv(hrefscsv, header = False, index = False)
        
        if i % 100 == 0:
            avg_time = (time() - start)/100
            start = time()
            print(str(season) + ' box_scores Remaining: ' + str(num_needed - i) + ' ' + str(avg_time*(num_needed - i)/60) + ' min')
        
        i += 1