from __future__ import print_function
import os.path
import pandas as pd
from collections import OrderedDict
from bs4 import BeautifulSoup
import re
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import Select
import sys
    
needed = pd.read_csv('csv\\school_index.csv', header = 0)[['season','school_id']]

filenames = ['facilities','coaches','schedules']

for filename in filenames:
            
    file_loc = 'csv\\{0}.csv'.format(filename)
    
    if os.path.isfile(file_loc.format(filename)):
        
        file_df = pd.read_csv(file_loc, header = 0)[['season','school_id']].drop_duplicates()
        
        needed = pd.merge(needed, file_df, how = 'left', on = ['season','school_id'], indicator = filename)
        
    else:
        
        needed.loc[:,filename] = 'left_only'
        
left = needed.loc[needed.apply(lambda x: 'left_only' in [x.facilities, x.coaches, x.schedules],
                               axis = 1)]


home_url = 'http://web1.ncaa.org/stats/StatsSrv/careersearch'

options = Options()
options.add_argument('--headless')

driver = webdriver.Firefox(firefox_options = options)


completed = len(needed) - len(left)
for index, row in left.sort_values(['season','school_id']).iterrows():
    
    sys.stdout.flush()
    perc_string = '%5.2f'%(float(completed)/len(needed)*100)
    print('Facilities, Coaches, and Schedules: {0}% Complete'.format(perc_string), end = '\r')
    
    driver.get(home_url)
    
    Select(driver.find_element_by_name('searchOrg')).select_by_value(str(row.school_id))
    Select(driver.find_element_by_name('academicYear')).select_by_value(str(row.season))
    Select(driver.find_elements_by_name('searchSport')[1]).select_by_value('MBB')
    
    button_xpath = '//input[@value="Search"]'
    
    driver.find_elements_by_xpath(button_xpath)[1].click()
    
    link_xpath = '//a[@href="javascript:showTeamPage(-100, {0}, \'MBB\', 1, {1});"]'\
        .format(row.season, row.school_id)
    
    driver.find_element_by_xpath(link_xpath).click()
    
            
    soup = BeautifulSoup(driver.page_source, 'lxml')
    
    dfs = []
    
    if row.facilities == 'left_only':
        arena_html = soup.find('b', text = 'Arena').find_parent('tbody')
        
        city = re.sub('(\xa0)+',' ',soup.find('b', text = 'Location').find_next().text.strip())
        arena = arena_html.find('b', text = 'Name').find_next().text.strip()
        capacity = int(re.sub(',','',arena_html.find('b', text = 'Capacity').find_next().text.strip()))
        
        fac = pd.DataFrame([[row.school_id, row.season, city, arena, capacity]],
                           columns = ['school_id','season','city','arena','capacity'])
        
        dfs.append(('facilities',fac))
        
        
    if row.coaches == 'left_only':
        
        coach_html = soup.find('b', text = re.compile('head coach(es)*', re.I)).find_parent('tbody')
        
        coach = OrderedDict()
        
        coach['school_id'] = [row.school_id]*len(coach_html.find_all('b', text = 'Name:'))
        coach['season'] = [row.season]*len(coach_html.find_all('b', text = 'Name:'))
        coach['coach_name'] = [re.sub('(\xa0)+',' ',x.find_next().find('a').text.strip())
            if x.find_next().find('a') != None else re.sub('(\xa0)+',' ',x.find_next().text.strip())
            for x in coach_html.find_all('b', text = 'Name:')]
        coach['coach_id'] = [int(re.compile('[0-9]+').search(x.find_next().find('a').get('href')).group(0))
            if x.find_next().find('a') != None else None for x in coach_html.find_all('b', text = 'Name:')]
        coach['record'] = [x.find_next().text for x in coach_html.find_all('b', text = 'Record')]
        
        coach = pd.DataFrame(coach)
        
        dfs.append(('coaches',coach))
        
        
    if row.schedules == 'left_only':
        
        record_xpath = '//a[@href="javascript:showRecord();"]'
        
        driver.find_element_by_xpath(record_xpath).click()
        
        soup = BeautifulSoup(driver.page_source, 'lxml')
        
        games = soup.find('td', text = 'Opponent').find_parent('tbody').find_all('tr', {'class': 'text'})
        games = [[y.contents for y in x.find_all('td')] for x in games]
        
        sched = OrderedDict()
        
        sched['school_id'] = [row.school_id]*len(games)
        sched['season'] = [row.season]*len(games)
        sched['opp_id'] = [int(re.compile('[0-9]+').search(x[0][1].get('href')).group(0))
            if re.compile('[0-9]+').search(x[0][1].get('href')) else None for x in games]
        sched['game_date'] = [x[1][0] for x in games]
        sched['school_score'] = [int(x[2][0]) for x in games]
        sched['opp_score'] = [int(x[3][0]) for x in games]
        sched['game_type'] = [x[4][0].strip() for x in games]
        sched['location'] = [x[5][0].strip() if x[5] else None for x in games]
        sched['ot'] = [int(x[6][0].strip().split(' ')[0]) if x[6][0].strip() != '-' else None for x in games]
        sched['attend'] = [int(re.sub(',','',x[7][0].strip())) for x in games]
        
        sched = pd.DataFrame(sched).drop_duplicates(['school_id','opp_id','game_date'], keep = "last")
        
        sched.loc[:,'game_date'] = pd.to_datetime(sched.loc[:,'game_date'])
        
        sched = sched.loc[(sched.game_date < '04-30-{0}'.format(row.season))
            & (sched.game_date > '10-01-{0}'.format(row.season - 1))]
        
        dfs.append(('schedules',sched))
        
        
    for filename2, df in dfs:
        
        file_loc2 = 'csv\\{0}.csv'.format(filename2)
        
        exist = os.path.isfile(file_loc2)
        
        with open(file_loc2, 'ab' if exist else 'wb') as csvfile:
            df.to_csv(csvfile, header = not exist, index = False)
            
            
    completed += 1

driver.quit()

sys.stdout.flush()
print('Facilities, Coaches, and Schedules: 100.00% Complete')