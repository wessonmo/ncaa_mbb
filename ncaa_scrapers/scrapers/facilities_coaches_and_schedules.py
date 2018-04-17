from __future__ import print_function
import os.path
import pandas as pd
from collections import OrderedDict
from bs4 import BeautifulSoup
import re
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import Select
import multiprocessing as mp
import sys


def facility_scrape(row, soup):
    
    arena_html = soup.find('b', text = 'Arena').find_parent('tbody')
            
    city = re.sub('(\xa0)+',' ',soup.find('b', text = 'Location').find_next().text.strip())
    arena = arena_html.find('b', text = 'Name').find_next().text.strip()
    capacity = int(re.sub(',','',arena_html.find('b', text = 'Capacity').find_next().text.strip()))
    
    return pd.DataFrame([[row.school_id, row.season, city, arena, capacity]],
                        columns = ['school_id','season','city','arena','capacity'])
    
    
def coaches_scrape(row, soup):
    
    coach_html = soup.find('b', text = re.compile('head coach(es)*', re.I)).find_parent('tbody')
            
    data = OrderedDict()
    
    data['school_id'] = [row.school_id]*len(coach_html.find_all('b', text = 'Name:'))
    data['season'] = [row.season]*len(coach_html.find_all('b', text = 'Name:'))
    data['coach_name'] = [re.sub('(\xa0)+',' ',x.find_next().find('a').text.strip())
        if x.find_next().find('a') != None else re.sub('(\xa0)+',' ',x.find_next().text.strip())
        for x in coach_html.find_all('b', text = 'Name:')]
    data['coach_id'] = [int(re.compile('[0-9]+').search(x.find_next().find('a').get('href')).group(0))
        if x.find_next().find('a') != None else None for x in coach_html.find_all('b', text = 'Name:')]
    data['record'] = [x.find_next().text for x in coach_html.find_all('b', text = 'Record')]
    
    return pd.DataFrame(data)


def schedule_scrape(driver, row):
    
    record_xpath = '//a[@href="javascript:showRecord();"]'
            
    driver.find_element_by_xpath(record_xpath).click()
    
    soup = BeautifulSoup(driver.page_source, 'lxml')
    
    games = [x.find_all('td') for x in
        soup.find('td', text = 'Opponent').find_parent('tbody').find_all('tr', {'class': 'text'})]
    
    data = OrderedDict()
    
    data['school_id'] = [row.school_id]*len(games)
    data['season'] = [row.season]*len(games)
    data['opp_id'] = [int(re.compile('[0-9]+').search(y.get('href')).group(0)) if y else None
         for y in [x[0].find('a') for x in games]]
    data['game_date'] = [x[1].text for x in games]
    data['school_score'] = [int(x[2].text) for x in games]
    data['opp_score'] = [int(x[3].text) for x in games]
    data['game_type'] = [x[4].text.strip() for x in games]
    data['location'] = [x[5].text.strip() if x[5].text else None for x in games]
    data['ot'] = [int(x[6].text.strip().split(' ')[0]) if '-' not in x[6].text else None for x in games]
    data['attend'] = [int(re.sub(',','',x[7].text.strip())) for x in games]
    
    data = pd.DataFrame(data).drop_duplicates(['school_id','opp_id','game_date'], keep = "last")
    
    data.loc[:,'game_date'] = pd.to_datetime(data.loc[:,'game_date'])
    
    data = data.loc[(data.game_date < '04-30-{0}'.format(row.season))
        & (data.game_date > '10-01-{0}'.format(row.season - 1))]
    
    return data


def data_scrape(options, home_url, row):
    
    driver = webdriver.Firefox(firefox_options = options)
    
    driver.get(home_url)
        
    Select(driver.find_element_by_name('searchOrg')).select_by_value(str(row.school_id))
    Select(driver.find_element_by_name('academicYear')).select_by_value(str(row.season))
    Select(driver.find_elements_by_name('searchSport')[1]).select_by_value('MBB')
    
    button_xpath = '//input[@value="Search"]'
    
    driver.find_elements_by_xpath(button_xpath)[1].click()
    
    link_xpath = '//a[@href="javascript:showTeamPage(-100, {0}, \'MBB\', 1, {1});"]'.format(row.season, row.school_id)
    
    driver.find_element_by_xpath(link_xpath).click()
    
            
    soup = BeautifulSoup(driver.page_source, 'lxml')
    
    dict_ = {}
    
    
    if row.facilities == 'left_only':
        
        dict_['facilities'] = facility_scrape(row, soup)
        
        
    if row.coaches == 'left_only':
        
        dict_['coaches'] = coaches_scrape(row, soup)
        
        
    if row.schedules == 'left_only':
        
        dict_['schedules'] = schedule_scrape(driver, row)
    
    
    driver.quit()
    
    return dict_


def update(index):
    
    print(' Facilities, Coaches, and Schedules:', end = '\r')
    
    needed = index[['season','school_id']]
    
    filenames = ['facilities','coaches','schedules']
    
    for filename in filenames:
                
        file_loc = 'csv\\{0}.csv'.format(filename)
        
        if os.path.isfile(file_loc.format(filename)):
            
            file_df = pd.read_csv(file_loc, header = 0)[['season','school_id']].drop_duplicates()
            
            needed = pd.merge(needed, file_df, how = 'left', on = ['season','school_id'], indicator = filename)
            
        else:
            
            needed.loc[:,filename] = 'left_only'
            
    scraped = needed.loc[needed.apply(lambda x: 'left_only' not in [x.facilities, x.coaches, x.schedules], axis = 1)]
    
    left = needed.loc[needed.apply(lambda x: 'left_only' in [x.facilities, x.coaches, x.schedules], axis = 1)]
    
    
    if len(left) > 0:
        
        home_url = 'http://web1.ncaa.org/stats/StatsSrv/careersearch'
        
        options = Options()
        options.add_argument('--headless')
        
        
        finished = 0
        
        chunk_size = 20
        
        for section in range(0, len(left), chunk_size):
            
            percent_complete = '%5.2f'%(float(finished + len(scraped))/len(needed)*100)
                
            sys.stdout.flush()
            print(' Facilities, Coaches, and Schedules: {0}% Complete'.format(percent_complete, section), end = '\r')
            
            chunk = left.iloc[section : section + chunk_size]
            
                        
            pool = mp.Pool()
            
            results = [pool.apply_async(data_scrape, args = (options, home_url, row))
                for idx, row in chunk.iterrows()]
            output = [p.get() for p in results]
            
            
            for dict_ in output:
                
                for filetype in dict_.keys():
                    
                    df = dict_[filetype]
                
                    file_loc2 = 'csv\\{0}.csv'.format(filetype)
                
                    exist = os.path.isfile(file_loc2)
                
                    with open(file_loc2, 'ab' if exist else 'wb') as csvfile:
                        df.to_csv(csvfile, header = not exist, index = False)
            
                        
            finished += chunk_size
            
            
                    
                    
    sys.stdout.flush()
    print(' Facilities, Coaches, and Schedules: 100.00% Complete\n')