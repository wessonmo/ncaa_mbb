import csv
import pandas as pd
from collections import OrderedDict
from bs4 import BeautifulSoup
import re
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import Select

def team_info(row):        
    options = Options()
    options.add_argument('--headless')

    driver = webdriver.Firefox(firefox_options = options)
        
    driver.get('http://web1.ncaa.org/stats/StatsSrv/careersearch')

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
        if soup.find('b', text = 'Arena'):
            arena_html = soup.find('b', text = 'Arena').find_parent('tbody')
            
            arena = arena_html.find('b', text = 'Name').find_next().text.strip()
            capacity = arena_html.find('b', text = 'Capacity').find_next().text.strip()
        else:
            arena, capacity = None, None
        
        dict_['facilities'] = pd.DataFrame([[row.season_id, row.school_id, arena, capacity]],
            columns = ['season_id','school_id','arena','capacity'])
        
    if row.coaches == 'left_only':
        coach_html = soup.find('b', text = re.compile('head coach(es)*', re.I)).find_parent('tbody')
            
        data = OrderedDict()

        data['season_id'] = [row.season_id]*len(coach_html.find_all('b', text = 'Name:'))
        data['school_id'] = [row.school_id]*len(coach_html.find_all('b', text = 'Name:'))
        data['coach_id'] = [int(re.compile('[0-9]+').search(x.find_next().find('a').get('href')).group(0))
            if x.find_next().find('a') != None else None for x in coach_html.find_all('b', text = 'Name:')]
        data['coach_name'] = [re.sub('(\xa0)+',' ',x.find_next().find('a').text.strip())
            if x.find_next().find('a') != None else re.sub('(\xa0)+',' ',x.find_next().text.strip())
            for x in coach_html.find_all('b', text = 'Name:')]
        data['record'] = [x.find_next().text for x in coach_html.find_all('b', text = 'Record')]
        
        dict_['coaches'] = pd.DataFrame(data)
        
    if row.schedules == 'left_only':
        record_xpath = '//a[@href="javascript:showRecord();"]'
            
        driver.find_element_by_xpath(record_xpath).click()
        
        soup = BeautifulSoup(driver.page_source, 'lxml')
        
        games = [x.find_all('td') for x in
            soup.find('td', text = 'Opponent').find_parent('tbody').find_all('tr', {'class': 'text'})]
        
        data = OrderedDict()
        
        data['season_id'] = [row.season_id]*len(games)
        data['school_id'] = [row.school_id]*len(games)
        data['date'] = [x[1].text for x in games]
        data['game_id'] = [None]*len(games)
        data['opp_id'] = [int(re.compile('[0-9]+').search(y.get('href')).group(0)) if y else None
             for y in [x[0].find('a') for x in games]]
        data['opp_text'] = ['{0}{1}'.format('@ ' if x[5].text.strip() == 'Away' else '', re.sub('%','',x[0].get_text()).strip()) for x in games]
        data['school_pts'] = [int(x[2].text) for x in games]
        data['opp_pts'] = [int(x[3].text) for x in games]
        data['ot'] = [int(x[6].text.strip().split(' ')[0]) if '-' not in x[6].text else None for x in games]
        
        data = pd.DataFrame(data).drop_duplicates(['school_id','opp_id','date'], keep = "last")
        
        data.loc[:,'date'] = pd.to_datetime(data.loc[:,'date'])
        
        data = data.loc[(data.date < '04-30-{0}'.format(row.season))
            & (data.date > '10-01-{0}'.format(row.season - 1))]
        
        dict_['schedules'] = data

    driver.quit()

    return dict_