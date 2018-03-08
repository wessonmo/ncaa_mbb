import csv
import pandas as pd
from collections import OrderedDict
from bs4 import BeautifulSoup
import re
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By 
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC 
from selenium.common.exceptions import TimeoutException, NoSuchElementException

school_divs = pd.read_csv('csv\\school_divs.csv', header = 0).sort_values('school_name')

try:
    school_info = pd.read_csv('csv\\school_info.csv', header = 0)
except IOError as error:
    if str(error) == 'File csv\school_info.csv does not exist':
        with open('csv\\school_info.csv', 'wb') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
            csvwriter.writerow(['school_id','season','city','arena','capacity'])
        school_info = pd.read_csv('ncaa_scrapers\\csv\\school_info.csv', header = 0)
    else:
        raise error

try:
    games = pd.read_csv('csv\\games.csv', header = 0)
except IOError as error:
    if str(error) == 'File csv\games.csv does not exist':
        with open('csv\\games.csv', 'wb') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
            csvwriter.writerow(['school_id','season','opp_id','game_date','school_score','opp_score', 'location','site','ot','attend'])
        games = pd.read_csv('csv\\games.csv', header = 0)
    else:
        raise error

browser = webdriver.Firefox()

form_base_xpath = '/html/body/table[2]/tbody/tr/td[2]/form/table/tbody/tr[2]/td/table/tbody/'
results_page_xpath = '/html/body/form/table[2]/tbody/tr/td[1]/table/tbody/tr[6]/td[2]/a'

for school_id, i in zip(set(school_divs.school_id),range(len(set(school_divs.school_id)))):
    
    school_index = school_divs.loc[school_divs.school_id == school_id]
    school_name = school_index.school_name.iloc[0]
    school_seasons = set(school_index.season)
    
    info_needed = school_seasons - set(school_info.loc[school_info.school_id == school_id].season)
    games_needed = school_seasons - set(games.loc[games.school_id == school_id].season)
    
    if len(info_needed | games_needed) > 0:
        print(school_name + '\t\t' + str(i) + '/' + str(len(set(school_divs.school_id))))
    
    for season in sorted(info_needed | games_needed):
    
        browser.get('http://web1.ncaa.org/stats/StatsSrv/careersearch')
        
        try:
            WebDriverWait(browser, 30).until(EC.element_to_be_clickable((By.XPATH, form_base_xpath + 'tr[1]/td[2]/select')))
        except TimeoutException:
            raise ValueError('no forms')
        
        Select(browser.find_element_by_xpath(form_base_xpath + 'tr[1]/td[2]/select')).select_by_value(str(school_id))
        Select(browser.find_element_by_xpath(form_base_xpath + 'tr[2]/td[2]/select')).select_by_value(str(season))
        Select(browser.find_element_by_xpath(form_base_xpath + 'tr[3]/td[2]/select')).select_by_value('MBB')
        browser.find_element_by_xpath(form_base_xpath + 'tr[6]/td/input').click()
        
        try:
            WebDriverWait(browser, 30)
            browser.find_element(By.XPATH,'/html/body/form/table/tbody/tr/td/table/tbody/tr/td/table/tbody/tr[2]/td[1]/a')
        except TimeoutException:
            raise ValueError('form result issue')
        except NoSuchElementException:
            with open('csv\\school_info.csv', 'ab') as infocsv:
                infowriter = csv.writer(infocsv, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
                infowriter.writerow([school_id,season,None,None,None])
            with open('csv\\games.csv', 'ab') as gamescsv:
                gamewriter = csv.writer(gamescsv, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
                gamewriter.writerow([school_id,season,None,None,None,None,None,None,None,None])
            print('\t' + str(season))
            continue
        
        browser.find_element_by_xpath('/html/body/form/table/tbody/tr/td/table/tbody/tr/td/table/tbody/tr[2]/td[1]/a').click()
        
        try:
            WebDriverWait(browser, 30).until(EC.element_to_be_clickable((By.XPATH, results_page_xpath)))
        except TimeoutException:
            raise ValueError('team info issue')
        
        if season in info_needed:
            
            soup = BeautifulSoup(browser.page_source, 'lxml')
            school_loc = re.sub('\xa0\xa0',' ',soup.find('b', text = 'Location').find_parent('tr').find_all('td')[1].text.strip())
            try:
                arena = soup.find('b', text = 'Arena').find_parent('tbody').contents[2].contents[3].text.strip()
                capacity = int(re.sub(',','',soup.find('b', text = 'Capacity').find_parent('tr').find_all('td')[1].text.strip()))
            except AttributeError:
                arena, capacity = None, None
            
            with open('csv\\school_info.csv', 'ab') as infocsv:
                infowriter = csv.writer(infocsv, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
                infowriter.writerow([school_id,season,school_loc,arena,capacity])
            
        if season in games_needed:
            
            if browser.find_element_by_xpath(results_page_xpath).text == '0-0':
                with open('csv\\games.csv', 'ab') as gamescsv:
                    gamewriter = csv.writer(gamescsv, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
                    gamewriter.writerow([school_id,season,None,None,None,None,None,None,None,None])
                print('\t' + str(season))
                continue
            
            browser.find_element_by_xpath(results_page_xpath).click()
            
            try:
                WebDriverWait(browser, 30).until(EC.visibility_of_element_located((By.XPATH,
                    '/html/body/table/tbody/tr[3]/td/form/table[2]/tbody')))
            except TimeoutException:
                raise ValueError('team results issue')
            
            soup = BeautifulSoup(browser.page_source, 'lxml')
            results = soup.find('td', text = 'Opponent').find_parent('tbody').find_all('tr',
                        class_ = lambda x: x != 'schoolColors')
            
            data = OrderedDict()
            
            data['school_id'] = [school_id]*len(results)
            data['season'] = [season]*len(results)
            data['opp_id'] = [int(re.compile('(?<=\()[0-9]+(?=\))').search(x.find_all('td')[0].find('a').get('href')).group(0))
                                if x.find_all('td')[0].find('a') != None else '999999' for x in results]
            data['game_date'] = [x.find_all('td')[1].text for x in results]
            data['school_score'] = [int(x.find_all('td')[2].text) for x in results]
            data['opp_score'] = [int(x.find_all('td')[3].text) for x in results]
            data['location'] = [x.find_all('td')[4].text.strip() for x in results]
            data['site'] = [x.find_all('td')[5].text for x in results]
            data['ot'] = [int(x.find_all('td')[6].text.strip().split(' ')[0])
                                if x.find_all('td')[6].text.strip() != '-' else '' for x in results]
            data['attend'] = [int(re.sub(',','',x.find_all('td')[7].text)) for x in results]
            
            results_df = pd.DataFrame(data).sort_values(['game_date','location'])\
                            .drop_duplicates(['school_id','opp_id','game_date'], keep = "last")
                
            with open('csv\\games.csv', 'ab') as matchescsv:
                results_df.to_csv(matchescsv, header = False, index = False)
            
        print('\t' + str(season))
        
browser.quit()