from __future__ import print_function
from utils.url_req import url_req
from utils.print_update import print_update
import parsers
import os
from requests.exceptions import ConnectionError, ConnectTimeout, ReadTimeout
from bs4 import BeautifulSoup
import pandas as pd
import re
from sys import stdout
from params import params


class base_data_type(object):

    def __init__(self, data_type, href_frame, url_ids, parse_data_types=None, scrape_data_type=None):
        self.data_type = data_type
        self.url_frame = 'http://stats.ncaa.org{0}'.format(href_frame)
        self.url_ids = url_ids
        self.parse_data_types = parse_data_types if parse_data_types else [data_type]
        self.scrape_data_type = scrape_data_type

        self.storage_dir = params['mbb']['storage_dir']
        self.seasons = range(params['mbb']['min_season'], params['mbb']['max_season'] + 1)
        self.divisions = range(1, params['mbb']['max_division'] + 1)

        self.html_path = '{0}\\html\\{1}'.format(self.storage_dir, self.data_type)
        self.csv_path = '{0}\\csv'.format(self.storage_dir)


    def _create_storage_folder(self, folder_path):
        if not os.path.exists(folder_path): os.makedirs(folder_path)


    def _define_all_webpages(self):
        if self.data_type == 'team_index':
            self.all_pages = set((x,y) for x in self.seasons for y in self.divisions)
        else:
            csv_file = '{0}\\{1}.csv'.format(self.csv_path, self.scrape_data_type)
            if self.data_type == 'box_score':
                df = pd.read_csv(csv_file)[self.url_ids[0]].drop_duplicates()
                self.all_pages = set((x, y) for x in df.values for y in [1,2])
            elif self.data_type == 'conference':
                df = pd.read_csv(csv_file)[self.url_ids[1]].drop_duplicates()
                self.all_pages = set((params['mbb']['max_season'], x) for x in df.values)
            else:
                df = pd.read_csv(csv_file)[self.url_ids].drop_duplicates()
                self.all_pages = set(tuple(x) for x in df.values)

    def _define_scraped_webpages(self):
        self.scraped_pages = set(tuple(int(y) for y in x[:-5].split('_')) for x in os.listdir(self.html_path))

    def _define_remain_webpages(self):
        self.remain_pages = list(self.all_pages - self.scraped_pages)

        self.completed, self.total = len(self.scraped_pages), len(self.all_pages)

    def _scrape_remain_webpages(self):
        if len(self.remain_pages) > 0:
            write_path = '{0}\\{1}.html'.format(self.html_path, '{0}_{1}' if len(self.remain_pages[0]) > 1 else '{0}')

            for page in self.remain_pages:
                try:
                    resp = url_req(self.url_frame.format(*page))
                    with open(write_path.format(*page), 'wb') as f: f.write(resp)

                    self.completed += 1
                    print_update('Scrape', self.completed, self.total)
                except (ConnectionError, ConnectTimeout, ReadTimeout, UnboundLocalError):
                    continue

        stdout.flush()
        print('\t{0: >6} : {1: <14}'.format('Scrape', 'Complete'))

    def scrape_to_file(self):
        self._create_storage_folder(self.html_path)
        self._define_all_webpages()
        self._define_scraped_webpages()
        self._define_remain_webpages()
        self._scrape_remain_webpages()

    def _define_all_html_files(self):
        self.all_htmls = set(x for x in os.listdir(self.html_path)
            if os.path.getsize('{0}\\{1}'.format(self.html_path, x)) > 20480)

    def _define_parsed_html_files(self):
        csv_file = '{0}\\{1}.csv'.format(self.csv_path, self.data_type)
        try:
            df = pd.read_csv(csv_file)[self.url_ids].drop_duplicates()
            self.parsed_htmls = set('{0}.html'.format('_'.join([str(y) for y in x])) for x in df.values)
        except IOError as err:
            if str(err) == 'File {0} does not exist'.format(csv_file):
                self.parsed_htmls = set()
            else:
                raise err


    def _define_remain_html_files__single_parse(self):
        self.remain_htmls = list(self.all_htmls - self.parsed_htmls)

        self.completed, self.total = len(self.parsed_htmls), len(self.all_htmls)

    def _parse_remain_html_files__single_parse(self):
        if len(self.remain_htmls) > 0:
            csv_file = '{0}\\{1}.csv'.format(self.csv_path, self.data_type)
            csv_exist = os.path.exists(csv_file)

            for file_name in self.remain_htmls:
                with open('{0}\\{1}'.format(self.html_path, file_name), 'r') as html_file:
                    soup = BeautifulSoup(html_file.read(), 'lxml')

                if re.compile('something went wrong').search(soup.text): continue

                df = getattr(parsers, self.data_type)(file_name, soup)
                df.to_csv(csv_file, mode='a' if csv_exist else 'w', header=not csv_exist, index=False)
                csv_exist = True

                self.completed += 1
                print_update('Parse', self.completed, self.total)

        stdout.flush()
        print('\t{0: >6} : {1: <14}'.format('Parse', 'Complete'))

    def _define_remain_html_files__multi_parse(self):
        self.remain_htmls = {key: self.parse_data_types for key in self.all_htmls}
        for data_type in self.parse_data_types:
            csv_file = '{0}\\{1}.csv'.format(self.csv_path, data_type)
            try:
                df = pd.read_csv(csv_file)[self.url_ids].drop_duplicates()
                parsed_files = set('{0}.html'.format('_'.join([str(y) for y in x])) for x in df.values)
                for p_file in parsed_files:
                    self.remain_htmls[p_file] = [x for x in self.remain_htmls[p_file] if x != data_type]
            except IOError as err:
                if str(err) == 'File {0} does not exist'.format(csv_file):
                    self.parsed_htmls = set()
                else:
                    raise err
        for key in [key for key in self.remain_htmls if self.remain_htmls[key] == []]: del self.remain_htmls[key]

        self.completed, self.total = len(self.all_htmls) - len(self.remain_htmls.keys()), len(self.all_htmls)

    def _parse_remain_html_files__multi_parse(self):
        if len(self.remain_htmls) > 0:
            for file_name in self.remain_htmls:
                with open('{0}\\{1}'.format(self.html_path, file_name), 'r') as html_file:
                    soup = BeautifulSoup(html_file.read(), 'lxml')

                if re.compile('something went wrong').search(soup.text): continue

                for data_type in self.parse_data_types:
                    csv_file = '{0}\\{1}.csv'.format(self.csv_path, data_type)
                    csv_exist = os.path.exists(csv_file)

                    if data_type in self.remain_htmls[file_name]:
                        df = getattr(parsers, data_type)(file_name, soup)
                        df.to_csv(csv_file, mode='a' if csv_exist else 'w', header=not csv_exist, index=False)

                self.completed += 1
                print_update('Parse', self.completed, self.total)

        stdout.flush()
        print('\t{0: >6} : {1: <14}'.format('Parse', 'Complete'))

    def parse_to_csv(self):
        self._create_storage_folder(self.csv_path)
        self._define_all_html_files()
        if len(self.parse_data_types) == 1:
            self._define_parsed_html_files()
            self._define_remain_html_files__single_parse()
            self._parse_remain_html_files__single_parse()
        else:
            self._define_remain_html_files__multi_parse()
            self._parse_remain_html_files__multi_parse()

    def dedupe_csv(self):
        print('\t{0: >6} : {1: <14}'.format('Dedupe', ''), end='\r')
        for data_type in self.parse_data_types:
            csv_file = '{0}\\{1}.csv'.format(self.csv_path, data_type)
            df = pd.read_csv(csv_file).drop_duplicates()
            df.to_csv(csv_file, mode='w', header=True, index=False)
        print('\t{0: >6} : {1: <14}'.format('Dedupe', 'Complete'))
