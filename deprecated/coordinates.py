import pandas as pd
import csv
import geopy
import re

geolocator = geopy.geocoders.GoogleV3(api_key = 'AIzaSyAQYCb7_Xff1jvyRC36UozcHEW0_-5WgLk')
#geolocator = geopy.geocoders.GoogleV3(api_key = 'AIzaSyBWdX0_taBnOF8jMsTUTO1ILb7i42inzXg')

school_info = pd.read_csv('ncaa_scrapers\\csv\\school_info.csv', header = 0)
school_info = school_info.loc[pd.isnull(school_info.city) == False]
school_info.loc[:,'arena_comb'] = school_info.apply(lambda x: (x.arena + ', ' if pd.isnull(x.arena) == False else '') + x.city, axis = 1)
school_list = ['season','school_id']

site_re = re.compile('([a-z]+( |-))*[a-z]+(,( )*([a-z]+ )*[a-z]+)+', re.I)

sites = pd.read_csv('ncaa_scrapers\\csv\\games.csv', header = 0)
sites = sites.loc[pd.isnull(sites.site) == False,['site']].drop_duplicates()
sites.loc[:,'site_sec'] = sites.site.apply(lambda x: site_re.search(x).group(0) if site_re.search(x) else None)

ncaa_sites = set(pd.read_csv('kaggle\\csv\\ncaa_sites.csv', header = 0).site)
for ncaa_site in [x for x in ncaa_sites if x not in set(sites.site)]:
    sites.loc[len(sites),] = (ncaa_site,ncaa_site)

site_list = ['site']

for df, var_list, title in zip([school_info, sites],[school_list,site_list],['school','game']):
    try:
        exist = pd.read_csv('geo\\csv\\' + title + '_loc.csv', header = 0)
    except:
        with open('geo\\csv\\' + title + '_loc.csv', 'wb') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
            csvwriter.writerow(var_list + ['latitude','longitude'])
        exist = pd.read_csv('geo\\csv\\' + title + '_loc.csv', header = 0)
        
    needed_loc = df.loc[~df[var_list[-2]].isin(set(exist[var_list[-2]])),var_list[-2:]].drop_duplicates()
        
    for index, row in needed_loc.iterrows():
        latlong = (None,None)
        for i in range(4):
            try:
                latlong = geolocator.geocode(row[var_list[-2]] if i < 3 else row[var_list[-1]])[1]
                print(row[var_list[-2]] if i < 3 else row[var_list[-1]])
                break
            except geopy.exc.GeocoderTimedOut:
                continue
            except TypeError:
                continue
        
        latlong_df = df.loc[df[var_list[-2]] == row[var_list[-2]],var_list]
        latlong_df.loc[:,'latitude'] = latlong[0]
        latlong_df.loc[:,'longitude'] = latlong[1]
            
        with open('geo\\csv\\' + title + '_loc.csv', 'ab') as csvfile:
            latlong_df.to_csv(csvfile, header = False, index = False)    