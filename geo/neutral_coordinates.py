import pandas as pd
import csv
import geopy
import re

geolocator = geopy.geocoders.GoogleV3(api_key = 'AIzaSyAQYCb7_Xff1jvyRC36UozcHEW0_-5WgLk')
#geolocator = geopy.geocoders.GoogleV3(api_key = 'AIzaSyBWdX0_taBnOF8jMsTUTO1ILb7i42inzXg')

site_re = re.compile('([a-z]+( |-))*[a-z]+(,( )*([a-z]+ )*[a-z]+)+', re.I)

sites = pd.read_csv('ncaa_scrapers\\csv\\games.csv', header = 0)
sites = sites.loc[pd.isnull(sites.site) == False,['site']].drop_duplicates()
sites.loc[:,'site_sec'] = sites.site.apply(lambda x: site_re.search(x).group(0) if site_re.search(x) else None)

ncaa_sites = set(pd.read_csv('kaggle\\csv\\ncaa_sites.csv', header = 0).Site)
for ncaa_site in [x for x in ncaa_sites if x not in set(sites.site)]:
    sites.loc[len(sites),] = (ncaa_site,None)

try:
    exist = pd.read_csv('geo\\csv\\neutral_coord.csv', header = 0)
except:
    with open('geo\\csv\\neutral_coord.csv', 'wb') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
        csvwriter.writerow(['site','latitude','longitude'])
    exist = pd.read_csv('geo\\csv\\neutral_coord.csv', header = 0)
    
needed = pd.merge(sites, exist, how = 'left', on = ['site'], indicator = 'exist')

for site in set(needed.loc[needed.exist == 'left_only'].site):
    latlong = (None,None)
    for i in range(4):
        try:
            latlong = geolocator.geocode(site_re.search(site).group(0) if site_re.search(site) and i < 2 else site)[1]
            break
        except geopy.exc.GeocoderTimedOut:
            continue
        except TypeError:
            continue

    needed.loc[(needed.exist == 'left_only') & (needed.site == site),['latitude','longitude']] = latlong
        
    with open('geo\\csv\\neutral_coord.csv', 'ab') as csvfile:
        needed.loc[(needed.exist == 'left_only') & (needed.site == site),['site','latitude','longitude']]\
            .to_csv(csvfile, header = False, index = False)