import pandas as pd
import csv
import geopy
import re

#geolocator = geopy.geocoders.GoogleV3(api_key = 'AIzaSyAQYCb7_Xff1jvyRC36UozcHEW0_-5WgLk')
geolocator = geopy.geocoders.GoogleV3(api_key = 'AIzaSyBWdX0_taBnOF8jMsTUTO1ILb7i42inzXg')

school_info = pd.read_csv('csv\\school_info.csv', header = 0)

site_re = re.compile('([a-z]+( |-))*[a-z]+(,( )*([a-z]+ )*[a-z]+)+', re.I)

games = pd.read_csv('csv\\games.csv', header = 0)
games = games.loc[pd.isnull(games.site) == False]

for df, var in zip([school_info, school_info, games],['city','arena','site']):
    try:
        exist = pd.read_csv('csv\\' + re.sub('y','ie',var) + 's.csv', header = 0)
    except:
        with open('csv\\' + re.sub('y','ie',var) + 's.csv', 'wb') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
            csvwriter.writerow([var, 'latitude', 'longitude'])
        exist = pd.read_csv('csv\\' + re.sub('y','ie',var) + 's.csv', header = 0)
        
    needed_loc = set(df[var]) - set(exist.loc[pd.isnull(exist.latitude) == False,var])
        
    for loc in list(needed_loc):
        latlong = (None,None)
        for i in range(3):
            try:
                latlong = geolocator.geocode(loc)[1]
                break
            except geopy.exc.GeocoderTimedOut:
                continue
            except TypeError:
                break
            
        if (latlong == (None,None)) and (var == 'site') and site_re.search(loc):
            loc_clean = site_re.search(re.sub('-',' ',loc)).group(0)
#            print(loc_clean)
            for i in range(2):
                try:
                    latlong = geolocator.geocode(loc_clean)[1]
                    break
                except geopy.exc.GeocoderTimedOut:
                    continue
                except TypeError:
                    break
            
        if latlong != (None,None):
            with open('csv\\' + re.sub('y','ie',var) + 's.csv', 'ab') as csvfile:
                csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
                csvwriter.writerow([loc,latlong[0],latlong[1]])
#        else:
#            print(loc)