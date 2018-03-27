import pandas as pd
import csv
import geopy

geolocator = geopy.geocoders.GoogleV3(api_key = 'AIzaSyAQYCb7_Xff1jvyRC36UozcHEW0_-5WgLk')
#geolocator = geopy.geocoders.GoogleV3(api_key = 'AIzaSyBWdX0_taBnOF8jMsTUTO1ILb7i42inzXg')

school_info = pd.read_csv('ncaa_scrapers\\csv\\school_info.csv', header = 0).drop(['capacity'], axis = 1)
school_info = school_info.loc[pd.isnull(school_info.city) == False]

try:
    exist = pd.read_csv('geo\\csv\\school_coord.csv', header = 0)
except:
    with open('geo\\csv\\school_coord.csv', 'wb') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_MINIMAL)
        csvwriter.writerow(['season','school_id','latitude','longitude'])
    exist = pd.read_csv('geo\\csv\\school_coord.csv', header = 0)
    
needed = pd.merge(school_info.loc[school_info.season < 2018], exist, how = 'left', on = ['season','school_id'], indicator = 'exist')

for city in set(needed.loc[needed.exist == 'left_only'].city):
    try:
        latlong = needed.loc[(needed.city == city) & (pd.isnull(needed.latitude) == False),['latitude','longitude']].iloc[1]
        print(city)
    except IndexError:
        latlong = (None,None)
        for i in range(4):
            try:
                latlong = geolocator.geocode(city)[1]
                print(city)
                break
            except geopy.exc.GeocoderTimedOut:
                continue
            except TypeError:
                continue

    needed.loc[(needed.exist == 'left_only') & (needed.city == city),['latitude','longitude']] = latlong
        
    with open('geo\\csv\\school_coord.csv', 'ab') as csvfile:
        needed.loc[(needed.exist == 'left_only') & (needed.city == city),['season','school_id','latitude','longitude']]\
            .to_csv(csvfile, header = False, index = False)