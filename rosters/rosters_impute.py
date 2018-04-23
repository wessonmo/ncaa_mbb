import pandas as pd

rosters = pd.read_csv('ncaa_scrapers\\csv\\rosters.csv')

rosters.loc[:,'ncaa_id'] = rosters.player_id.apply(lambda x: True if not pd.isnull(x) else False)

len_miss = len(rosters.loc[~rosters.ncaa_id])
max_id = rosters.loc[rosters.ncaa_id].player_id.max()

rosters.loc[~rosters.ncaa_id,'player_id'] = [x + max_id for x in range(len_miss)]

with open('csv\\imputed_rosters.csv', 'wb') as csvfile:
    rosters.to_csv(csvfile, header = True, index = False)