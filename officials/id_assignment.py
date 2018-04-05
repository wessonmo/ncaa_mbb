import pandas as pd
#import re
#from fuzzywuzzy import fuzz
from fuzzywuzzy import process

parsed_names = pd.read_csv('officials\\csv\\parsed_names.csv', header = 0)
parsed_names = parsed_names.loc[(pd.isnull(parsed_names.official) == False)]
parsed_names = parsed_names.loc[parsed_names.official.apply(lambda x: len(x) >= 6)]

agg = parsed_names.groupby('official').agg('count').reset_index()

popular = set(agg.loc[agg.game_id >= 25].official)
unpopular = set(agg.loc[agg.game_id < 5].official)

for name in unpopular:
    match = process.extractBests(name, popular - set([name]))
    if match[0][1] >= 90:
        print(name, match[0][0])
        