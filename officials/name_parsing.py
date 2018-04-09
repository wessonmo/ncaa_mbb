import pandas as pd
import re
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from collections import OrderedDict
from  __builtin__ import any

raw_df = pd.DataFrame()

for season in range(2012,2019):
    seas_df = pd.read_csv('ncaa_scrapers\\csv\\officials_' + str(season) + '.csv', header = 0)
    
    seas_df.loc[:,'officials'] = seas_df.officials.apply(lambda x: '' if pd.isnull(x) else x)
    seas_df.loc[:,'officials'] = seas_df.officials.apply(lambda x: None
          if (re.compile('[a-z]{5,}', re.I).search(x) == None) or re.compile('none ', re.I).search(x) else x)
    
    dt = pd.read_csv('ncaa_scrapers\\csv\\game_ids_' + str(season) + '.csv', header = 0)[['game_id','game_date']].drop_duplicates()
    dt.loc[:,'game_date'] = pd.to_datetime(dt.game_date)
    
    df = pd.merge(seas_df, dt, how = 'left', on = 'game_id')
    
    raw_df = pd.concat([raw_df,df.loc[pd.isnull(df.officials) == False]]).sort_values(['game_date','game_id']).reset_index(drop = True)

name_suffix = ', *((?=[js]r)|(?=ii)|(?=iii))(?! Neid)'
    
raw_df.loc[:,'off_scrub'] = raw_df.officials.apply(lambda x: re.sub(name_suffix,' ',x.lower()))

#[x for x in set(''.join(list(comb.loc[pd.isnull(comb.off_scrub) == False].off_scrub))) if re.compile('[a-z0-9]', re.I).search(x) == None]
#
#comb2.loc[comb2.officials.str.contains('(?<=\s)\s+', regex = True)]

sep = '\n[ ]+\n[ ]+| and | \' |\\n|&|(?<![a-z])-|,|/|;|\||:'

ref = '(r(ef(\.)*(eree)*)*[s0-9@!]*(?![a-z])+)'
ump = '(u(mp(\.)*(ire)*)*[\-s0-9@!]*)'
off = '(o(ff(\.)*(icial)*)*[s0-9@!]*)'
alt = '(a(lt(\.)*(ternative)*)*[s0-9@!]*)'

pre = '^\(*(' + '|'.join([ref,ump,off,alt]) + ')\)*[\-\s\)]+'
post = '[\-\s\(]+\(*(' + '|'.join([ref,ump,off,alt]) + ')\.*[\)_]*$'

punc = '([^a-z \-]+)|((?<![a-z])\-)|(\-(?![a-z]))|(?<=\s)\s+'

clean_df = pd.DataFrame(columns = ['game_id','game_date','order','official'])
for index, row in raw_df[['game_id','game_date','off_scrub']].iterrows():
    officials = [re.sub(pre +'|' + post + '|\s\".*?\"','',x.strip()) for x in
                 re.split(sep + '|[\\[\(\<].*?[\\]\)\>]', row.off_scrub)
                 if (len(re.findall(' ', x.strip())) > 0) and (re.compile(' [a-z]\.*$').search(x.strip()) == None)]
    
    if re.compile(punc).search(''.join(officials)):
        officials = [re.sub(punc, '', x).strip() for x in officials]
    
    clean_df = pd.concat([clean_df, pd.DataFrame(data = {'game_id': [row.game_id]*len(officials[:3]),
                                                         'game_date': [row.game_date]*len(officials[:3]),
                                                         'order': range(len(officials[:3])),
                                                         'official': officials[:3]})], ignore_index = True)

#[x for x in set(''.join(list(clean.loc[pd.isnull(clean.official) == False].official))) if re.compile('[a-z]', re.I).search(x) == None]
#
#clean.loc[clean.official.str.contains('lonnie', regex = True)]

name_str = ' (?!(jr|sr|ii))(?!(van|von) )(?!(mc|st) )(?!de la )'

for i in range(2):
    
    multiple = set(clean_df.loc[clean_df.official.apply(lambda x: len(re.findall(name_str, x)) > 2)].official)
    single = set(clean_df.loc[clean_df.official.apply(lambda x: len(x) >= 6)
        & clean_df.official.apply(lambda x: len(re.findall(name_str, x)) in [1,2])
        & clean_df.official.apply(lambda x: re.compile('^[a-z] [a-z\-]+$').search(x) == None)].official)
    
    for string in multiple:
        matches = [x[0] for x in process.extractBests(string, single, scorer = fuzz.partial_ratio, score_cutoff = 100)]
        matches = [x for x in matches if not any(x in y for y in set(matches) - set([x]))]
        
        if len(matches) > 0:
            new_string = string
            new_names = []
            
            for name in matches:
                string_start = string.find(name)
                string_end = string.find(name) + len(name)
                
                if 0 in [string_start, string_end]:
                    new_string = re.sub(name,'',new_string).strip()
                else:
                    new_string = re.sub(name,'&&',new_string).strip()
            
            new_names = matches + ([x.strip() for x in new_string.split('&&') if x.strip() != ''] if new_string != '' else [])
            new_names = [re.sub(pre +'|' + post,'',x.strip()) for x in new_names if re.sub(pre +'|' + post,'',x.strip()) != '']
            
            new_names_pos = [string.index(x) for x in new_names]
            new_names = [x for x in map(new_names.__getitem__, [new_names_pos.index(x) for x in sorted(new_names_pos)])
                            if re.compile('^[a-z\-]+$').search(x) == None]
            
            for index, row in clean_df.loc[clean_df.official == string].iterrows():
                clean_df.drop(index, inplace = True)
                
                data = OrderedDict()
                
                data['game_id'] = [row.game_id]*len(new_names)
                data['game_date'] = [row.game_date]*len(new_names)
                data['order'] = [int(row['order'] + x) for x in range(len(new_names))]
                data['official'] = new_names
                
                clean_df = pd.concat([clean_df,pd.DataFrame(data)]).sort_values(['game_date','game_id','order']).reset_index(drop = True)
                    
            print(string, new_names)
        
with open('officials\\csv\\parsed_names.csv', 'wb') as csvfile:
    clean_df.to_csv(csvfile, header = True, index = False)