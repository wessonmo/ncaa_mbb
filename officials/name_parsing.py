import pandas as pd
import re

ref = '(r(ef(\.)*(eree)*)*(s)*[0-9@!]*(?![a-z])+)'
ump = '(u(mp(\.)*(ire)*)*(s|\-)*[0-9@!]*)'
off = '(o(ff(\.)*(icial)*)*(s)*[0-9@!]*)'
alt = '(a(lt(\.)*(ternative)*)*(s)*[0-9@!]*)'
ref_str = '(^|\-|\s)(' + '|'.join([ref,ump,off,alt]) + ')(?=((\.)*\-|\s|:|,|;|$))'

enc_str = '(\(|<|\[|").*?(\)|>|\]|")+'

html_str = '(\\\\).*(\=)'

alt2 = '(^a(lt(\.)*)*(\-|\s))|((\-|\s)a(lt(\.)*)*$)'
ump2 = '(\(|\s)*u[0-9](_|\))*((?=$)|(?=\s))'

name_suffix = ', *((?=(j|s)r)|(?=ii)|(?=iii))(?! Neid)'

comb = pd.DataFrame()

for season in range(2012,2019):
    df = pd.read_csv('ncaa_scrapers\\csv\\officials_' + str(season) + '.csv', header = 0)
    
    comb = pd.concat([comb,df.loc[pd.isnull(df.officials) == False]]).sort_values(['game_id','officials']).reset_index(drop = True)

comb.loc[:,'off_scrub'] = comb.officials.apply(lambda x: re.sub('|'.join([ref_str,enc_str,html_str]), ';', x.lower()))
comb.loc[:,'off_scrub'] = comb.off_scrub.apply(lambda x: re.sub('\s+', ' ', x))
comb.loc[:,'off_scrub'] = comb.off_scrub.apply(lambda x: re.sub(name_suffix,' ',x))
comb.loc[:,'off_scrub'] = comb.off_scrub.apply(lambda x: re.sub('((?<=^[a-z])|(?<=( |,)[a-z]))\.(?=[a-z]{4,})','. ',x))
comb.loc[:,'off_scrub'] = comb.off_scrub.apply(lambda x: re.sub(', (?!and )(?!& )',',',x))

#[x for x in set(''.join(list(comb.loc[pd.isnull(comb.off_scrub) == False].off_scrub))) if re.compile('[a-z0-9]', re.I).search(x) == None]
#
#comb.loc[comb.off_scrub.str.contains(',&', regex = True)]

sep = ' and | \' |\\n|&|(?<![a-z])-(?![a-z])|,|/|;|\||:'
name_str = ' (?!(jr|sr|ii))(?!(van|von) )(?!(mc|st) )'

clean = pd.DataFrame(columns = ['game_id','official'])
for index, row in comb.loc[comb.officials.str.contains('[a-z]{5,}', regex = True),['game_id','off_scrub']].iterrows():
#    if row.game_id == 4286046:
#        break
    officials = [re.sub('|'.join([alt2,ump2]),'',x).strip() for x in re.split(sep, row.off_scrub)
                    if (re.compile('[a-z]', re.I).search(x))]
    
    if (len(officials) == 1) and len(re.findall(name_str, officials[0])) in [5,7]:
        names = [x for x in re.split(name_str,officials[0]) if x not in ['',None]]
        
        officials = [' '.join(names[0:2]).strip(),' '.join(names[2:4]).strip(),' '.join(names[4:6]).strip()]\
            + ([] if len(re.findall(name_str, officials[0])) == 5 else [' '.join(names[6:8]).strip()])
            
    multiple = [x for x in officials if len(re.findall(name_str, x)) == 3]
    if len(multiple) > 0:
        names = [y for y in re.split(name_str,x) for x in multiple if y not in ['',None]]
        
        for string in multiple:
            idx = officials.index(string)
            officials[idx:idx + 1] = ' '.join(names[0:2]).strip(),' '.join(names[2:4]).strip()
    
    clean = pd.concat([clean, pd.DataFrame(data = {'game_id': [row.game_id]*len(officials[:3]),
                                                   'official': officials[:3]})], ignore_index = True)

#[x for x in set(''.join(list(clean.loc[pd.isnull(clean.official) == False].official))) if re.compile('[a-z]', re.I).search(x) == None]
#
#clean.loc[clean.official.str.contains('\-')]
#
#
#clean.loc[:,'len'] = clean.official.apply(lambda x: len(x))
#clean.loc[:,'spaces'] = clean.official.apply(lambda x: len(re.findall(name_str, x)))
#
#clean.loc[(clean.official.str.contains('\.| ', regex = True)) & (clean.spaces.isin([1,3,5]))].sort_values('len', ascending = False).head(15)
    
punc = '([^a-z \-]+)|((?<![a-z])\-)|(\-(?![a-z]))'
    
clean.loc[:,'official'] = clean.official.apply(lambda x: re.sub(punc, '', x).strip())

with open('officials\\csv\\parsed_names.csv', 'wb') as csvfile:
    clean.drop_duplicates().to_csv(csvfile, header = True, index = False)