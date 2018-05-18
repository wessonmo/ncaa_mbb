import pandas as pd
import re
from fuzzywuzzy import process, fuzz



rosters = pd.read_csv('rosters\csv\\imputed_rosters.csv')

game_ids = pd.read_csv('ncaa_data\\csv\\schedules.csv')

#game_ids with 2 DI teams
game_id_set = pd.merge(game_ids.loc[~pd.isnull(game_ids.school_id) & ~pd.isnull(game_ids.opp_id)], rosters[['season_id','school_id']].drop_duplicates(), how = 'left', on = ['season_id','school_id'], indicator = 'school')\
    .merge(rosters[['season_id','school_id']].drop_duplicates(), how = 'left', left_on = ['season_id','opp_id'], right_on = ['season_id','school_id'], indicator = 'opp')
game_id_season = game_id_set.loc[:,['season_id','game_id']].drop_duplicates()  
    
game_id_set = set(game_id_set.loc[(game_id_set.school == 'both') & (game_id_set.opp == 'both')].game_id)

box_scores = pd.read_csv('ncaa_data\\csv\\box_scores.csv')
box_scores = box_scores.loc[box_scores.game_id.isin(game_id_set) & ~pd.isnull(box_scores.period)]

pbps_raw = pd.read_csv('ncaa_data\\csv\\pbps.csv')
pbps_raw = pbps_raw.loc[~pd.isnull(pbps_raw.period)]
pbps_raw = pbps_raw.loc[pbps_raw.game_id.isin(box_scores.game_id.unique())]
pbps_raw = pbps_raw.loc[~pbps_raw.game_id.isin([4274098, 4042151,1449131,2833067])]

pbps = pbps_raw.copy()

pbps.loc[:,'order'] = pbps.groupby('game_id').cumcount()


#season stuff
game_seasons = game_ids.loc[:,['season_id','game_id']].drop_duplicates()
if game_seasons.groupby('game_id').agg('count').reset_index()\
    .sort_values('season_id', ascending = False).iloc[0].season_id > 1:
        raise ValueError('Duplicate Game IDs')

pbps = pd.merge(pbps, game_seasons, how = 'left', on = 'game_id')


#score stuff
if len(set(pbps.loc[pbps.score.apply(lambda x: True if x == '-' else False)].game_id.unique())\
       - set([4312939, 4312978, 4313095])) > 0:

    issues = list(set(pbps.loc[pbps.score.apply(lambda x: True if x == '-' else False)].game_id.unique())\
       - set([4312939, 4312978, 4313095]))

    raise ValueError('Problematic score format in game_ids: {0}'.format(issues))

else:

    pbps.score = pbps.score.apply(lambda x: None if x == '-' else x)

    pbps.score = pbps.groupby(['game_id']).score.ffill().fillna('0-0')

#for x in pbps.time.drop_duplicates():':' not in x
# time stuff
if len(set(pbps.loc[pbps.time.apply(lambda x: ':' not in x)].game_id.unique()) - set([3957358])) > 0:

    issues = list(set(pbps.loc[pbps.time.apply(lambda x: ':' not in x)].game_id.astype(int).unique()) - set([3957358]))

    raise ValueError('Problematic time format in game_ids: {0}'.format(issues))

else:

    pbps = pbps.loc[pbps.time.apply(lambda x: ':' in x)]

    pbps.loc[:,'seconds_remaining'] = pbps.apply(lambda x: int(x.time.split(':')[0])*60 + int(x.time.split(':')[1])
        + max((2 - x.period)*20*60,0), axis = 1)

#player_re = re.compile('[A-Z]+([, \-]{1,2}[A-Z]{2,})+(?= )')
player_re = re.compile('[A-Z,\- \.\']{6,}?((?= [A-Z][a-z])|(?= [a-z]))')
event_re = re.compile('[A-Z][a-z]+( [A-Z][a-z]+)*')

for school in [1,2]:

    temp = pbps.loc[~pd.isnull(pbps['school{0}_event'.format(school)])].rename(index = str, columns =
                    {'school{0}_id'.format(school): 'school_id', 'school{0}_event'.format(school): 'event'})
    
    temp.loc[:,'school_points'] = temp.score.apply(lambda x: int(x.split('-')[school - 1]))
    temp.loc[:,'opp_points'] = temp.score.apply(lambda x: int(x.split('-')[abs(school - 2)]))
    temp.loc[:,'player_name'] = temp.event.apply(lambda x: player_re.search(x).group(0) if player_re.search(x) else None)
    temp.loc[:,'event_type'] = temp.event.apply(lambda x: event_re.search(x).group(0) if event_re.search(x)
        else ('Defensive Points' if x == 'TEAM DEF PTS' else None))
    temp.loc[:,'shot'] = temp.event.apply(lambda x: re.compile('made|missed').search(x).group(0)
            if re.compile('made|missed').search(x) else None)

    temp = temp.loc[:,['season_id','game_id','order','period','seconds_remaining','school_id','school_points','opp_points',
                       'event','player_name','event_type','shot']]

    pbps2 = pd.concat([pbps2, temp]) if 'pbps2' in locals().keys() else temp
# print(pbps2.head())

player_names = pbps2.loc[~pd.isnull(pbps2.player_name),['season_id','school_id','player_name']].drop_duplicates()

#player match stuff
for season_id in player_names.season_id.unique():
    for school_id in player_names.loc[player_names.season_id == season_id].school_id.unique():
        roster = rosters.loc[(rosters.season_id == season_id) & (rosters.school_id == school_id)]
        if len(roster) == 0: continue
        roster.loc[:,'player_name'] = roster.player_name.apply(lambda x: re.sub(',', '', x.lower()))
        roster.loc[:,'player_name'] = roster.player_name.apply(lambda x: re.sub('[\.]|( jr)| i{2,}', '', x.lower()))
        

        pbp_names = player_names.loc[(player_names.season_id == season_id) & (player_names.school_id == school_id)]\
            .player_name.unique()

        for name in pbp_names:
            name_scrub = ' '.join(reversed(re.sub('^(i{2,}|jr),','',name.lower()).split(' ')))\
                if re.compile('^(i{2,}|jr),').search(name.lower())\
                else re.sub('( |,)([iv]{2,3}|jr)(?=,)|(?<=,)([iv]{2,3}|jr)( |$)', '', name.lower())
            name_scrub = re.sub(' {2,}',' ',re.sub(',', ' ', name_scrub)).strip()
#            print(name_scrub)
            match = process.extractOne(name_scrub, set(roster.player_name), scorer = fuzz.partial_ratio)
            
            if match[1] >= 80:
                roster_row = roster.loc[roster.player_name == match[0]].iloc[0]
                player_names.loc[(player_names.season_id == season_id)
                & (player_names.school_id == school_id) & (player_names.player_name == name),'player_id']\
                    = int(roster_row.player_id)
            else:
                sec_match = process.extractOne(re.sub('jr', 'junior', name_scrub), set(roster.player_name), scorer = fuzz.token_set_ratio)
                if sec_match[1] >= 95:
                    roster_row = roster.loc[roster.player_name == sec_match[0]].iloc[0]
                    player_names.loc[(player_names.season_id == season_id)
                    & (player_names.school_id == school_id) & (player_names.player_name == name),'player_id']\
                        = int(roster_row.player_id)
                else:
                    print(name_scrub, match, season_id, school_id, 'FAIL', sec_match[1])
                


#print(player_names.head())

agg_check = player_names.groupby(['season_id','school_id','player_name']).agg('count')\
.reset_index().sort_values('player_id', ascending = False)

if agg_check.player_id.iloc[0] > 1:
    raise ValueError('too many player_ids matched to one player')
else:
    player_names.loc[~pd.isnull(player_names.player_id), 'ncaa_id'] = True
    
    miss_names = player_names.loc[pd.isnull(player_names.player_id)].player_name.unique()
    max_id = player_names.loc[~pd.isnull(player_names.player_id)].player_id.max()
    for i, name in enumerate(miss_names):
        player_names.loc[player_names.player_name == name,['player_id','ncaa_id']] = i + 1 + max_id, False
        

pbps3 = pd.merge(pbps2, player_names.drop('ncaa_id', axis = 1), how = 'left', on = ['season_id', 'school_id', 'player_name'])
pbps3.loc[:,'sub_event'] = pbps3.event_type.apply(lambda x: True if x in ['Enters Game','Leaves Game'] else False)
pbps3.loc[:,'pot_deadball_event'] = pbps3.event_type.apply(lambda x: True if x in
          ['Commits Foul','Deadball Rebound','Free Throw','Media Timeout','Second Timeout','Team Timeout','Timeout',
           'Tip In','Turnover'] else False)
pbps3.sort_values(['season_id','game_id','period','seconds_remaining'], ascending = [True,True,True,False]).reset_index()



##lineup determinations
#lineup = pd.merge(lineup, game_id_season, how = 'left', on = 'game_id')
                        
                        
                        
starters = box_scores.loc[box_scores.order <= 4,['game_id','period','school_id','player_id']].copy()
starters.loc[:,'event_type'] = 'Enters Game'
starters.loc[:,'seconds_remaining'] = starters.period.apply(lambda x: (3 - x)*20*60)

lineup = pd.concat([starters,pbps3.sort_values(['game_id','school_id','period','seconds_remaining'], ascending = [True, True, True,False]).reset_index(drop = True)
    


#lineup.loc[:,'sub_event'] = lineup.event_type.apply(lambda x: True if x in ['Enters Game','Leaves Game'] else False)
#lineup.loc[:,'pot_deadball_event'] = lineup.event_type.apply(lambda x: True if x in
#          ['Commits Foul','Deadball Rebound','Free Throw','Media Timeout','Second Timeout','Team Timeout','Timeout',
#           'Tip In','Turnover'] else False)
lineup.loc[~lineup.sub_event & (lineup.seconds_remaining == 2400),'seconds_remaining'] = 2399
lineup.sort_values(['season_id','school_id','game_id','school_id','period','seconds_remaining','sub_event'], ascending = [True,True,True, True, True,False,True], inplace = True)

phase_ids = lineup.loc[lineup.sub_event,['season_id','school_id','game_id', 'period', 'seconds_remaining','sub_event']]\
    .drop_duplicates().sort_values(['season_id','school_id','game_id', 'period', 'seconds_remaining'],
                    ascending = [True, True, True, True, False])
phase_ids.loc[:,'phase_id'] = range(len(phase_ids))

lineup = pd.merge(lineup, phase_ids, how = 'left', on = ['season_id','school_id','game_id','period','seconds_remaining','sub_event'])\
    .sort_values(['season_id','school_id','game_id', 'period', 'seconds_remaining','sub_event'], ascending = [True, True, True, True, False, True]).reset_index(drop = True)
lineup.loc[:,'phase_id'] = lineup.groupby(['season_id','school_id','game_id']).phase_id.ffill()

for season_id in lineup.season_id.unique():
    for school_id in lineup.loc[lineup.season_id == season_id].school_id.unique():
        team_data = lineup.loc[(lineup.season_id == season_id) & (lineup.school_id == school_id)]\
            .sort_values(['game_id', 'period', 'seconds_remaining','sub_event'], ascending = [True, True, False, True]).reset_index(drop = True)
        
        for player_id in team_data.player_id.unique():
            team_data.loc[team_data.sub_event, str(int(player_id))] =\
                team_data.loc[team_data.sub_event].apply(lambda x: (1 if x.event_type == 'Enters Game' else 0)
                    if x.player_id == player_id else None, axis = 1)
            
            team_data.loc[:, str(int(player_id))] = team_data.groupby(['game_id','period'])[str(int(player_id))].ffill()
            team_data.loc[:, str(int(player_id))] = team_data.loc[:,str(int(player_id))].fillna(0)
        
        team_data.loc[:,'lineup_count'] = team_data.apply(lambda x:
            sum([x[str(int(col))] for col in team_data.player_id.unique()]), axis = 1)
            
        for player_id in team_data.player_id.unique():
            miss_sub_in = team_data.loc[~team_data.sub_event & (team_data.player_id == player_id) & (team_data[str(int(player_id))] == 0)]
            if len(miss_sub_in) > 0:
                print('missing sub in')
                break
            
        team_data.loc[~team_data.sub_event & (team_data.lineup_count != 5)].phase_id.unique()
        team_data.loc[team_data.phase_id == 7233]
#        team_data.loc[:,'lineup_error'] = team_data.apply(lambda x:
#            False if sum([x[str(int(col))] for col in team_data.player_id.unique()]) == 5 else True, axis = 1)
            
        
            
        
        
        

#for game_id in sorted(lineup.game_id.unique()):
#    for school_id in sorted(lineup.loc[lineup.game_id == game_id].school_id.unique()):
#        game_data = lineup.loc[(lineup.game_id == game_id) & (lineup.school_id == school_id)]
#        for id_ in game_data.player_id.unique(): game_data.loc[:, str(int(id_))] = None
#        
#        for phase_id in sorted(lineup.loc[(lineup.game_id == game_id) & (lineup.school_id == school_id)].phase_id.unique()):
#            for index, row in game_data.loc[(game_data.phase_id == phase_id) & game_data.sub_event].iterrows():
#                game_data.loc[index, str(int(row.player_id))] = 1 if row.event_type == 'Enters Game' else 0
#            
#        for id_ in game_data.player_id.unique():
#            game_data.loc[:, str(int(id_))] = game_data.groupby('period')[str(int(id_))].ffill()
#            game_data.loc[:, str(int(id_))] = game_data.groupby('period')[str(int(id_))].fillna(0)
#            
#        game_data.loc[:,'lineup_count'] = game_data.apply(lambda x:
#            sum([x[str(int(col))] for col in game_data.player_id.unique()]), axis = 1)
#            
#        broken_events = game_data.loc[(game_data.lineup_count != 5) & ~game_data.sub_event].phase_id.unique()
#        if len(broken_events) > 0:
#            break
#        else:
#            print(game_id, school_id, len(broken_events))
#            
#        for phase_id in sorted(lineup.loc[(lineup.game_id == game_id) & (lineup.school_id == school_id)].phase_id.unique()):
#            game_data.loc[(game_data.phase_id == phase_id) & ~game_data.sub_event]
#
#lineup.loc[pd.isnull(lineup.school_id)]






#     subs = pbps.loc[pbps['{0}sub'.format(school_string)].apply(lambda x: True if x != 0 else False),
#                          ['game_id','period','time']].drop_duplicates(keep = 'first')
#     subs.loc[:,'{0}session_change'.format(school_string)] = 1
#     subs.drop(['game_id','period','time'], axis = 1, inplace = True)

#     pbps2 = pd.merge(pbps, subs, how = 'left', left_index = True, right_index = True)
#     pbps2.loc[:,'{0}session_id'.format(school_string)] = pbps2['{0}session_change'.format(school_string)].fillna(0).cumsum().astype(int)


# for season_id in sorted(game_ids.season_id.unique()):

#     for school_id in sorted(game_ids.loc[game_ids.season_id == season_id].school_id.unique()):

#         tm_game_ids = [x for x in
#             sorted(game_ids.loc[(game_ids.season_id == season_id) & (game_ids.school_id == school_id)].game_id)
#             if x in set(pbps.game_id.unique())]
#         tm_roster = rosters.loc[(rosters.season_id == season_id) & (rosters.school_id == school_id)]

#         for game_id in tm_game_ids:

#             for period in [1,2]:

#                 side = 1 if pbps.loc[pbps.game_id == game_id,'school1_id'].iloc[0] == school_id else 2

#                 variables = [x for x in pbps.columns if x in ['period','time','school{0}_event'.format(side)]]

#                 team_pbp = pbps.loc[(pbps.game_id == game_id) & ~pd.isnull(pbps['school{0}_event'.format(side)]),variables]\
#                     .rename(columns = {'school{0}_event'.format(side): 'event'})

#                 team_pbp.loc[:,'player_name'] = team_pbp.event.apply(lambda x: re.compile('[A-Z\-]+,[A-Z]+').search(x).group(0)
#                 if re.compile('[A-Z\-]+,[A-Z]+').search(x) else None)

#                 team_pbp.loc[:,'roster_name'] = team_pbp.player_name.apply(lambda x: process.extractOne(
#                         re.sub(',',' ',x.lower()), [re.sub('[\-\.,]','',y.lower()) for y in tm_roster.player_name])[0]
#                         if not pd.isnull(x) else None)

#                 team_pbp.loc[:,'subs'] = team_pbp.event.apply(lambda x: (1 if re.compile('Enters').search(x) else 0)
#                     if re.compile('Enters|Leaves').search(x) else None)

#                 for x in [re.sub('[\-\.,]','',x.lower()) for x in tm_roster.player_name]: team_pbp.loc[:,x] = None

#                 for idx, row in team_pbp.loc[~pd.isnull(team_pbp.subs)].iterrows(): team_pbp.at[idx, row.roster_name] = row.subs

#                 for col in [re.sub('[\-\.,]','',x.lower()) for x in tm_roster.player_name]:

#                     if len(team_pbp.loc[~pd.isnull(team_pbp[col]),col]) > 0:

#                         first_index = team_pbp.loc[~pd.isnull(team_pbp[col])].iloc[0].name
