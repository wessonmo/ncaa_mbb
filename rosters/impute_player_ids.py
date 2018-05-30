from params import params
import pandas as pd
import sqlalchemy
import os

storage_dir = params['mbb']['storage_dir']

def impute_player_ids():
    roster_imputed_path = '{0}\\roster\\csv\\roster_imputed.csv'.format(storage_dir)
    roster_imputed_exist = os.path.exists(roster_imputed_path)

    if roster_imputed_exist:
        team_index = pd.read_csv('{0}\\ncaa_data\\csv\\team_index.csv')[['season_id', 'school_id']]
        all_team_ids = set(tuple(x) for x in team_index.values)

        roster_imputed = pd.read_csv(impute_path)[['season_id', 'school_id']].drop_duplicates()
        imputed_team_ids = set(tuple(x) for x in roster_imputed.values)

        unimputed_teams = True if len(all_team_ids - imputed_team_ids) > 0 else False
    else:
        folder_path = '{0}\\roster\\csv'.format(storage_dir)
        if not os.path.exists(folder_path): os.makedirs(folder_path)

        unimputed_teams = True

    if not roster_imputed_exist or unimputed_teams:
        rosters_path = '{0}\\ncaa_data\\csv\\roster.csv'.format(storage_dir)
        rosters = pd.read_csv(rosters_path)

        rosters.loc[:,'ncaa_id'] = rosters.player_id.apply(lambda x: True if not pd.isnull(x) else False)

        len_miss = len(rosters.loc[~rosters.ncaa_id])
        max_id = rosters.loc[rosters.ncaa_id].player_id.max()

        rosters.loc[~rosters.ncaa_id,'player_id'] = [x + max_id for x in range(len_miss)]

        roster_imputed_path = '{0}\\roster\\csv\\roster_imputed.csv'.format(storage_dir)
        rosters.to_csv(roster_imputed_path, mode='w', header=True, index=False)
