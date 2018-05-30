from params import params
from ncaa_data.get_ncaa_data import  get_ncaa_data
from rosters.impute_player_ids import impute_player_ids

if __name__ == '__main__':
    get_ncaa_data()
    impute_player_ids()
