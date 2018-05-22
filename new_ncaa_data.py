from __future__ import print_function
import sqlalchemy
from ncaa_data import new_scrapers
from sys import argv

if __name__ == '__main__':
    schema_name = argv[1]
    storage_dir = argv[2]
    max_season = int(argv[3])
    max_division = int(argv[3]) if len(argv) > 4 else 1

    seasons = range(2013, max_season + 1)
    divisions = range(1, max_division + 1)


    engine = sqlalchemy.create_engine('postgresql://postgres:@localhost:5432/ncaa_mbb2')
    engine.execute('CREATE SCHEMA IF NOT EXISTS {0};'.format(schema_name))

    part_inputs = (schema_name, storage_dir, engine)

    for data_type in ['team_index', 'team_info', 'roster','summary', 'box_score', 'pbp']:
        print(data_type)

        inputs = part_inputs + (data_type,)\
            + ((seasons, divisions) if data_type == 'team_index' else ())

        instance = getattr(new_scrapers, data_type)(*inputs)
        instance.scrape_data()
        instance.parse_data()
