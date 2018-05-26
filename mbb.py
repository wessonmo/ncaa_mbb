import sqlalchemy
from ncaa_data.base_class import  base_class
from ncaa_data.params import params

def create_db_and_schema(db_name, schema_name):
    try:
        db_test = sqlalchemy.create_engine('postgresql://postgres:@localhost:5432/{0}'.format(db_name))
        db_test.connect()
        db_test.close()
    except Exception as err:
        if 'database "{0}" does not exist'.format(db_name) in str(err):
            engine = sqlalchemy.create_engine('postgresql://postgres:@localhost:5432/postgres')
            engine = engine.connect()
            engine.execute('commit')
            engine.execute('CREATE DATABASE {0}'.format(db_name))
            engine.close()

    engine = sqlalchemy.create_engine('postgresql://postgres:@localhost:5432/{0}'.format(db_name))
    engine.execute('CREATE SCHEMA IF NOT EXISTS {0};'.format(schema_name))

    return engine


def get_ncaa_data(engine, schema_name, storage_dir, max_season, max_division=1):
    seasons = range(2013, max_season + 1)
    divisions = range(1, max_division + 1)

    for data_type in ['team_index', 'team_info', 'roster', 'summary', 'box_score', 'pbp']:
        print(data_type)

        instance = base_class(
            schema_name, storage_dir, engine, data_type,
            href_frame = params[data_type]['href_frame'],
            url_ids = params[data_type]['url_ids'],
            parse_file_types = params[data_type]['parse_file_types'],
            scrape_table = params[data_type]['scrape_table'],
            seasons = seasons if data_type == 'team_index' else None,
            divisions = divisions if data_type == 'team_index' else None
        )
        instance.scrape_data()
        instance.parse_data()

if __name__ == '__main__':
    engine = create_db_and_schema(
        db_name = 'ncaa_mbb',
        schema_name = 'ncaa_data'
        )

    get_ncaa_data(
        engine = engine,
        schema_name = 'ncaa_data',
        storage_dir = 'E:\\ncaa_mbb\\ncaa_data',
        max_season = 2018
        )
