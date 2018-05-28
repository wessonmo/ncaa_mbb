import sqlalchemy
from ncaa_data.base_class import  base_data_type
from params import params

def create_db_if_not_exist(db_name):
    try:
        db_check = sqlalchemy.create_engine('postgresql://postgres:@localhost:5432/{0}'.format(db_name))
        db_check.connect()
        db_check.close()
    except Exception as err:
        if 'database "{0}" does not exist'.format(db_name) in str(err):
            db_create = sqlalchemy.create_engine('postgresql://postgres:@localhost:5432/postgres')
            db_create.execute('commit')
            db_create.execute('CREATE DATABASE {0}'.format(db_name))
            db_create.close()

def get_ncaa_data(engine):
    for name in params['ncaa_data']['data_types_order']:
        pars = params['ncaa_data']['data_types'][name]

        print(name)

        data_type = base_data_type(
            engine = engine,
            data_type = name,
            href_frame = pars['href_frame'],
            url_ids = pars['url_ids'],
            parse_file_types = pars['parse_file_types'],
            scrape_table = pars['scrape_table']
            )
        data_type.scrape_data()
        data_type.parse_data()

if __name__ == '__main__':
    create_db_if_not_exist(db_name=params['mbb']['db_name'])
    engine = sqlalchemy.create_engine('postgresql://postgres:@localhost:5432/{0}'.format(params['mbb']['db_name']))

    engine.execute('CREATE SCHEMA IF NOT EXISTS {0};'.format(params['ncaa_data']['schema_name']))
    get_ncaa_data(engine=engine)
