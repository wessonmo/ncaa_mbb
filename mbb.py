from ncaa_data.base_class import  base_data_type
from params import params

def get_ncaa_data():
    for name in params['ncaa_data']['data_types_order']:
        pars = params['ncaa_data']['data_types'][name]

        print(name)

        data_type = base_data_type(
            data_type = name,
            href_frame = pars['href_frame'],
            url_ids = pars['url_ids'],
            parse_data_types = pars['parse_data_types'],
            scrape_data_type = pars['scrape_data_type']
            )
        data_type.scrape_to_file()
        data_type.parse_to_csv()
        data_type.dedupe_csv()

if __name__ == '__main__':
    get_ncaa_data()
