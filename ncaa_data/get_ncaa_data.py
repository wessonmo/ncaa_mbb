from params import params
from ncaa_data.base_class import  base_data_type

ncaa_data_params = params['ncaa_data']

def get_ncaa_data():
    for data_type in ncaa_data_params['data_types_order']:
        values = ncaa_data_params['data_types'][data_type]

        print(data_type)

        data_type = base_data_type(
            data_type = data_type,
            href_frame = values['href_frame'],
            url_ids = values['url_ids'],
            parse_data_types = values['parse_data_types'],
            scrape_data_type = values['scrape_data_type']
            )
        data_type.scrape_to_file()
        data_type.parse_to_csv()
        data_type.dedupe_csv()
