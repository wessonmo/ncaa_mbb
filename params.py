params = {
    'mbb': {
       'min_season': 2013,
       'max_season': 2018,
       'max_division': 1,
       'storage_dir': 'E:\\ncaa_mbb'
       },
    'ncaa_data': {
        'data_types_order': ['team_index','team_info','conference','roster','summary','box_score','pbp'],
        'data_types': {
            'team_index': {
                    'href_frame': '/team/inst_team_list?academic_year={0}&conf_id=-1&division={1}&sport_code=MBB',
                    'url_ids': ['season', 'division'],
                    'scrape_data_type': None,
                    'parse_data_types': None
                    },
            'team_info': {
                    'href_frame': '/team/{1}/{0}',
                    'url_ids': ['season_id', 'school_id'],
                    'scrape_data_type': 'team_index',
                    'parse_data_types': ['coach', 'facility', 'schedule']
                    },
            'conference': {
                    'href_frame': '/teams/history/MBB/{1}',
                    'url_ids': ['season', 'school_id'],
                    'scrape_data_type': 'team_index',
                    'parse_data_types': ['conference']
                    },
            'roster': {
                    'href_frame': '/team/{1}/roster/{0}',
                    'url_ids': ['season_id', 'school_id'],
                    'scrape_data_type': 'team_index',
                    'parse_data_types': None
                    },
            'summary': {
                    'href_frame': '/game/period_stats/{0}',
                    'url_ids': ['game_id'],
                    'scrape_data_type': 'schedule',
                    'parse_data_types': None
                    },
            'box_score': {
                    'href_frame': '/game/box_score/{0}?period_no={1}',
                    'url_ids': ['game_id', 'period'],
                    'scrape_data_type': 'schedule',
                    'parse_data_types': None
                    },
            'pbp': {
                    'href_frame': '/game/play_by_play/{0}',
                    'url_ids': ['game_id'],
                    'scrape_data_type': 'schedule',
                    'parse_data_types': ['game_time', 'game_location', 'officials', 'pbp']
                    }
            }
        }
    }
