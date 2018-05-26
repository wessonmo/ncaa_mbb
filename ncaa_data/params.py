params = {
        'team_index':{
                'href_frame': '/team/inst_team_list?academic_year={0}&conf_id=-1&division={1}&sport_code=MBB',
                'url_ids': ['season', 'division'],
                'scrape_table': None,
                'parse_file_types': None
                },
        'team_info':{
                'href_frame': '/team/{1}/{0}',
                'url_ids': ['season_id', 'school_id'],
                'scrape_table': 'team_index',
                'parse_file_types': ['coach', 'facility', 'schedule']
                },
        'roster':{
                'href_frame': '/team/{1}/roster/{0}',
                'url_ids': ['season_id', 'school_id'],
                'scrape_table': 'team_index',
                'parse_file_types': None
                },
        'summary':{
                'href_frame': '/game/period_stats/{0}',
                'url_ids': ['game_id'],
                'scrape_table': 'schedule',
                'parse_file_types': None
                },
        'box_score':{
                'href_frame': '/game/box_score/{0}?period_no={1}',
                'url_ids': ['game_id', 'period'],
                'scrape_table': 'schedule',
                'parse_file_types': None
                },
        'pbp':{
                'href_frame': '/game/play_by_play/{0}',
                'url_ids': ['game_id'],
                'scrape_table': 'schedule',
                'parse_file_types': ['game_time', 'game_location', 'officials', 'pbp']
                },
        }
