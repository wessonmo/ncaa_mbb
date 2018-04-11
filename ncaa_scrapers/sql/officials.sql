begin;

drop table if exists ncaa.officials;

create table ncaa.officials (
	game_id		int,
	official	text,
	add primary key (game_id, official)
);

truncate table ncaa.officials;

\set csv_path '\'' :folder_path '\\officials_' :yr '.csv\''
copy ncaa.officials from :csv_path with header delimiter as ',' csv quote as '"';

commit;