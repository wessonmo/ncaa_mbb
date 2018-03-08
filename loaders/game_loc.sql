begin;

drop table if exists mbb.game_loc;

create table mbb.game_loc (
	site		text,
	site_sec	text,
	latitude	float,
	longitude	float,
	primary key (site)
);

truncate table mbb.game_loc;

\set csv_path '\'' :folder_path '\\game_loc.csv\''
copy mbb.game_loc from :csv_path with header delimiter as ',' csv quote as '"';

commit;