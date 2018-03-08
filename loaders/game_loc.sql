begin;

drop table if exists geo.game_loc;

create table geo.game_loc (
	site		text,
	site_sec	text,
	latitude	float,
	longitude	float
	primary key (site)
);

truncate table geo.game_loc;

copy geo.game_loc from '/csv/game_loc.csv' with delimiter as ',' csv quote as '"';

commit;