begin;

drop table if exists geo.school_dist;

create table geo.school_dist (
	game_date	date,
	school_id	text,
	opp_id		text,
	school_dist	float,
	opp_dist	float
	primary key (game_date, school_id, opp_id)
);

truncate table geo.school_dist;

copy geo.school_dist from '/csv/school_dist.csv' with delimiter as ',' csv quote as '"';

commit;