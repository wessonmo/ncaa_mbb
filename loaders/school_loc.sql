begin;

drop table if exists geo.school_loc;

create table geo.school_loc (
	season		int,
	school_id	int,
	arena_comb	text,
	city		text,
	latitude	float,
	longitude	float
	primary key (season, school_id)
);

truncate table geo.school_loc;

copy geo.school_loc from '/csv/school_loc.csv' with delimiter as ',' csv quote as '"';

commit;