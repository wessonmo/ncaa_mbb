begin;

drop table if exists mbb.school_info;

create table mbb.school_info (
	school_id	int,
	season		int,
	city		text,
	arena		text,
	capacity	int
	primary key (school_id, season)
);

truncate table mbb.school_info;

copy mbb.school_info from '/csv/school_info.csv' with delimiter as ',' csv quote as '"';

commit;