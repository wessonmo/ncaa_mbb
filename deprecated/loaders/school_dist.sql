begin;

drop table if exists dist;
create temporary table dist (
	game_date	date,
	school_id	float,
	opp_id		float,
	school_dist	float,
	opp_dist	float,
	primary key (game_date, school_id, opp_id)
);

truncate table dist;

\set csv_path '\'' :folder_path '\\school_dist.csv\''
copy dist from :csv_path with header delimiter as ',' csv quote as '"';

drop table if exists mbb.school_dist;
create table mbb.school_dist as (
	select distinct
        game_date,
        least(school_id,opp_id) as low_id,
        greatest(school_id,opp_id) as high_id,
        case
            when school_id < opp_id then round(school_dist)
            else round(opp_dist)
        end as low_dist,
        case
            when school_id < opp_id then round(opp_dist)
            else round(school_dist)
        end as high_dist
    from
        dist
);

delete
	from mbb.school_dist
	where (game_date, low_id, high_id, low_dist + high_dist)::text in (
		select (game_date, low_id, high_id, min(low_dist + high_dist))::text
		from mbb.school_dist
		where (game_date, low_id, high_id)::text in (
			select (game_date, low_id, high_id)::text
			from mbb.school_dist
			group by game_date, low_id, high_id
			having count(*) > 1
			)
		group by game_date, low_id, high_id
	)
;

alter table mbb.school_dist add primary key (game_date, low_id, high_id);

commit;