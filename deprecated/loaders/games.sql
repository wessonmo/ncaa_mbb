begin;

drop table if exists g;
create temporary table g (
	school_id		float,
	season			float,
	opp_id			float,
	game_date		date,
	school_score	float,
	opp_score		float,
	location		text,
	site			text,
	ot				float,
	attend			float
);

truncate table g;

\set csv_path '\'' :folder_path '\\games.csv\''
copy g from :csv_path with header delimiter as ',' csv quote as '"';

delete from g where opp_id is null;
-- delete from g where game_date = '2009-01-16' and school_id = 403 and opp_id = 8580;
-- delete from g where game_date = '2012-02-14' and school_id = 30067 and opp_id = 30083;
-- delete from g where game_date = '2012-01-11' and school_id = 658 and opp_id = 613;
-- delete from g where game_date = '2012-01-12' and school_id = 1079 and opp_id = 2746;

drop table if exists mbb.games;
create table mbb.games as (
	select distinct
        g.season,
        game_date,
		game_date - min_date as season_day,
        case
            when location = 'Neutral' or school_id < opp_id then location
            when location = 'Away' then 'Home'
            else 'Away'
        end as location,
        coalesce(ot,0) as ot,
        least(school_id,opp_id) as low_id,
        greatest(school_id,opp_id) as high_id,
        case
            when school_id < opp_id then school_score
            else opp_score
        end as low_score,
        case
            when school_id < opp_id then opp_score
            else school_score
        end as high_score
    from
        g
		left join (
			select distinct
				season,
				min(game_date) as min_date
			from g
			group by season
			) as dt
			on g.season = dt.season
);

delete from mbb.games where (game_date, low_id, high_id)::text in (
	select (game_date, low_id, high_id)::text
	from mbb.games
	group by game_date, low_id, high_id
	having count(*) > 1
	)
	and ot = 0;

alter table mbb.games add primary key (game_date, low_id, high_id);

commit;