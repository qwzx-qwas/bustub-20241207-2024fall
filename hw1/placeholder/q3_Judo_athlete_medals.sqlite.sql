select a.name as winner_name, sum(judo_athletes.medal_number) as medal_count
from(
    select a.code,a.name, count(m.medal_code) as medal_number
    from athletes a
    join medals m on a.code = m.winner_code
    where a.discipline like '%Judo%'
    group by a.code, a.name

    union all

    select a.code,a.name, count(m.medal_code) as medal_number
    from teams t
    join athletes a on t.athletes_code = a.code
    join medals m on t.code = m.winner_code
    where a.discipline like '%Judo%'
    group by a.code, a.name

) as judo_athletes
where medal_number > 0
group by winner_name
order by medal_count desc, winner_name asc;
