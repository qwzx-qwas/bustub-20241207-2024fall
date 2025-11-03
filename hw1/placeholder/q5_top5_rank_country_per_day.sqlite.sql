--构建一个临时表来找到每天前五名中出现次数最多的国家
--（我觉得可以最后按字母顺序排一下序，再选最前的国家，来处理平手的情况）
with 
    top5_raw as (
        select *
        from results
        where rank <= 5
    )
--top5_appperance表：筛选出每天排名前五的记录
    top5_appperance as (
        select r.date, a.country_code
        from top5_raw r
        join athletes a on r.participant_code = a.code

        union all
        select r.date, a.country_code
        from top5_raw r
        join teams t on r.participant_code = t.code
        join athletes a on t.athletes_code = a.code

    )

--计算每天每个国家在前五名中出现的次数
    country_count as (
        select 
            date,
            country_code,
            count(*) as top5_appearances
        from top5_appperance
        group by date, country_code
    )

--计算GDP排名和人口排名

    country_rank as (
        select 
            code,
            rank() over (order by "GDP ($ per capita)" desc) as gdp_rank,
            rank() over (order by "Population" desc) as population_rank
        from countries
    )

    --进行排序
    ranked_countries as (
        select
            date,
            country_code,
            top5_appearances,
            row_number() over (partition by date order by top5_appearances desc, country_code asc) as rn
        from country_count
    )

select
    rc.date,
    rc.country_code,
    rc.top5_appearances,
    cr.gdp_rank,
    cr.population_rank
from ranked_countries rc
left join country_rank cr on rc.country_code = cr.code
where rc.rn = 1
order by rc.date ASC;






