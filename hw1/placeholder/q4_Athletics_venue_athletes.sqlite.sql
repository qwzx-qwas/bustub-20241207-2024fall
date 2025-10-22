-- 找到所有参加举办过田径场馆的运动员和队伍
with a as (
    select *
    from atheletes
    where code in
        (select distinct participant_code
        from 
            (
                select participant_code
                from results left join venues on results.venue = venues.venue   --筛选为场地相同的且都为田径场地的，个人参赛的
                where venues.disciplines like '%Athletics%' and participant_type = 'Person'

                union
                select athletes_code as participant_code 
                from teams
                where code in (
                    select participant_code
                    from results left join venues on results.venue = venues.venue   --筛选为场地相同的且都为田径场地的，团队参赛的
                    where venues.disciplines like '%Athletics%' and participant_type = 'Team')
            )
        )

)

select 
    name as athlete_name, 
    country_code as represented_country_code,
    nationality_code as nationality_country_code
from
    (   
        select t.*, countries.Latitude as country_latitude, countries.Longitude as country_longitude
        from
        (select a.*, countries.Latitude as nationality_latitude, countries.Longitude as nationality_longitude
        from a left join countries on a.nationality_code = countries.code) as t   --内层连接，获取运动员国籍的经纬度
        left join countries on t.country_code = countries.code    --外层连接，获取运动员代表国家的经纬度
        where 
        country_latitude is not null 
        and country_longitude is not null 
        and nationality_latitude is not null 
        and nationality_longitude is not null    --筛选出经纬度不为空 
    )
order by 
    (country_latitude - nationality_latitude)^2 + (country_longitude -  nationality_longitude)^2 desc, athlete_name;
    
