/* 
获取奖牌表中的获奖者，再从获奖者找到运动员表或队伍表中的所属国家
以及对应的项目，将二者与教练的对比，如果相同就+1，最后只显示大于等于1的教练
*/


select c.name as coach_name, count(*) as medal_number 
from coaches c
join (   
    select m.winner_code, a.country_code, a.disciplines
    from medals m
    join athletes a on m.winner_code = a.code
    union
    select m.winner_code, t.country_code, t.disciplines
    from medals m
    join teams t on m.winner_code = t.code
) as medal_country_discipline_table  /*生成一个临时的结果集，as后跟别名*/
on c.country_code = medal_country_discipline_table.country_code     /*将两个表匹配，并设置连接条件*/
   and c.disciplines = medal_country_discipline_table.disciplines
group by c.code, c.name             /*分组合并*/
having count(*) >= 1
order by medal_number desc, coach_name; /*desc降序排列*/

 