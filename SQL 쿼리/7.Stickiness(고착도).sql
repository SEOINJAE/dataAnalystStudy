--DAU와 MAU의 비율. 고착도(stickiness) 월간 사용자들중 얼마나 많은 사용자가 주기적으로 방문하는가? 재방문 지표로 서비스의 활성화 지표 제공

--DAU와 MAU의 비율
with 
temp_dau as (
select :current_date as curr_date, count(distinct user_id) as dau
from ga.ga_sess
where visit_stime >= (:current_date - interval '1 days') and visit_stime < :current_date
), 
temp_mau as (
select :current_date as curr_date, count(distinct user_id) as mau
from ga.ga_sess
where visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date
)
select a.current_day, a.dau, b.mau, round(100.0 * a.dau/b.mau, 2) as stickieness
from temp_dau a
	join temp_mau b on a.curr_date = b.curr_date
;

- 일주일간 stickiess, 평균 stickness
select *, round(100.0 * dau/mau, 2) as stickieness
	, round(avg(100.0 * dau/mau) over(), 2) as avg_stickieness
from ga.daily_acquisitions
where curr_date between to_date('2016-10-25', 'yyyy-mm-dd') and to_date('2016-10-31', 'yyyy-mm-dd')
