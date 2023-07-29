/*고객별 RFM 구하기*/

-- recency, frequency, monetary 각각에 5 ntile을 적용하여 고객별 RFM 구하기
with 
temp_01 as ( 
select a.user_id, max(date_trunc('day', order_time))::date as max_ord_date
, to_date('20161101', 'yyyymmdd') - max(date_trunc('day', order_time))::date  as recency
, count(distinct a.order_id) as freq
, sum(prod_revenue) as money
from ga.orders a
	join ga.order_items b on a.order_id = b.order_id
group by a.user_id
)
select *
	-- recency, frequency, money 각각을 5개 등급으로 나눔. 1등급이 가장 높고, 5등급이 가장 낮음.
	, ntile(5) over (order by recency asc rows between unbounded preceding and unbounded following) as recency_rank
	, ntile(5) over (order by freq desc rows between unbounded preceding and unbounded following) as freq_rank
	, ntile(5) over (order by money desc rows between unbounded preceding and unbounded following) as money_rank
from temp_01;
/*단순 ntile로 할경우 정규 분포가 아닌 형태에서는 정확한 분석이 힘들 수 있음 때문에 범위를 수동으로 설정하고 할당해야할 수 있음 */


--  recency, frequency, monetary 각각에 대해서 범위를 설정하고 이 범위에 따라 RFM 등급 할당.
with
temp_01 as (
	select 'A' as grade, 1 as fr_rec, 14 as to_rec, 5 as fr_freq, 9999 as to_freq, 100.0 as fr_money, 999999.0 as to_money
	union all
	select 'B', 15, 50, 3, 4, 50.0, 99.999
	union all
	select 'C', 51, 99999, 1, 2, 0.0, 49.999
)
select * from temp_01;

-- 전체 쿼리
with 
temp_01 as ( 
select a.user_id, max(date_trunc('day', order_time))::date as max_ord_date
, to_date('20161101', 'yyyymmdd') - max(date_trunc('day', order_time))::date  as recency
, count(distinct a.order_id) as freq
, sum(prod_revenue) as money
from ga.orders a
	join ga.order_items b on a.order_id = b.order_id
group by a.user_id
), 
temp_02 as (
	select 'A' as grade, 1 as fr_rec, 14 as to_rec, 5 as fr_freq, 9999 as to_freq, 300.0 as fr_money, 999999.0 as to_money
	union all
	select 'B', 15, 50, 3, 4, 50.0, 299.999
	union all
	select 'C', 51, 99999, 1, 2, 0.0, 49.999
) 
--select * from temp_02; 등급 구간 확인
,temp_03 as (
select a.*
	, b.grade as recency_grade, c.grade as freq_grade, d.grade as money_grade
from temp_01 a
	left join temp_02 b on a.recency between b.fr_rec and b.to_rec
	left join temp_02 c on a.freq between c.fr_freq and c.to_freq
	left join temp_02 d on a.money between d.fr_money and d.to_money
)
select * -- 비즈니스에 맞춰 룰을 정하고 등급을 산정
	, case when recency_grade = 'A' and freq_grade in ('A', 'B') and money_grade = 'A' then 'A'
	       when recency_grade = 'B' and freq_grade = 'A' and money_grade = 'A' then 'A'
	       when recency_grade = 'B' and freq_grade in ('A', 'B', 'C') and money_grade = 'B' then 'B'
	       when recency_grade = 'C' and freq_grade in ('A', 'B') and money_grade = 'B' then 'B'
	       when recency_grade = 'C' and freq_grade = 'C' and money_grade = 'A' then 'B'
	       when recency_grade = 'C' and freq_grade = 'C' and money_grade in ('B', 'C') then 'C'
	       when recency_grade in ('B', 'C') and money_grade = 'C' then 'C'
	       else 'C' end as total_grade
from temp_03
;



/*
select * from temp_03;
select freq_grade, count(*)
from temp_03 group by freq_grade
*/

