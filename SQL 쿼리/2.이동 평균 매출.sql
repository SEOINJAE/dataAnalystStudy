/* 5일 이동 평균 매출 구하기 */
with 
temp_01 as (
select date_trunc('day', order_date)::date as d_day
	, sum(amount) as sum_amount
from nw.orders a
	join nw.order_items b on a.order_id = b.order_id
where order_date >= to_date('1996-07-08', 'yyyy-mm-dd')
group by date_trunc('day', order_date)::date
)
select d_day, sum_amount
	, avg(sum_amount) over (order by d_day rows between 4 preceding and current row) as m_avg_5day
from temp_01;



/* 5일 이동 평균을 구하되 맨 앞 시작점에서 5일을 채울 수 없는 경우는 Null 표시 */
with 
temp_01 as (
select date_trunc('day', order_date)::date as d_day
	, sum(amount) as sum_amount
from nw.orders a
	join nw.order_items b on a.order_id = b.order_id
where order_date >= to_date('1996-07-08', 'yyyy-mm-dd')
group by date_trunc('day', order_date)::date
),
temp_02 as (
select d_day, sum_amount
	, avg(sum_amount) over (order by d_day rows between 4 preceding and current row) as m_avg_5days
	, row_number() over (order by d_day) as rnum
from temp_01
)
select d_day, sum_amount, rnum
	, case when rnum < 5 then Null
	       else m_avg_5days end as m_avg_5days
from temp_02;


/* 5일 이동 가중평균 매출액 구하기, 당일 날짜에서 가까운 날짜일 수록 가중치를 증대
 * 5일 중 가장 먼 날짜는 매출액의 0.5, 중간 날짜 2, 3, 4는 매출액 그대로, 당일은 1.5 * 매출액으로 가중치 부여 */
with 
temp_01 as (
select date_trunc('day', order_date)::date as d_day
	, sum(amount) as sum_amount
	, row_number() over (order by date_trunc('day', order_date)::date) as rnum
from nw.orders a
	join nw.order_items b on a.order_id = b.order_id
where order_date >= to_date('1996-07-08', 'yyyy-mm-dd')
group by date_trunc('day', order_date)::date
),
temp_02 as (
select a.d_day, a.sum_amount, a.rnum, b.d_day as d_day_back, b.sum_amount as sum_amount_back, b.rnum as rnum_back 
from temp_01 a
	join temp_01 b on a.rnum between b.rnum and b.rnum + 4  -- 범위 제한을 줘서 조인함 카티산 제곱은 막고 필요한 부분만 뻥튀기함
	--join temp_01 b on b.rnum between a.rnum - 4 and a.rnum;
)
select d_day
	, avg(sum_amount_back) as m_avg_5days
	-- sum을 건수인 5로 나누어 평균이 됨.
	, sum(sum_amount_back)/5 as m_avg_5days_01
	-- 가중 이동 평균을 구하기 위해 가중치 값에 따라 sum을 구함. 
	, sum(case when rnum - rnum_back = 4 then 0.5 * sum_amount_back
	           when rnum - rnum_back in (3, 2, 1) then sum_amount_back
	           when rnum - rnum_back = 0 then 1.5 * sum_amount_back 
	      end) as m_weighted_sum
	-- 위에서 구한 가중치 값에 따른 sum을 5로 나눠서 가중 이동 평균을 구함.
	, sum(case when rnum - rnum_back = 4 then 0.5 * sum_amount_back
		   when rnum - rnum_back in (3, 2, 1) then sum_amount_back
		   when rnum - rnum_back = 0 then 1.5 * sum_amount_back 
	      end) / 5 as m_w_avg_sum
	, count(*) as cnt  -- 5건이 안되는 초기 데이터는 삭제하기 위해서
from temp_02
group by d_day
having count(*) = 5
order by d_day
;