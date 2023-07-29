/*이전 주문이후 현주문까지 걸린 기간 및 걸린 기간의 히스토그램 구하기*/

-- 주문 테이블에서 이전 주문 이후 걸린 기간 구하기.

with
temp_01 as (
select order_id, customer_id, order_date
	, lag(order_date) over (partition by customer_id order by order_date) as prev_ord_date
from nw.orders
), 
temp_02 as (
select order_id, customer_id, order_date
	, order_date - prev_ord_date as days_since_prev_order
from temp_01 
where prev_ord_date is not null
)
select * from temp_02;

-- 이전 주문이후 걸린 기간의 히스토그램 구하기
with
temp_01 as (
select order_id, customer_id, order_date
	, lag(order_date) over (partition by customer_id order by order_date) as prev_ord_date
from nw.orders
), 
temp_02 as (
select order_id, customer_id, order_date
	, order_date - prev_ord_date as days_since_prev_order
from temp_01 
where prev_ord_date is not null
)
-- bin의 간격을 10으로 설정.
select floor(days_since_prev_order/10.0)*10 as bin, count(*) bin_cnt
from temp_02 
group by floor(days_since_prev_order/10.0)*10 order by 1 
;


/*월별 사용자 평균 주문 건수 */
with
temp_01 as (
select customer_id, date_trunc('month', order_date)::date as month_day, count(*) as order_cnt 
from nw.orders
group by customer_id, date_trunc('month', order_date)::date
)
select month_day, avg(order_cnt), max(order_cnt), min(order_cnt)
from temp_01
group by month_day
order by month_day;
