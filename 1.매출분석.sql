/* 일별 매출액 */
select date_trunc('day', order_date):: date as day
	, sum(amount) as total_amount
	, count(distinct a.order_id) as daily_ord_cnt
from nw.orders as a
	left join nw.order_items as b on a.order_id = b.order_id 
group by date_trunc('day', order_date):: date
order by day
;

/* 일별, 상품별 매출액 */
select date_trunc('day', order_date):: date as day
	, b.product_id 
	, sum(amount) as total_amount
	, count(distinct a.order_id) as daily_ord_cnt
from nw.orders as a
	left join nw.order_items as b on a.order_id = b.order_id 
group by date_trunc('day', order_date):: date
	, b.product_id 
order by day, b.product_id 
;
