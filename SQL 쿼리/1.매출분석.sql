/* North Wind 상거래 데이터 세트 */
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

/* 월별 상품 카테고리별 매출액 및 주문 건수, 월 전체 매출액 대비 비율 */
with 
base as (
select d.category_name
	, to_char(date_trunc('month', order_date), 'yyyymm') as month_day
	, sum(amount) as sum_amount
	, count(distinct a.order_id) as monthly_ord_cnt
from nw.orders a
	join nw.order_items b on a.order_id = b.order_id
	join nw.products c on b.product_id = c.product_id 
    join nw.categories d on c.category_id = d.category_id
group by d.category_name
	, to_char(date_trunc('month', order_date), 'yyyymm')
)
select *
	, sum(sum_amount) over (partition by month_day) as month_tot_amount
	, round(sum_amount / sum(sum_amount) over (partition by month_day), 3) * 100 as month_ratio
from base;

/* 상품별 전체 매출액 및 해당 상품 테고리 전체 매출액 대비 비율, 해당 상품카테고리에서 매출 순위 */
with
temp_01 as (  -- max 를 활용해서 group by 절에 없는 상품들의 이름들도 가져올 수 있다.
	select a.product_id, max(product_name) as product_name, max(category_name) as category_name
		, sum(amount) as sum_amount
	from nw.order_items a
		join nw.products b
			on a.product_id = b.product_id
		join nw.categories c 
			on b.category_id = c.category_id
	group by a.product_id
)
select product_name, sum_amount as product_sales
	, category_name
	, sum(sum_amount) over (partition by category_name) as category_sales
	, sum_amount / sum(sum_amount) over (partition by category_name) as product_category_ratio
	, row_number() over (partition by category_name order by sum_amount desc) as product_rn
from temp_01
order by category_name, product_sales desc;


/* 동년도 월별 누적 매출 및 동일 분기 월별 누적 매출 */
with 
temp_01 as (
select date_trunc('month', order_date)::date as month_day
	, sum(amount) as sum_amount
from nw.orders a
	join nw.order_items b on a.order_id = b.order_id
group by date_trunc('month', order_date)::date
)
select month_day, sum_amount
	, sum(sum_amount) over (partition by date_trunc('year', month_day) order by month_day) as cume_year_amount
	, sum(sum_amount) over (partition by date_trunc('quarter', month_day) order by month_day) as cume_quarter_amount
from temp_01;




