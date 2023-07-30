/*MAU를 신규 사용자, 기존 사용자(재방문) 건수로 분리하여 추출(세션 건수도 함께 추출)*/
with
temp_01 as (
select a.sess_id, a.user_id, a.visit_stime, b.create_time
	, case when b.create_time >= (:current_date - interval '30 days') and b.create_time < :current_date then 1
	     else 0 end as is_new_user
from ga.ga_sess a
	join ga.ga_users b on a.user_id = b.user_id
where visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date
)
select count(distinct user_id) as user_cnt
	, count(distinct case when is_new_user = 1 then user_id end) as new_user_cnt
	, count(distinct case when is_new_user = 0 then user_id end) as repeat_user_cnt
	, count(*) as sess_cnt
from temp_01;


/* 채널별로 MAU를 신규 사용자, 기존 사용자로 나누고, 채널별 비율까지 함께 계산 */
select channel_grouping, count(distinct user_id) from ga.ga_sess group by channel_grouping;

with
temp_01 as (
select a.sess_id, a.user_id, a.visit_stime, b.create_time, channel_grouping
	, case when b.create_time >= (:current_date - interval '30 days') and b.create_time < :current_date then 1
	     else 0 end as is_new_user
from ga.ga_sess a
	join ga.ga_users b on a.user_id = b.user_id
where visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date
),
temp_02 as (
select channel_grouping
	, count(distinct case when is_new_user = 1 then user_id end) as new_user_cnt
	, count(distinct case when is_new_user = 0 then user_id end) as repeat_user_cnt
	, count(distinct user_id) as channel_user_cnt
	, count(*) as sess_cnt
from temp_01
group by channel_grouping
)
select channel_grouping, new_user_cnt, repeat_user_cnt, channel_user_cnt, sess_cnt
	, 100.0*new_user_cnt/sum(new_user_cnt) over () as new_user_cnt_by_channel
	, 100.0*repeat_user_cnt/sum(repeat_user_cnt) over () as repeat_user_cnt_by_channel
from temp_02;


/*채널별 고유 사용자 건수와 매출금액 및 비율, 주문 사용자 건수와 주문 매출 금액 및 비율
 * 채널별로 고유 사용자 건수와 매출 금액을 구하고 고유 사용자 건수 대비 매출 금액 비율을 추출
 * 또한 고유 사용자 중에서 주문을 수행한 사용자 건수를 추출 후 주문 사용자 건수 대비 매출 금액 비율을 추출*/

with temp_01 as (
	select a.sess_id, a.user_id, a.channel_grouping
		, b.order_id, b.order_time, c.product_id, c.prod_revenue 
	from ga_sess a
		left join orders b on a.sess_id = b.sess_id
		left join order_items c on b.order_id = c.order_id
	where a.visit_stime >= (:current_date - interval '30 days') and a.visit_stime < :current_date
)
select channel_grouping
	, sum(prod_revenue) as ch_amt -- 채널별 매출
	--, count(distinct sess_id) as ch_sess_cnt -- 채널별 고유 세션 수
	, count(distinct user_id) as ch_user_cnt -- 채널별 고유 사용자 수
	--, count(distinct case when order_id is not null then sess_id end) as ch_ord_sess_cnt -- 채널별 주문 고유 세션수
	, count(distinct case when order_id is not null then user_id end) as ch_ord_user_cnt -- 채널별 주문 고유 사용자수
	--, sum(prod_revenue)/count(distinct sess_id) as ch_amt_per_sess -- 접속 세션별 주문 매출 금액
	, sum(prod_revenue)/count(distinct user_id) as ch_amt_per_user -- 접속 고유 사용자별 주문 매출 금액
	-- 주문 세션별 매출 금액
	--, sum(prod_revenue)/count(distinct case when order_id is not null then sess_id end) as ch_ord_amt_per_sess
	-- 주문 고유 사용자별 매출 금액
	, sum(prod_revenue)/count(distinct case when order_id is not null then user_id end) as ch_ord_amt_per_user
from temp_01
group by channel_grouping order by ch_user_cnt desc;





