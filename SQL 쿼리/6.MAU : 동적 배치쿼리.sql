/*일별 세션건수, 일별 방문 사용자(유저), 사용자별 평균 세션 수 */
with temp_01 as 
(
	select to_char(date_trunc('day', visit_stime), 'yyyy-mm-dd') as d_day
		-- ga_ses 테이블에는 sess_id로 unique하므로 count(sess_id)와 동일
		, count(distinct sess_id) as daily_sess_cnt
		, count(sess_id) as daily_sess_cnt_again
		, count(distinct user_id) as daily_user_cnt 
	from ga.ga_sess
	group by to_char(date_trunc('day', visit_stime), 'yyyy-mm-dd')
)
select * 
	, 1.0*daily_sess_cnt/daily_user_cnt as avg_user_sessions
	-- 아래와 같이 정수와 정수를 나눌 때 postgresql은 정수 형변환 함. 1.0을 곱해주거나 명시적으로 float type 선언
	--, daily_sess_cnt/daily_user_cnt
from temp_01;


/DAU, WAU, MAU 구하기 */

-- 일별 방문한 고객 수 (DAU)
select date_trunc('day', visit_stime)::date as d_day, count(distinct user_id) as user_cnt 
from ga.ga_sess 
--where visit_stime between to_date('2016-10-25', 'yyyy-mm-dd') and to_timestamp('2016-10-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss')
group by date_trunc('day', visit_stime)::date;

-- 주별 방문한 고객 수(WAU)
select date_trunc('week', visit_stime)::date as week_day, count(distinct user_id) as user_cnt
from ga.ga_sess
--where visit_stime between to_date('2016-10-24', 'yyyy-mm-dd') and to_timestamp('2016-10-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss')
group by date_trunc('week', visit_stime)::date order by 1;

-- 월별 방문한 고객 수(MAU)
select date_trunc('month', visit_stime)::date as month_day, count(distinct user_id) as user_cnt 
from ga.ga_sess 
--where visit_stime between to_date('2016-10-2', 'yyyy-mm-dd') and to_timestamp('2016-10-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss')
group by date_trunc('month', visit_stime)::date;


/* 아래는 하루 주기로 계속 DAU, MAU(이전 7일), MAU(이전 30일)을 계속 추출. */

-- interval로 전일 7일 구하기
select to_date('20161101', 'yyyymmdd') - interval '7 days';

-- 오라클의 sysdae 사용과 같음 batch용 동적으로 쿼리 작성
select :current_date, count(distinct user_id) as wau
from ga.ga_sess
where visit_stime >= (:current_date - interval '7 days') and visit_stime < :current_date;

-- 현재 일을 기준으로 전일의 DAU구하기
select :current_date, count(distinct user_id) as dau
from ga.ga_sess
where visit_stime >= (:current_date - interval '1 days') and visit_stime < :current_date;


--daily_acquisitions 현재일 기준으로 새롭게 계산되는 배치 쿼리
insert into daily_acquisitions
select 
	:current_date, 
	-- scalar subquery
	(select count(distinct user_id) as dau
	from ga_sess
	where visit_stime >= (:current_date - interval '1 days') and visit_stime < :current_date
	),
	(select count(distinct user_id) as wau
	from ga_sess
	where visit_stime >= (:current_date - interval '7 days') and visit_stime < :current_date
	),
	(select count(distinct user_id) as mau
	from ga_sess
	where visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date
	)
;
--데이터 입력 확인
select * from daily_acquisitions;



/과거 일자별로 DAU 생성*/
with 
temp_00 as (
select generate_series('2016-08-02'::date , '2016-11-01'::date, '1 day'::interval)::date as current_date
)
select b.current_date, count(distinct user_id) as dau
from ga_sess a
	cross join temp_00 b
where visit_stime >= (b.current_date - interval '1 days') and visit_stime < b.current_date
group by b.current_date;

-- 과거 일자별로 DAU 생성
with 
temp_00 as (
select generate_series('2016-08-02'::date , '2016-11-01'::date, '1 day'::interval)::date as current_date
)
select b.current_date, count(distinct user_id) as wau
from ga_sess a
	cross join temp_00 b
where visit_stime >= (b.current_date - interval '7 days') and visit_stime < b.current_date
group by b.current_date;

-- 과거 일자별로 지난 7일 WAU 생성.
with 
temp_00 as (
select generate_series('2016-08-02'::date , '2016-11-01'::date, '1 day'::interval)::date as current_date
)
select b.current_date, count(distinct user_id) as mau
from ga_sess a
	cross join temp_00 b
where visit_stime >= (b.current_date - interval '30 days') and visit_stime < b.current_date
group by b.current_date;


-- 과거 일자별로 지난 30일의 MAU 생성
with 
temp_00 as (
select generate_series('2016-08-02'::date , '2016-11-01'::date, '1 day'::interval)::date as current_date
)
select b.current_date, count(distinct user_id) as mau
from ga_sess a
	cross join temp_00 b
where visit_stime >= (b.current_date - interval '30 days') and visit_stime < b.current_date
group by b.current_date;


--데이터 확인 81587, 80693, 80082
select count(distinct user_id) as mau
	from ga_sess
	where visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date;


--과거 일자별로 DAU 생성하는 임시 테이블 생성
drop table if exists daily_dau;

create table daily_dau
as
with 
temp_00 as (
select generate_series('2016-08-02'::date , '2016-11-01'::date, '1 day'::interval)::date as current_date
)
select b.current_date, count(distinct user_id) as dau
from ga_sess a
	cross join temp_00 b
where visit_stime >= (b.current_date - interval '1 days') and visit_stime < b.current_date
group by b.current_date
;

-- 과거 일자별로 MAU 생성하는 임시 테이블 생성
drop table if exists daily_wau;

create table daily_wau
as
with 
temp_00 as (
select generate_series('2016-08-02'::date , '2016-11-01'::date, '1 day'::interval)::date as current_date
)
select b.current_date, count(distinct user_id) as wau
from ga_sess a
	cross join temp_00 b
where visit_stime >= (b.current_date - interval '7 days') and visit_stime < b.current_date
group by b.current_date;


-- 과거 일자별로 MAU 생성하는 임시 테이블 생성
drop table if exists daily_mau;

create table daily_mau
as
with 
temp_00 as (
select generate_series('2016-08-02'::date , '2016-11-01'::date, '1 day'::interval)::date as current_date
)
select b.current_date, count(distinct user_id) as mau
from ga_sess a
	cross join temp_00 b
where visit_stime >= (b.current_date - interval '30 days') and visit_stime < b.current_date
group by b.current_date;


-- DAU, WAU, MAU 임시테이블을 일자별로 조인하여 daily_acquisitions 테이블 생성.
drop table if exists daily_acquisitions;

create table daily_acquisitions
as
select a.current_date, a.dau, b.wau, c.mau
from daily_dau a
	join daily_wau b on a.current_date = b.current_date
	join daily_mau c on a.current_date = c.current_date
;

select * from daily_acquisitions;



