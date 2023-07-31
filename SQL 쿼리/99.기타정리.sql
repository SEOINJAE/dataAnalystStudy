-- 날짜 동적 배치 쿼리 오라클 기준 / 주의 시작, 월의 시작, 년의 시작
WITH BASE_DATE AS
(
    SELECT
        TO_DATE(SYSDATE,'YYYYMMDD') AS BASE_DATE
    FROM DUAL
)
,
BASE_INPUT AS
(
    SELECT
        (SELECT BASE_DATE FROM BASE_DATE) AS BASE_DATE,
        (SELECT TO_CHAR(BASE_DATE, 'YYYY') FROM BASE_DATE) AS BASE_YEAR,
        CASE
            WHEN(SELECT BASE_DATE-1 FROM BASE_DATE) < (SELECT LAST_DAY(BASE_DATE) FROM BASE_DATE) THEN (SELECT TRUNC(BASE_DATE-1, 'MONTH') FROM BASE_DATE)
            ELSE (SELECT TRUNC(BASE_DATE, 'MONTH') FROM BASE_DATE)
        END AS MONTH_START,
        CASE
            WHEN(SELECT BASE_DATE-1 FROM BASE_DATE) < (SELECT LAST_DAY(BASE_DATE) FROM BASE_DATE) THEN (SELECT BASE_DATE-1 FROM BASE_DATE)
            ELSE(SELECT LAST_DAY(BASE_DATE) FROM BASE_DATE)
        END AS MONTH_END,
        (SELECT BASE_DATE-7 FROM BASE_DATE) AS WEEK_START,
        (SELECT BASE_DATE-1 FROM BASE_DATE) AS WEEK_END
    FROM DUAL
)
;


-- stickiness = DAU/MAU 
WITH daily AS (
SELECT
  date_format(created_at, "%Y-%m-%d") AS day,
  date_format(created_at, "%M %Y") AS month,
  count(*) AS dau
FROM
  login_history
GROUP BY
  date_format(created_at, "%Y-%m-%d"),
  date_format(created_at, "%M %Y")
),
monthly AS (
SELECT
  date_format(created_at, "%M %Y") AS month,
  count(user_id) AS mau
FROM
  login_history
GROUP BY
  date_format(created_at, "%M %Y")
)
SELECT
daily.day,
daily.dau,
monthly.mau,
concat(
  round(daily.dau / monthly.mau * 100, 1),
  '%'
) AS 'DAU/MAU'
FROM daily
	JOIN monthly ON daily.month = monthly.month
order BY daily.day desc
;



-- 롤링 리텐션 
WITH TEMP as
(   SELECT customer_id, DATE(MIN(invoice_date)) AS first_purchase,
         DATE(MAX(invoice_date)) AS recent_purchase,
         DATE_DIFF(DATE(MAX(invoice_date)), DATE(MIN(invoice_date)), DAY) AS diff_day
  FROM data.sales
  GROUP BY customer_id
)
SELECT COUNT(customer_id) AS total_customer,
       COUNT(CASE WHEN diff_day>=29 THEN 1 END) AS retention_customer,
       COUNT(CASE WHEN diff_day>=29 THEN 1 END) / COUNT(customer_id) AS rolling_retention_30
FROM temp
;






