WITH date_range AS (
	SELECT DATE_TRUNC('month', CURRENT_DATE) AS start_date,
		DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1' MONTH - INTERVAL '1' DAY AS end_date
),
accounts AS (
	SELECT DISTINCT accountid as aws_account_id
	FROM test_table2
),
days AS (
	SELECT concat(
			cast(year(start_date) as varchar(4)),
			'-',
			lpad(cast(month(start_date) as varchar(3)), 2, '0'),
			'-',
			lpad(cast((abs(d._day) + 1) as varchar(3)), 2, '0')
		) as date 
	FROM (
			SELECT start_date,
				sequence(
					0,
					DATE_DIFF(
						'day',
						date_range.end_date,
						date_range.start_date
					)
				) AS days
			FROM date_range
		),
		UNNEST(days) AS d(_day)
),
missing_days AS (
	SELECT accounts.aws_account_id,
		d.date
	FROM accounts
		CROSS JOIN days d
		LEFT JOIN (
			SELECT accountid,
				date_trunc('day', timestamp) AS date
			FROM test_table2,
				date_range
			WHERE timestamp >= date_range.start_date
				AND timestamp <= date_range.end_date
		) t ON accounts.aws_account_id = t.accountid
		AND cast(d.date as timestamp) = t.date
	WHERE t.accountid IS NULL
)
SELECT aws_account_id as accountid, date as missing_days
FROM missing_days
ORDER BY aws_account_id,
	date
