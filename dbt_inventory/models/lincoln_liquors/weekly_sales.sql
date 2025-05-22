{{
  config(
    materialized = 'table',
    schema       = 'lincoln_liquors'
  )
}}

WITH daily AS (
  SELECT
    date::date                         AS sales_date,
    SUM(bottles_sold)                  AS daily_bottles
  FROM lincoln_liquors.iowa_liquor_sales
  GROUP BY date::date
),

weekly AS (
  SELECT
    EXTRACT(isoyear FROM sales_date)::int  AS sales_year,
    EXTRACT(week    FROM sales_date)::int  AS sales_week,
    SUM(daily_bottles)                     AS sales_quantity
  FROM daily
  GROUP BY 1,2
),

weekly_calc AS (
  SELECT
    sales_year,
    sales_week,
    sales_quantity                     AS current_sales_quantity,

    -- Lag 1 week
    CASE WHEN sales_week > 1 THEN sales_year ELSE sales_year - 1 END                AS prev1_year,
    CASE
      WHEN sales_week > 1
        THEN sales_week - 1
      ELSE (SELECT EXTRACT(week FROM make_date(sales_year - 1, 12, 28))::int)
    END                                                                              AS prev1_week,

    -- Lag 2 weeks
    CASE WHEN sales_week > 2 THEN sales_year ELSE sales_year - 1 END                AS prev2_year,
    CASE
      WHEN sales_week > 2
        THEN sales_week - 2
      ELSE ((SELECT EXTRACT(week FROM make_date(sales_year - 1, 12, 28))::int) - (2 - sales_week))
    END                                                                              AS prev2_week,

    -- Lag 3 weeks
    CASE WHEN sales_week > 3 THEN sales_year ELSE sales_year - 1 END                AS prev3_year,
    CASE
      WHEN sales_week > 3
        THEN sales_week - 3
      ELSE ((SELECT EXTRACT(week FROM make_date(sales_year - 1, 12, 28))::int) - (3 - sales_week))
    END                                                                              AS prev3_week,

    -- Lag 4 weeks
    CASE WHEN sales_week > 4 THEN sales_year ELSE sales_year - 1 END                AS prev4_year,
    CASE
      WHEN sales_week > 4
        THEN sales_week - 4
      ELSE ((SELECT EXTRACT(week FROM make_date(sales_year - 1, 12, 28))::int) - (4 - sales_week))
    END                                                                              AS prev4_week,

    -- Lag 13 weeks
    CASE WHEN sales_week > 13 THEN sales_year ELSE sales_year - 1 END               AS prev13_year,
    CASE
      WHEN sales_week > 13
        THEN sales_week - 13
      ELSE ((SELECT EXTRACT(week FROM make_date(sales_year - 1, 12, 28))::int) - (13 - sales_week))
    END                                                                              AS prev13_week,

    -- Lag 26 weeks
    CASE WHEN sales_week > 26 THEN sales_year ELSE sales_year - 1 END               AS prev26_year,
    CASE
      WHEN sales_week > 26
        THEN sales_week - 26
      ELSE ((SELECT EXTRACT(week FROM make_date(sales_year - 1, 12, 28))::int) - (26 - sales_week))
    END                                                                              AS prev26_week,

    -- Lag 52 weeks (previous year same week)
    sales_year - 1                                                                   AS prev52_year,
    sales_week                                                                       AS prev52_week

  FROM weekly
)

SELECT
  w.sales_year,
  w.sales_week,
  w.current_sales_quantity,

  -- 1-week lag
  w.prev1_year       AS last_week_year,
  w.prev1_week       AS last_week_week,
  p1.sales_quantity  AS last_week_quantity,
  w.current_sales_quantity - p1.sales_quantity           AS last_week_diff,

  -- 2-week lag
  w.prev2_year       AS last_2_week_year,
  w.prev2_week       AS last_2_week_week,
  p2.sales_quantity  AS last_2_week_quantity,
  w.current_sales_quantity - p2.sales_quantity           AS last_2_week_diff,

  -- 3-week lag
  w.prev3_year       AS last_3_week_year,
  w.prev3_week       AS last_3_week_week,
  p3.sales_quantity  AS last_3_week_quantity,
  w.current_sales_quantity - p3.sales_quantity           AS last_3_week_diff,

  -- 4-week lag
  w.prev4_year       AS last_4_week_year,
  w.prev4_week       AS last_4_week_week,
  p4.sales_quantity  AS last_4_week_quantity,
  w.current_sales_quantity - p4.sales_quantity           AS last_4_week_diff,

  -- 13-week lag
  w.prev13_year      AS last_13_week_year,
  w.prev13_week      AS last_13_week_week,
  p13.sales_quantity AS last_13_week_quantity,
  w.current_sales_quantity - p13.sales_quantity         AS last_13_week_diff,

  -- 26-week lag
  w.prev26_year      AS last_26_week_year,
  w.prev26_week      AS last_26_week_week,
  p26.sales_quantity AS last_26_week_quantity,
  w.current_sales_quantity - p26.sales_quantity         AS last_26_week_diff,

  -- 52-week lag
  w.prev52_year      AS last_year_year,
  w.prev52_week      AS last_year_week,
  p52.sales_quantity AS last_year_quantity,
  w.current_sales_quantity - p52.sales_quantity         AS last_year_diff

FROM weekly_calc AS w

LEFT JOIN weekly AS p1
  ON p1.sales_year = w.prev1_year
 AND p1.sales_week = w.prev1_week

LEFT JOIN weekly AS p2
  ON p2.sales_year = w.prev2_year
 AND p2.sales_week = w.prev2_week

LEFT JOIN weekly AS p3
  ON p3.sales_year = w.prev3_year
 AND p3.sales_week = w.prev3_week

LEFT JOIN weekly AS p4
  ON p4.sales_year = w.prev4_year
 AND p4.sales_week = w.prev4_week

LEFT JOIN weekly AS p13
  ON p13.sales_year = w.prev13_year
 AND p13.sales_week = w.prev13_week

LEFT JOIN weekly AS p26
  ON p26.sales_year = w.prev26_year
 AND p26.sales_week = w.prev26_week

LEFT JOIN weekly AS p52
  ON p52.sales_year = w.prev52_year
 AND p52.sales_week = w.prev52_week

ORDER BY
  w.sales_year,
  w.sales_week
