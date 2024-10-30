{{
  config(
    materialized = 'incremental',
    unique_key = ['customer_id', 'day_time'],
    incremental_strategy = 'delete+insert'
  )
}}

WITH daily_fees AS (
    SELECT 
        customer_id,
        day_time,
        SUM(protocol_fee_amount) as daily_fee_amount
    FROM {{ ref('stg_transactions') }}
    {% if is_incremental() %}
        WHERE day_time >= (SELECT MAX(day_time) FROM {{ this }})
    {% endif %}
    GROUP BY customer_id, day_time
),

cumulative_fees AS (
    SELECT 
        customer_id,
        day_time,
        SUM(daily_fee_amount) OVER (
            PARTITION BY customer_id 
            ORDER BY day_time
            ROWS UNBOUNDED PRECEDING
        ) as total_protocol_fee_amount
    FROM daily_fees
)

SELECT 
    customer_id,
    day_time,
    total_protocol_fee_amount as protocol_fee_amount
FROM cumulative_fees
