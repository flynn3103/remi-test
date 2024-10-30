SELECT
    t.customer_id,
    t.day_time,
    ABS(
        SUM(t.protocol_fee_amount) - 
        dcm.protocol_fee_amount
    ) as difference
FROM {{ ref('stg_transactions') }} t
JOIN {{ ref('daily_customer_metrics') }} dcm
    ON t.customer_id = dcm.customer_id
    AND t.day_time = dcm.day_time
GROUP BY t.customer_id, t.day_time, dcm.protocol_fee_amount
HAVING difference > 0.0001