{{
  config(
    materialized = 'view'
  )
}}

SELECT
    customer_id,
    CAST(transaction_time AS DATE) as day_time,
    CAST(protocol_fee_amount AS FLOAT) as protocol_fee_amount
FROM {{ ref('transactions') }}