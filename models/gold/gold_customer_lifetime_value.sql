{{
    config(
        materialized = "table",
        file_format = "delta",
        location_root = "/mnt/gold/customer_lifetime_value"
    )
}}

-- Customer lifetime value with segmentation based on RFM (Recency, Frequency, Monetary) scores.
with sales as (
    select * from {{ ref('silver_salesorder') }}
),

customer_metrics as (
    select
        CustomerID,
        count(distinct SalesOrderID)        as order_count,
        sum(LineTotal)                      as total_revenue,
        avg(TotalDue)                       as avg_order_value,
        min(OrderDate)                      as first_order_date,
        max(OrderDate)                      as last_order_date,
        datediff(max(OrderDate), min(OrderDate)) as customer_tenure_days,
        count(distinct date_format(OrderDate, 'yyyy-MM')) as active_months
    from sales
    group by CustomerID
),

customer_info as (
    select
        CustomerID,
        FullName,
        EmailAddress,
        CompanyName
    from {{ ref('silver_customer') }}
),

-- Simple RFM segmentation
rfm as (
    select
        m.*,
        datediff(current_date(), m.last_order_date) as days_since_last_order,
        ntile(5) over (order by datediff(current_date(), m.last_order_date) desc) as recency_score,
        ntile(5) over (order by m.order_count desc)                               as frequency_score,
        ntile(5) over (order by m.total_revenue desc)                             as monetary_score
    from customer_metrics m
),

segmented as (
    select
        r.*,
        (recency_score + frequency_score + monetary_score) as rfm_total,
        case
            when (recency_score + frequency_score + monetary_score) >= 13 then 'Champions'
            when (recency_score + frequency_score + monetary_score) >= 10 then 'Loyal Customers'
            when (recency_score + frequency_score + monetary_score) >= 7  then 'Potential Loyalists'
            when recency_score >= 4 and frequency_score <= 2              then 'New Customers'
            when recency_score <= 2 and frequency_score >= 3              then 'At Risk'
            when recency_score <= 2 and frequency_score <= 2              then 'Lost'
            else 'Needs Attention'
        end as customer_segment
    from rfm
)

select
    s.CustomerID,
    c.FullName,
    c.EmailAddress,
    c.CompanyName,
    s.order_count,
    round(s.total_revenue, 2)           as total_revenue,
    round(s.avg_order_value, 2)         as avg_order_value,
    s.first_order_date,
    s.last_order_date,
    s.days_since_last_order,
    s.customer_tenure_days,
    s.active_months,
    s.recency_score,
    s.frequency_score,
    s.monetary_score,
    s.rfm_total,
    s.customer_segment
from segmented s
inner join customer_info c on s.CustomerID = c.CustomerID
