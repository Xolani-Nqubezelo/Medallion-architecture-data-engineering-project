{{
    config(
        materialized = "table",
        file_format = "delta",
        location_root = "/mnt/gold/sales_summary"
    )
}}

-- Monthly sales summary with key KPIs for BI dashboards and executive reporting.
with sales as (
    select * from {{ ref('silver_salesorder') }}
),

monthly_summary as (
    select
        date_format(OrderDate, 'yyyy-MM')               as order_month,
        year(OrderDate)                                 as order_year,
        month(OrderDate)                                as order_month_num,
        count(distinct SalesOrderID)                    as total_orders,
        count(distinct CustomerID)                      as unique_customers,
        sum(OrderQty)                                   as total_units_sold,
        sum(LineTotal)                                  as total_revenue,
        sum(TaxAmt)                                     as total_tax,
        sum(Freight)                                    as total_freight,
        sum(TotalDue)                                   as total_due,
        avg(TotalDue)                                   as avg_order_value,
        sum(LineTotal) / nullif(count(distinct SalesOrderID), 0) as revenue_per_order,
        count(distinct case when OnlineOrderFlag = true
            then SalesOrderID end)                      as online_orders,
        count(distinct case when OnlineOrderFlag = false
            then SalesOrderID end)                      as offline_orders
    from sales
    group by
        date_format(OrderDate, 'yyyy-MM'),
        year(OrderDate),
        month(OrderDate)
)

select
    order_month,
    order_year,
    order_month_num,
    total_orders,
    unique_customers,
    total_units_sold,
    round(total_revenue, 2)         as total_revenue,
    round(total_tax, 2)             as total_tax,
    round(total_freight, 2)         as total_freight,
    round(total_due, 2)             as total_due,
    round(avg_order_value, 2)       as avg_order_value,
    round(revenue_per_order, 2)     as revenue_per_order,
    online_orders,
    offline_orders,
    round(online_orders / nullif(total_orders, 0) * 100, 1) as online_order_pct
from monthly_summary
order by order_month
