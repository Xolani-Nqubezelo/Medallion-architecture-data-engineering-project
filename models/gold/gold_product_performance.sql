{{
    config(
        materialized = "table",
        file_format = "delta",
        location_root = "/mnt/gold/product_performance"
    )
}}

-- Product performance metrics aggregated across the full order history.
with sales as (
    select * from {{ ref('silver_salesorder') }}
),

products as (
    select * from {{ ref('silver_product') }}
),

product_metrics as (
    select
        s.ProductID,
        sum(s.OrderQty)                                 as total_units_sold,
        count(distinct s.SalesOrderID)                  as total_orders,
        sum(s.LineTotal)                                as total_revenue,
        avg(s.UnitPrice)                                as avg_selling_price,
        min(s.UnitPrice)                                as min_selling_price,
        max(s.UnitPrice)                                as max_selling_price,
        avg(s.UnitPriceDiscount)                        as avg_discount_rate,
        sum(s.UnitPriceDiscount * s.UnitPrice * s.OrderQty)
                                                        as total_discount_amount,
        min(s.OrderDate)                                as first_sale_date,
        max(s.OrderDate)                                as last_sale_date
    from sales s
    group by s.ProductID
),

enriched as (
    select
        p.ProductID,
        p.Name                                          as product_name,
        p.ProductNumber,
        p.Color,
        p.StandardCost,
        p.ListPrice,
        p.Size,
        p.Weight,
        p.SellStartDate,
        p.SellEndDate,
        p.DiscontinuedDate,
        m.total_units_sold,
        m.total_orders,
        round(m.total_revenue, 2)                       as total_revenue,
        round(m.avg_selling_price, 2)                   as avg_selling_price,
        round(m.min_selling_price, 2)                   as min_selling_price,
        round(m.max_selling_price, 2)                   as max_selling_price,
        round(m.avg_discount_rate * 100, 2)             as avg_discount_pct,
        round(m.total_discount_amount, 2)               as total_discount_amount,
        round((m.avg_selling_price - p.StandardCost) /
              nullif(m.avg_selling_price, 0) * 100, 2)  as gross_margin_pct,
        round(m.total_revenue - (p.StandardCost * m.total_units_sold), 2)
                                                        as gross_profit,
        m.first_sale_date,
        m.last_sale_date
    from products p
    left join product_metrics m on p.ProductID = m.ProductID
)

select * from enriched
