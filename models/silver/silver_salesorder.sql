{{
    config(
        materialized = "table",
        file_format = "delta",
        location_root = "/mnt/silver/salesorder"
    )
}}

with header_source as (
    select * from {{ ref('bronze_salesorderheader') }}
),

detail_source as (
    select * from {{ ref('bronze_salesorderdetail') }}
),

header_deduped as (
    select
        *,
        row_number() over (partition by SalesOrderID order by ModifiedDate desc) as _row_num
    from header_source
),

detail_deduped as (
    select
        *,
        row_number() over (partition by SalesOrderDetailID order by ModifiedDate desc) as _row_num
    from detail_source
),

header_cleaned as (
    select
        SalesOrderID,
        cast(RevisionNumber as int)                  as RevisionNumber,
        cast(OrderDate as date)                      as OrderDate,
        cast(DueDate as date)                        as DueDate,
        cast(ShipDate as date)                       as ShipDate,
        cast(Status as tinyint)                      as Status,
        cast(OnlineOrderFlag as boolean)             as OnlineOrderFlag,
        trim(SalesOrderNumber)                       as SalesOrderNumber,
        nullif(trim(PurchaseOrderNumber), '')        as PurchaseOrderNumber,
        nullif(trim(AccountNumber), '')              as AccountNumber,
        CustomerID,
        ShipToAddressID,
        BillToAddressID,
        trim(ShipMethod)                             as ShipMethod,
        nullif(trim(CreditCardApprovalCode), '')     as CreditCardApprovalCode,
        cast(SubTotal as decimal(19, 4))             as SubTotal,
        cast(TaxAmt as decimal(19, 4))               as TaxAmt,
        cast(Freight as decimal(19, 4))              as Freight,
        cast(TotalDue as decimal(19, 4))             as TotalDue,
        nullif(trim(Comment), '')                    as Comment,
        cast(ModifiedDate as timestamp)              as ModifiedDate,
        _loaded_at
    from header_deduped
    where _row_num = 1
      and SalesOrderID is not null
      and CustomerID is not null
),

detail_cleaned as (
    select
        SalesOrderDetailID,
        SalesOrderID,
        cast(OrderQty as int)                        as OrderQty,
        ProductID,
        cast(UnitPrice as decimal(19, 4))            as UnitPrice,
        cast(UnitPriceDiscount as decimal(5, 4))     as UnitPriceDiscount,
        cast(LineTotal as decimal(38, 6))            as LineTotal,
        cast(ModifiedDate as timestamp)              as ModifiedDate
    from detail_deduped
    where _row_num = 1
      and SalesOrderDetailID is not null
),

joined as (
    select
        d.SalesOrderDetailID,
        d.SalesOrderID,
        h.SalesOrderNumber,
        h.CustomerID,
        h.OrderDate,
        h.DueDate,
        h.ShipDate,
        h.Status,
        h.OnlineOrderFlag,
        h.PurchaseOrderNumber,
        h.AccountNumber,
        h.ShipToAddressID,
        h.BillToAddressID,
        h.ShipMethod,
        h.SubTotal,
        h.TaxAmt,
        h.Freight,
        h.TotalDue,
        h.Comment,
        d.ProductID,
        d.OrderQty,
        d.UnitPrice,
        d.UnitPriceDiscount,
        d.LineTotal,
        h.ModifiedDate,
        h._loaded_at
    from detail_cleaned d
    inner join header_cleaned h on d.SalesOrderID = h.SalesOrderID
)

select * from joined
