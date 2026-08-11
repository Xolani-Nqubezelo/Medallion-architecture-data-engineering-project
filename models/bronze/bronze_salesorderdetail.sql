{{
    config(
        materialized = "view"
    )
}}

select
    SalesOrderID,
    SalesOrderDetailID,
    OrderQty,
    ProductID,
    UnitPrice,
    UnitPriceDiscount,
    LineTotal,
    rowguid,
    ModifiedDate,
    current_timestamp() as _loaded_at
from {{ source('saleslt', 'salesorderdetail') }}
