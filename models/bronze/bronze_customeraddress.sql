{{
    config(
        materialized = "view"
    )
}}

select
    CustomerID,
    AddressID,
    AddressType,
    rowguid,
    ModifiedDate,
    current_timestamp() as _loaded_at
from {{ source('saleslt', 'customeraddress') }}
