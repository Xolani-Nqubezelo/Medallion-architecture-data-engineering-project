{{
    config(
        materialized = "view"
    )
}}

select
    AddressID,
    AddressLine1,
    AddressLine2,
    City,
    StateProvince,
    CountryRegion,
    PostalCode,
    rowguid,
    ModifiedDate,
    current_timestamp() as _loaded_at
from {{ source('saleslt', 'address') }}
