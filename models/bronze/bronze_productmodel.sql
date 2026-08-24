{{
    config(
        materialized = "view"
    )
}}

select
    ProductModelID,
    Name,
    CatalogDescription,
    rowguid,
    ModifiedDate,
    current_timestamp() as _loaded_at
from {{ source('saleslt', 'productmodel') }}
