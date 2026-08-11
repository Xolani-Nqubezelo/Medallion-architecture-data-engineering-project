{{
    config(
        materialized = "view"
    )
}}

select
    CustomerID,
    NameStyle,
    Title,
    FirstName,
    MiddleName,
    LastName,
    Suffix,
    CompanyName,
    SalesPerson,
    EmailAddress,
    Phone,
    rowguid,
    ModifiedDate,
    current_timestamp() as _loaded_at
from {{ source('saleslt', 'customer') }}
