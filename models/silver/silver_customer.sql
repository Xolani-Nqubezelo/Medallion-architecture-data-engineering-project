{{
    config(
        materialized = "table",
        file_format = "delta",
        location_root = "/mnt/silver/customer"
    )
}}

with source as (
    select * from {{ ref('bronze_customer') }}
),

deduped as (
    select
        *,
        row_number() over (partition by CustomerID order by ModifiedDate desc) as _row_num
    from source
),

cleaned as (
    select
        CustomerID,
        cast(NameStyle as boolean)                                        as NameStyle,
        trim(Title)                                                        as Title,
        trim(FirstName)                                                    as FirstName,
        trim(MiddleName)                                                   as MiddleName,
        trim(LastName)                                                     as LastName,
        trim(Suffix)                                                       as Suffix,
        trim(CompanyName)                                                  as CompanyName,
        trim(SalesPerson)                                                  as SalesPerson,
        lower(trim(EmailAddress))                                          as EmailAddress,
        trim(Phone)                                                        as Phone,
        concat_ws(' ',
            nullif(trim(FirstName), ''),
            nullif(trim(MiddleName), ''),
            nullif(trim(LastName), '')
        )                                                                  as FullName,
        cast(ModifiedDate as timestamp)                                    as ModifiedDate,
        _loaded_at
    from deduped
    where _row_num = 1
      and CustomerID is not null
)

select * from cleaned
