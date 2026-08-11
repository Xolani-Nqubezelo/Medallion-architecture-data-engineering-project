{{
    config(
        materialized = "table",
        file_format = "delta",
        location_root = "/mnt/silver/product"
    )
}}

with source as (
    select * from {{ ref('bronze_product') }}
),

deduped as (
    select
        *,
        row_number() over (partition by ProductID order by ModifiedDate desc) as _row_num
    from source
),

cleaned as (
    select
        ProductID,
        trim(Name)                                  as Name,
        trim(ProductNumber)                         as ProductNumber,
        nullif(trim(Color), '')                     as Color,
        cast(StandardCost as decimal(19, 4))        as StandardCost,
        cast(ListPrice as decimal(19, 4))           as ListPrice,
        nullif(trim(Size), '')                      as Size,
        cast(Weight as decimal(8, 2))               as Weight,
        ProductCategoryID,
        ProductModelID,
        cast(SellStartDate as date)                 as SellStartDate,
        cast(SellEndDate as date)                   as SellEndDate,
        cast(DiscontinuedDate as date)              as DiscontinuedDate,
        cast(ModifiedDate as timestamp)             as ModifiedDate,
        _loaded_at
    from deduped
    where _row_num = 1
      and ProductID is not null
      and Name is not null
)

select * from cleaned
