{{
    config(
        materialized = "view"
    )
}}

select
    ProductID,
    Name,
    ProductNumber,
    Color,
    StandardCost,
    ListPrice,
    Size,
    Weight,
    ProductCategoryID,
    ProductModelID,
    SellStartDate,
    SellEndDate,
    DiscontinuedDate,
    ThumbNailPhoto,
    ThumbnailPhotoFileName,
    rowguid,
    ModifiedDate,
    current_timestamp() as _loaded_at
from {{ source('saleslt', 'product') }}
