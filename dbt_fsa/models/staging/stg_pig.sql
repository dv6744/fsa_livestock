with source as (
    select
        *,
        _TABLE_SUFFIX as table_suffix
    from {{ source("raw", "pig_data") }}
),

renamed as (
    select
        cast(Species as string) as species,
        cast(InspectionType as string) as inspection_type,
        cast(Condition as string) as condition,
        parse_date('%Y-%m', YearMonth) as date_yearmonth,
        cast(Country as string) as country,
        cast(NumberOfConditions as int64) as num_conditions,
        cast(Throughput as int64) as throughput,
        cast(NumberOfThroughputPlants as int64) as num_throughput_plants,
        cast(PercentageOfThroughput as float64) as pct_throughput,
        table_suffix
    from source
)

select * from renamed
