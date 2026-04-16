with staged_pig as (
    select * from {{ ref("stg_pig") }}
),

staged_poultry as (
    select * from {{ ref("stg_poultry") }}
)

select * from staged_pig
union all
select * from staged_poultry
