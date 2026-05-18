{{
    config(
        materialized = 'table',
    )
}}

with days as (
    {{
        dbt.date_spine(
            'day',
            "to_date('01/01/2000','mm/dd/yyyy')",
            "to_date('01/01/2030','mm/dd/yyyy')"
        )
    }}
),

final as (
    select cast(date_day as date) as date_day
    from days
)

select * from final
where date_day >= dateadd(year, -10, current_date())
  and date_day < dateadd(year, 1, current_date())
