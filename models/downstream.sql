-- depends_on: {{ ref('dbt_dugout', 'fct_player_salaries') }}
select * from {{ ref('dbt_dugout'~var('project_suffix'), 'dim_roster') }}