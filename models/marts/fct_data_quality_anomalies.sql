with alerts as (
    select * from {{ ref('alerts_anomaly_detection') }}
),

owner_expanded as (
    select
        a.alert_id,
        a.data_issue_id,
        a.test_execution_id,
        a.test_unique_id,
        a.model_unique_id,
        a.detected_at,
        cast(a.detected_at as date) as detected_date,
        a.database_name,
        a.schema_name,
        a.table_name,
        a.column_name,
        a.alert_type,
        a.sub_type,
        a.alert_description,
        a.owners as owners_raw,
        coalesce(
            nullif(trim(regexp_replace(owner_value.value::string, '^"|"$', '')), ''),
            'unassigned'
        ) as owner_name,
        a.tags,
        a.alert_results_query,
        a.other,
        a.test_name,
        a.test_short_name,
        a.test_params,
        a.severity,
        a.status,
        a.result_rows
    from alerts as a,
        lateral flatten(
            input => coalesce(
                try_parse_json(nullif(a.owners, '')),
                to_variant(array_construct(nullif(a.owners, '')))
            ),
            outer => true
        ) as owner_value
),

final as (
    select
        md5(coalesce(alert_id, '') || '|' || owner_name) as alert_owner_id,
        alert_id,
        data_issue_id,
        test_execution_id,
        test_unique_id,
        model_unique_id,
        detected_at,
        detected_date,
        database_name,
        schema_name,
        table_name,
        column_name,
        database_name || '.' || schema_name || '.' || table_name as monitored_relation,
        coalesce(column_name, '__table__') as monitored_column,
        alert_type,
        sub_type,
        alert_description,
        owners_raw,
        owner_name,
        tags,
        alert_results_query,
        other,
        test_name,
        test_short_name,
        test_params,
        severity,
        status,
        result_rows
    from owner_expanded
)

select * from final
