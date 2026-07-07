{% test elementary_test_materialization(bool_expression) %}

    select *
    from (select {{ bool_expression }}) as validation_errors
    where not validation_errors.{{ bool_expression }}

{% endtest %}
