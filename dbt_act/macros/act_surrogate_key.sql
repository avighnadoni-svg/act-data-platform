{% macro act_surrogate_key(columns) %}

    {{ return(
        dbt_utils.generate_surrogate_key(columns)
    ) }}

{% endmacro %}