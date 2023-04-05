{% macro generate_incremental_predicates(date_column) %}

{% set begin_date_predicate %}
     {{date_column}} >= '{{var('begin_date', '1992-01-01')}}'
{% endset %}

{% set end_date_predicate %}
     {{date_column}} <= '{{var('end_date', '1998-08-02')}}'; 
{% endset %}

{% set predicate_array = [begin_date_predicate, end_date_predicate] %}

{{ return(predicate_array) }}

{% endmacro %}
