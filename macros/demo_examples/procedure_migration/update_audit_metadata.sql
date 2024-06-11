{% macro update_audit_metadata(
    audit_metadata_tgt='pass',
    transaction_metadata_tgt='pass',
    ln_max_batch_sk_cbx_resp='pass',
    li_process_interval='pass',
    ln_commit_cnt='pass',
    target_table='pass',
    md_table_name='pass'
) %}

  {% if audit_metadata_tgt == 'pass' %}
     {{return("select 'placeholder for metadata table updates'")}}
  {% endif %}
  
  {% set audit_metadata_update_sql %}
    UPDATE {{audit_metadata_tgt}}
    SET ins_src_batch_sk  = {{ln_max_batch_sk_cbx_resp}}
    WHERE UPPER(order_process_cd) = {{md_table_name}};
  {% endset %}

  {% do run_query(audit_metadata_update_sql) %}
  
  {% set transaction_metadata_update_sql %}
    UPDATE {{transaction_metadata_tgt}}
    SET process_interval = {{li_process_interval}},
    update_cnt = {{ln_commit_cnt}}
    WHERE UPPER(order_process_cd) = {{md_table_name}};
  {% endset %}

  {% do run_query(transaction_metadata_update_sql) %}
  
  {% set ld_max_ts_update_sql %}
    SELECT COALESCE(MAX(cep_change_ts),ld_min_ts) INTO ld_max_ts 
    FROM {{target_table}}
  {% endset %}

  {% do run_query(ld_max_ts_update_sql) %}

{% endmacro %}