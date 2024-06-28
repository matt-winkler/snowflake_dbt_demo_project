{%- macro create_constraints(results, graph) -%}
  {%- for res in results -%}
    {%- if res.node.config.materialized in ['table', 'incremental', 'snapshot', 'seed'] -%}
      {%- set table_identifier = res.node.database ~ '.' ~ res.node.schema ~ '.' ~ res.node.alias -%}
      {%- do log('on node: ' ~ table_identifier, info=true) -%}
        {%- set columns = res.node.columns -%}
          {%- for col in columns -%}
            {%- set constraints = columns[col].meta.constraints -%}
            {%- for const in constraints -%}
              
              {%- do log('found constraint: ' ~ const ~ 'on column: ' ~ col, info=true) -%}
              
              {%- do create_constraint_from_config(
                graph['nodes'], 
                constraint=const, 
                table_identifier=table_identifier, 
                column_names=[col])
              -%}
            
            {%- endfor -%}
          {%- endfor %}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro -%}

{%- macro create_constraint_from_config(nodes, constraint, table_identifier, column_names) -%}
  {%- if constraint == 'unique' -%}
    {%- do create_unique_key(table_identifier, column_names=column_names) -%}
  
  {%- elif constraint == 'not_null' -%}  
    {%- do log('pass not_null', info=true) -%}
  
  {%- elif constraint == 'primary' -%}  
    {%- do log('pass primary', info=true) -%}
  
  {%- elif constraint.keys()|list == ['foreign_key'] -%}
    {%- set pk_relation = constraint['foreign_key']['to'] -%}
    {%- set pk_column = constraint['foreign_key', 'field'] -%}

    {# TODO: error handling if an invalid node reference to the foreign key table is specified #}

    {%- for node in nodes.values() | selectattr("resource_type", "equalto", "model") -%}
      
      {%- if node.name == pk_relation -%}
        {%- set pk_relation = [node.database, node.schema, node.alias]|join('.') -%}
        
        {%- do create_foreign_key(
            pk_table_relation=pk_relation, 
            pk_column_names=[pk_column], 
            fk_table_relation=table_identifier, 
            fk_column_names=column_names
        ) -%}
        
      {%- endif -%}
    {%- endfor -%}
    
  {%- endif -%}
{%- endmacro -%}