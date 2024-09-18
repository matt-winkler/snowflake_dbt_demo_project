{%- macro create_constraints(results, graph) -%}
  {% if execute %}
    
    {%- set model_nodes = graph['nodes'].values() | selectattr("resource_type", "equalto", "model") -%}
    {%- for res in results -%}
      {%- if res.node.config.materialized in ['table', 'incremental', 'snapshot', 'seed'] -%}
        {%- set table_identifier = res.node.database ~ '.' ~ res.node.schema ~ '.' ~ res.node.alias -%}
        {%- do log('on node: ' ~ table_identifier, info=true) -%}
        {%- set constraints = [] -%}
        
        {# extract node level constraints #}
        {%- set node_constraints = res.node.config.meta.constraints -%}
        {%- if node_constraints -%}
          {%- for node_const in node_constraints -%}
            {%- if node_const['type'] == 'foreign_key' -%}
              {%- do log('setting foreign key constraints at the node level is not supported. use column level specification instead', info=true) -%}
            {%- else -%}
              {%- do constraints.append({
                'type': node_const['type'], 
                'columns': node_const['columns']
              }) -%}
            {%- endif -%}
          {%- endfor -%}
        {%- endif -%}

        {# extract the column level constraints #}
        {%- set columns = res.node.columns -%}
        {%- for col in columns -%}
          {%- set col_constraints = columns[col].meta.constraints -%}
          {%- if col_constraints -%}
            {%- for col_const in col_constraints -%}
              {%- do constraints.append({
                'type': col_const,
                'columns': [col]
              }) -%}
            {%- endfor -%}
          {%- endif -%}
        {%- endfor %}
        
        {# apply extracted constraints #}
        {%- for const in constraints -%}
          {%- do log('applying constraint: ' ~ const, info=true) -%}
          {%- do create_constraint_from_config(
            model_nodes, 
            constraint=const['type'], 
            table_identifier=table_identifier, 
            column_names=const['columns'])
          -%}
      
        {%- endfor -%}

      {%- endif -%}
    {%- endfor -%}

  {%- endif -%}
{%- endmacro -%}

{%- macro create_constraint_from_config(model_nodes, constraint, table_identifier, column_names) -%}
  {%- if constraint not in ['unique', 'not_null', 'primary_key'] and constraint.keys()|list != ['foreign_key'] -%}
    {%- do exceptions.raise_compiler_error(
         "`constraint` argument must be one of ['unique', 'not_null', 'primary_key', 'foreign_key'] Got: '" ~ constraint ~"'.'"
    ) -%}
  {%- endif -%}

  {%- if constraint == 'unique' -%}
    {%- do create_unique_key(table_identifier, column_names=column_names) -%}
  
  {%- elif constraint == 'not_null' -%}  
    {%- do create_not_null(table_identifier, column_names=column_names) -%}
  
  {%- elif constraint == 'primary_key' -%}  
    {%- do create_primary_key(table_identifier, column_names=column_names) -%}
  
  {%- elif constraint.keys()|list == ['foreign_key'] -%}
    {%- set pk_relation = constraint['foreign_key']['to'] -%}
    {%- set pk_column = constraint['foreign_key']['field'] -%}

    {%- for node in model_nodes -%}
      
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