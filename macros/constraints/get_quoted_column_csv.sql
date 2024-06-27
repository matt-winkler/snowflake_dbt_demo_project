{%- macro get_quoted_column_csv(column_array, quote_columns=false) -%}

    {%- set column_list = get_quoted_column_list(column_array, quote_columns) -%}
    {%- set columns_csv=column_list | join(', ') -%}
    {{ return(columns_csv) }}

{%- endmacro -%}

{%- macro get_quoted_column_list(column_array, quote_columns=false) -%}

    {%- if not quote_columns -%}
        {%- set column_list=column_array -%}
    {%- elif quote_columns -%}
        {%- set column_list=[] -%}
        {%- for column in column_array -%}
            {%- set column_list = column_list.append( adapter.quote(column) ) -%}
        {%- endfor -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "`quote_columns` argument must be one of [True, False] Got: '" ~ quote ~"'.'"
        ) }}
    {%- endif -%}

    {{ return(column_list) }}

{%- endmacro -%}