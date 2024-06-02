{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {% set friend = var("friend", None) %}
    {% set model_to_rename = "result_friends_books" %}

    {%- if friend and node.name == model_to_rename -%}
        {{ friend | trim ~ "_" ~ "books" }}
    {%- else -%}
        {{ node.name }}
    {%- endif -%}

{%- endmacro %}