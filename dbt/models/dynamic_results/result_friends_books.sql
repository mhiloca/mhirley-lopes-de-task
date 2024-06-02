{%- set friend = var('friend') -%}


SELECT 
    fb."title",
    fb."author"
FROM {{ ref("friends_books") }} AS fb
WHERE team = '{{ friend }}'
