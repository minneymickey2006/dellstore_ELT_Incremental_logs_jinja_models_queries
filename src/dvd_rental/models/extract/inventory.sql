{% set config = {
    "extract_type": "incremental",
    "incremental_column": "last_update",
    "key_columns":["inventory_id"]

} %}

select 
    inventory_id, 
    film_id, 
    store_id, 
    last_update 
from 
    {{ source_table }}

{% if is_incremental %}
    where last_upadte > '{{ incremental_value }}'
{% endif %}
