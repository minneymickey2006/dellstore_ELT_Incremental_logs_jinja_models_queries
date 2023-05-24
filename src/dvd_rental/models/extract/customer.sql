{% set config = {
    "extract_type": "full"
} %}

select 
    customer_id, 
    store_id, 
    first_name, 
    last_name,
    email,
    address_id,
    activebool,
    create_date,
    last_update,
    active
from 
    {{ source_table }}
