{% set config ={
    "extract_type": "full"
} %}

select
    staff_id,
    first_name,
    last_name,
    address_id,
    email,
    store_id,
    active,
    username,
    last_upadte
from
    {{ source_table }}