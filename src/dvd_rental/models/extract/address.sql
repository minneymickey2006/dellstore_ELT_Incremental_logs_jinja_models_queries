{% set config = {
    "extract_type": full
}%}

select
    address_id,
    address,
    address2,
    district,
    city_id,
    postal_code,
    phone,
    phone,
    last_update
from
    {{ source_table }}