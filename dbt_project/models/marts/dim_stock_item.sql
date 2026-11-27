select
    si.stock_item_id,
    si.stock_item_name,
    si.brand,
    si.size,
    si.unit_price,
    si.recommended_retail_price,
    si.is_chiller_stock,
    {{ json_text('si.custom_fields', 'CountryOfManufacture') }} as country_of_manufacture,
    {{ json_string_array('si.custom_fields', 'Tags') }}         as tags,
    col.color_name,
    s.supplier_name
from {{ ref('stg_warehouse_stock_items') }} si
left join {{ ref('stg_warehouse_colors') }} col   on col.color_id = si.color_id
left join {{ ref('stg_purchasing_suppliers') }} s on s.supplier_id = si.supplier_id
