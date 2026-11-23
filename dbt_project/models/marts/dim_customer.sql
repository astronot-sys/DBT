select
    c.customer_id,
    c.customer_name,
    bt.customer_name                              as bill_to_customer,
    cc.customer_category_name                     as customer_category,
    coalesce(bg.buying_group_name, 'Independent') as buying_group,
    ci.city_name,
    sp.state_province_name,
    sp.sales_territory,
    co.country_name,
    c.credit_limit,
    c.account_opened_date,
    c.is_on_credit_hold
from {{ ref('stg_sales_customers') }} c
left join {{ ref('stg_sales_customers') }} bt             on bt.customer_id = c.bill_to_customer_id
left join {{ ref('stg_sales_customer_categories') }} cc   on cc.customer_category_id = c.customer_category_id
left join {{ ref('stg_sales_buying_groups') }} bg         on bg.buying_group_id = c.buying_group_id
left join {{ ref('stg_application_cities') }} ci          on ci.city_id = c.delivery_city_id
left join {{ ref('stg_application_state_provinces') }} sp on sp.state_province_id = ci.state_province_id
left join {{ ref('stg_application_countries') }} co       on co.country_id = sp.country_id