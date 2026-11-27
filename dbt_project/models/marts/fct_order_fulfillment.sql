-- One row per sales order: was it delivered, on time, and how much was picked?
-- An order has at most one invoice; the invoice carries the confirmed delivery time.
with order_lines as (
    select
        order_id,
        count(*)            as order_line_count,
        sum(quantity)       as ordered_quantity,
        sum(picked_quantity) as picked_quantity
    from {{ ref('stg_sales_order_lines') }}
    group by order_id
),

delivered as (
    select
        o.order_id,
        o.customer_id,
        o.salesperson_person_id,
        o.order_date,
        o.expected_delivery_date,
        o.backorder_order_id,
        i.invoice_id,
        i.invoice_date,
        i.confirmed_delivery_time,
        cast(i.confirmed_delivery_time as date) as delivered_date
    from {{ ref('stg_sales_orders') }} o
    left join {{ ref('stg_sales_invoices') }} i on i.order_id = o.order_id
)

select
    d.order_id,
    d.customer_id,
    d.salesperson_person_id,
    d.order_date,
    d.expected_delivery_date,
    d.invoice_id,
    d.invoice_date,
    d.confirmed_delivery_time,
    d.delivered_date,
    d.delivered_date is not null                              as is_delivered,
    coalesce(d.delivered_date <= d.expected_delivery_date, false) as is_on_time,
    {{ dbt.datediff('d.order_date', 'd.delivered_date', 'day') }}             as days_to_deliver,
    {{ dbt.datediff('d.expected_delivery_date', 'd.delivered_date', 'day') }} as days_late,
    d.backorder_order_id is not null                          as has_backorder,
    l.order_line_count,
    l.ordered_quantity,
    l.picked_quantity
from delivered d
left join order_lines l on l.order_id = d.order_id
