select
    il.invoice_line_id,
    i.invoice_id,
    i.order_id,
    i.invoice_date,
    i.customer_id,
    i.salesperson_person_id,
    il.stock_item_id,
    il.quantity,
    il.unit_price,
    il.extended_price                                       as revenue_incl_tax,
    cast(il.extended_price - il.tax_amount as decimal(18, 2)) as revenue_excl_tax,
    il.tax_amount,
    il.line_profit,
    i.is_credit_note
from {{ ref('stg_sales_invoice_lines') }} il
join {{ ref('stg_sales_invoices') }} i on i.invoice_id = il.invoice_id
