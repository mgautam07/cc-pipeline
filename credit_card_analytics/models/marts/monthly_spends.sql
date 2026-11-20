with transactions as (
    select month(txn_date) as txn_month, year(txn_date) as txn_year, amount
    from {{ source('silver', 'transactions') }}
    where merchant not like 'bbps payment received%'
      and merchant not like 'cashback%'
),
cashback as (
    select month(txn_date) as txn_month, year(txn_date) as txn_year, amount
    from {{ source('silver', 'transactions') }}
    where merchant like 'cashback%'
),
calculation as (
    select
        txn_month,
        txn_year,
        sum(t.amount) as total_amount,
        sum(c.amount) as total_cashback,
        count(*) as total_number_of_transactions
    from transactions t
    left join cashback c using(txn_month, txn_year)
    group by txn_month, txn_year
    order by txn_month, txn_year desc
),
monthly_change as (
    select
        txn_month,
        txn_year,
        total_amount,
        total_cashback,
        total_number_of_transactions,
        lag(total_amount) over (order by txn_month desc, txn_year desc) as last_month_total_amount
    from calculation
)
select
    txn_month,
    txn_year,
    total_number_of_transactions,
    total_cashback,
    total_amount,
    (total_amount - last_month_total_amount) / last_month_total_amount as month_over_month_change
from monthly_change 