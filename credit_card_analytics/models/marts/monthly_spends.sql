with transactions as (
    select month(txn_date) as txn_month, year(txn_date) as txn_year,amount
    from {{ source('silver', 'transactions') }}
    where merchant not like 'bbps payment received%'
      and merchant not like 'cashback%'
)

select
    txn_month,
    txn_year,
    sum(amount) as total_amount
from transactions
group by txn_month, txn_year
order by txn_month, txn_year desc