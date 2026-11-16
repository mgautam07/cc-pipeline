with transactions as (
    select month(txn_date) as txn_month, year(txn_date) as txn_year, merchant, amount
    from {{ source('silver', 'transactions') }}
    where is_credit = false
),

merchants as (
    select * from {{ ref('merchant_category') }}
),

combined as (
    select * from transactions t
    left join merchants m on t.merchant = m.statement_name
)

select
    txn_month,
    txn_year,
    coalesce(category, 'Uncategorized') as category,
    sum(amount) as total_amount
from combined
group by txn_month, txn_year, category
order by txn_month, txn_year, total_amount desc