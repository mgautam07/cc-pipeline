with merchants as (
    select * from {{ ref('merchant_category') }}
),

transactions as (
    select txn_date, merchant, amount, type
    from {{ source('silver', 'transactions') }}
)

select *
from transactions t
left join merchants m on t.merchant = m.statement_name
where m.statement_name is null
      and t.merchant not like 'bbps payment received%'
      and t.merchant not like 'cashback%'
order by txn_date desc
