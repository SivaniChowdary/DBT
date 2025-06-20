with
    cte_customers as (
        select id as customer_id, first_name, last_name from {{ ref("stg_customers") }}
    ),

    cte_orders as (
        select id as order_id, user_id, order_date, status from {{ ref("stg_orders") }}
    ),

    cte_country as (select * from {{ ref("customer_info") }}),

    transform as (
        select cte_customers.*, cte_orders.*, cte_country.country
        from cte_customers
        left join cte_orders
        left join cte_country
        where
            cte_orders.user_id = cte_customers.customer_id
            or cte_country.user_id = cte_customers.customer_id
    )

select *
from transform




/*with customers as (
    select
        id as customer_id,
        first_name,
        last_name
    from raw.jaffle_shop.customers
),

orders as (
    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status
    from raw.jaffle_shop.orders
),

customer_orders as (

    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from orders
    group by 1
),

final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders
    from customers
    left join customer_orders using (customer_id)
)
select * from final */
    
