select count(*) from jaffle_shop.customers
union all
select count(*) from jaffle_shop.orders
union all
select count(*) from stripe.payments