create table payments_2020 (
customer_id int,
plan_id int,
plan_name varchar(13),
payment_date date,
amount decimal (5,2),
payment_order int
)



;with cte as (
select 
customer_id,
a.plan_id,
plan_name,
start_date as payment_date,
start_date,
lead(start_date, 1) over(partition by customer_id order by start_date, a.plan_id) as next_date,
price as amount
from subscriptions a left join plans b
on a.plan_id = b.plan_id
),
cte_2 as (
select
customer_id,
plan_id,
plan_name,
payment_date,
start_date,
case
	when next_date is null or year(next_date) > '2020' then '2020' 
	else next_date
end as next_date,
amount
from cte
where plan_id not in (0, 4)
),
cte_3 as (
select
customer_id,
plan_id,
plan_name,
payment_date,
start_date,
next_date,
dateadd(month, -1, next_date) as next_date_2,
amount
from cte_2
),
recursive as (
select 
customer_id,
plan_id,
plan_name,
start_date,
payment_date = (select top 1 start_date from cte_3 where customer_id = a.customer_id and plan_id = a.plan_id),
next_date,
next_date_2,
amount
from cte_3 a

union all

select
customer_id,
plan_id,
plan_name,
start_date,
dateadd(month, - 1, payment_date) as payment_date,
next_date,
next_date_2,
amount
from recursive date_cte
where payment_date > next_date_2 and plan_id != 3
)
insert into payments_2020 (customer_id, plan_id, plan_name, payment_date, amount, payment_order)
select 
customer_id,
plan_id,
plan_name,
payment_date,
amount,
rank() over(partition by customer_id order by customer_id, plan_id, payment_date) as payment_order
from recursive date_cte
where year(payment_date) = '2020'
order by
customer_id,
plan_id,
payment_date

select *
from payments_2020