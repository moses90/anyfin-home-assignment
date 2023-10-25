--A. Can you provide the customer_id of all customers with max dpd > 10 days?

select distinct
	customer_id
from cycles
where dpd > 10;

--B. Can you provide the application_ids for the first application of each customer

select 
	customer_id,
	first_value(id) over (partition by customer_id order by created_at) as first_application_id
from applications
where customer_id is not null;

--C. Can you provide the customer_id of all customers that had more than 1 application within a time period of 30 days?

with applications_cte as (
	select 
		*,
		lead(created_at) over (partition by customer_id order by created_at) as customer_next_application_date 
	from applications
),
day_diff as (
	select
		*,
		date_part('day', (customer_next_application_date::timestamp - created_at::timestamp)) as days_between_applications
	from applications_cte
	where customer_next_application_date is not null
)
select distinct
	customer_id
from day_diff 
where days_between_applications < 30;


--D. Can you provide a list with customers_ids who had only open or overdue cycles? (hint: check unique values of cycles status column)

select 
	customer_id 
from cycles
where status in ('open', 'overdue')


--E. Can you provide an ordered list of customers over their percentage of overdue cycles per customer?

with cycles_cte as (
	select 
		* ,
		count(*) over (partition by customer_id) as number_of_cycles_per_customer,
		count(*) over (partition by customer_id, status) as number_of_cycles_per_customer_status
	from cycles
),
overdue_percentage as (
	select 
		* ,
		ceiling(100 * (number_of_cycles_per_customer_status::float / number_of_cycles_per_customer::float)) as overdue_percentage
	from cycles_cte 
	where status = 'overdue' 
)
select distinct
	customer_id,
	overdue_percentage
from overdue_percentage
order by overdue_percentage desc
