select 
	customer_key
	, count(*)  as duplicate_count
from [Gold].[dim_customers]
group by customer_key
HAVING count(*) > 1;
