select 
row_number() over(order by p.id) as rn_id,
row_number() over(order by p.timestamp) as rn_ts,
from payment p
where not rn_id = rn_ts
order by (rn_id - rn_ts);
