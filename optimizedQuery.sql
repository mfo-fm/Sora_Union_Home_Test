CREATE INDEX idx_clickup_name ON sora_union_data_warehouse.clickup_table(Name, hours);
CREATE INDEX idx_float_name ON sora_union_data_warehouse.float_table(Name, Estimated_Hours);

with Float as (
select name,role,sum (estimated_hours) Total_Allocated_Hours
from sora_union_data_warehouse.float_table
group by name,role),
ClickUp as (
select name,sum (hours) Total_Tracked_Hours
from sora_union_data_warehouse.clickup_table
group by name)
select  c.name,f.role,c.Total_Tracked_Hours,f.Total_Allocated_Hours
from ClickUp c 
join Float f
on c.name = f.name
where Total_Tracked_Hours > 100
order by Total_Allocated_Hours desc;


