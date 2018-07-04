select * from palm_sales_plan_vw;

create or replace view palm_sales_plan_vw
as
select c.FECHA, c.CLIENTE, c.PRODUCTO,
  c.TM
from palm_sales_plan c
;
  
  select * from palm_sales_plan;
 