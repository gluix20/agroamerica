select * from palm_contratos_det_vw;

create or replace view palm_contratos_det_vw
as
select c.contrato_cod, 
c.mes, c.fecha_ini, c.fecha_fin, c.tm, a.cliente, a.producto
from palm_contratos_det c
left outer join palm_contratos a on (c.contrato_cod=a.contrato_cod)
;
  
  select * from PALM_contratos_det;
 