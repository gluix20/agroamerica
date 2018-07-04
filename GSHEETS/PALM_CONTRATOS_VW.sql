select * from palm_contratos_vw;

create or replace view palm_contratos_vw
as
select c.ano, c.cliente, c.contrato_cod, 
c.tipo_contrato, c.producto, c.total_contrato, c.dias_credito, 
c.operacion, c.regla_precio, c.fecha_contrato
from palm_contratos c;
  
  select * from PALM_contratos;
 