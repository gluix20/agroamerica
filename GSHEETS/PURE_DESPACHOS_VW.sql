select * from pure_despachos_vw
;

create or replace view pure_despachos_vw
as

select c.ano,
    c.semana,
    c.procesadora,
    c.cliente,
    c.producto,
    c.fecha_despacho,
    c.tms,
    c.container,
    c.bl,
    c.discharge_port,
    c.shipping_line,
    c.arrival_date,
    c.tipo_doc,
    c.no_doc,
    c.fecha_doc,
    c.codigo_oc,
    c.precio,
    c.total_usd,
    c.fecha_pago,
    c.fecha_efectiva,
    c.abonos,
    c.concepto_nc,
    c.past_due,
    c.dias_credito,
  --decode(c.producto,'CPO',nvl(c.total_tm,0),0) tm_cpo_des, 
  --decode(c.producto,'CPKO',nvl(c.total_tm,0),0) tm_cpko_des,

  decode(c.tipo_doc, 'FC',
  case
  when trunc(sysdate) - trunc(c.fecha_doc) between 0 and 30 then '0 - 30'
  when trunc(sysdate) - trunc(c.fecha_doc) between 31 and 35 then '31 - 35'
  when trunc(sysdate) - trunc(c.fecha_doc) between 36 and 40 then '36 - 40'
  when trunc(sysdate) - trunc(c.fecha_doc) between 41 and 45 then '41 - 45'
  else '46 o más'
  end,null) rango,
  case when c.past_due <= 0 then 'BEFORE DUE'
  when c.past_due > 0 then 'PAST DUE'
  else '' end tipo_due,
  o.fecha_orden
  from PURE_DESPACHOS c
  left outer join pure_ordenes o on (c.codigo_oc=o.codigo_oc)
  ;
  
  select * from pure_despachos
  ;
  select * from pure_contratos;
  select * from pure_ordenes;
  