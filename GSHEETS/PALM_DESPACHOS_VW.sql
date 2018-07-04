select * from palm_despachos_vw
where cliente = 'OLEOSUR'
and rango = '46 o más'
and fecha_efectiva is null
;

create or replace view palm_despachos_vw
as

select c.ano,
  c.semana,
  c.extractora, c.cliente, c.producto, 
  c.fecha_despacho,
  c.placas_t1,
  TM_T1 ,
  ACIDEZ_T1 ,
  HUMEDAD_T1 ,
  PLACAS_T2 ,
  TM_T2 ,
  ACIDEZ_T2 ,
  HUMEDAD_T2 ,
  TOTAL_TM tm_despachos,
  decode(c.producto,'CPO',nvl(c.total_tm,0),0) tm_cpo_des, 
  decode(c.producto,'CPKO',nvl(c.total_tm,0),0) tm_cpko_des,
  TIPO_DOC ,
  NO_DOC , 
  FECHA_DOC , 
  decode(tipo_doc, 'FC',
  case
  when trunc(sysdate) - trunc(fecha_doc) between 0 and 30 then '0 - 30'
  when trunc(sysdate) - trunc(fecha_doc) between 31 and 35 then '31 - 35'
  when trunc(sysdate) - trunc(fecha_doc) between 36 and 40 then '36 - 40'
  when trunc(sysdate) - trunc(fecha_doc) between 41 and 45 then '41 - 45'
  else '46 o más'
  end,null) rango,
  c.mes_oc, c.oc, c.precio,
  TOTAL_USD ,
  FECHA_PAGO ,
  FECHA_EFECTIVA ,
  CONCEPTO_NC ,
  PAST_DUE,
  case when past_due <= 0 then 'BEFORE DUE'
  when past_due > 0 then 'PAST DUE'
  else '' end tipo_due,
  
  o.fecha_orden,
  co.contrato_cod,
  fe.mes mes_despacho,
  fe.periodo perdiodo_despacho,
  fe.periodo_cod perdiodo_cod_despacho 
  from PALM_DESPACHOS c
  left outer join palm_ordenes o on (c.oc=o.codigo)
  left outer join palm_contratos co on (o.contrato_cod=co.contrato_cod)
  left outer join stg_fecha fe on (c.fecha_despacho = fe.fecha);
  
  select * from palm_despachos
  where oc='OS1603-04'
  ;
  select * from palm_contratos;
  select * from palm_ordenes;
  