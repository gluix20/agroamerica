select * from palm_ordenes_vw
where cliente='OLEOFINOS'
;

create or replace view palm_ordenes_vw
as
select 
ANO,
	MES, 
	CLIENTE,
	CODIGO,
  TIPO_CONTRATO,
  PRECIO,
  TOTAL_OC,
  o.contrato_cod,
  o.fecha_orden,
  o.producto
  from PALM_ORDENES o;
  
  select * from PALM_ORDENES;