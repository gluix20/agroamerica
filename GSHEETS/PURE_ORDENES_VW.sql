select * from pure_ordenes_vw
;

create or replace view pure_ordenes_vw
as 
SELECT
    o.ano,
    o.mes,
    o.cliente,
    o.codigo_oc,
    o.ref_externa,
    o.fecha_orden,
    o.tipo,
    o.producto,
    o.precio,
    o.total_oc,
    o.contrato,
    o.observacion,
    o.tm_contrato,
    o.jjoin,
    o.tm_orden,
    o.contrato_cod
FROM
    pure_ordenes o;

  
  select * from Pure_ORDENES;