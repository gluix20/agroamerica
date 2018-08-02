CREATE OR REPLACE FORCE VIEW "FARMING_PLUS"."VW_CUADRILLA_PESAJE_1"
AS 
  select lpad(trim(p.cuadrilla), 4, '0') cuadrilla
  , round(sum(d.cant_racimos),0) cant_racimos
  , round(sum(d.peso_fruto),2) peso_fruto_total
  , round(sum(d.peso_fruto) / sum(d.cant_racimos),2) peso_fruto_avg
  , round(sum(d.peso_fruto/f.peso_x_caja),0) cajas_pot
  , trunc(d.fecha_hora) fecha
  , d.finca_id
  from pesajes_det d
  join pesajes p on (p.pesaje_id = d.pesaje_id) 
  join fincas f on (p.finca_id = f.finca_id)
  join cables c on c.cable_id = p.cable_id
  join cintas ci on d.cinta_id = ci.cinta_id 
  where d.estado is null   
  group by lpad(trim(p.cuadrilla), 4, '0')
  , trunc(d.fecha_hora)
  , d.finca_id
  order by 1
;