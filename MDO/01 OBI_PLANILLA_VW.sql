select * from obi_planilla_vw;
select * from trabajos;

CREATE OR REPLACE FORCE VIEW OBI_PLANILLA_VW
AS
with desde_ano as (
select 2015 ano from dual
),

trab as
(
select /*+ USE_NL(t) NO_MERGE(t) PUSH_PRED(t) */
1 instancia, t.cia, t.planilla, t.locno, t.nomec, t.clave, t.aplic, trunc(t.fecha) fecha, 
t.codigo, t.ano, t.valor, t.ajuste, t.bonom, t.bonoh, t.hrs, t.cantidad 
from trabajos t
where t.ano >= (select ano from desde_ano)
union all
select /*+ USE_NL(t) NO_MERGE(t) PUSH_PRED(t) */
2 instancia, t.cia, t.planilla, t.locno, t.nomec, t.clave, t.aplic, trunc(t.fecha) fecha, 
t.codigo, t.ano, t.valor, t.ajuste, t.bonom, t.bonoh, t.hrs, t.cantidad 
from trabajos@sierra t
where t.ano >= (select ano from desde_ano)
union all
select /*+ USE_NL(t) NO_MERGE(t) PUSH_PRED(t) */
3 instancia, t.cia, t.planilla, t.locno, t.nomec, t.clave, t.aplic, trunc(t.fecha) fecha, 
t.codigo, t.ano, t.valor, t.ajuste, t.bonom, t.bonoh, t.hrs, t.cantidad 
from trabajos@vegas t
where t.ano >= (select ano from desde_ano)
union all
select /*+ USE_NL(t) NO_MERGE(t) PUSH_PRED(t) */
4 instancia, t.cia, t.planilla, t.locno, t.nomec, t.clave, t.aplic, trunc(t.fecha) fecha, 
t.codigo, t.ano, t.valor, t.ajuste, t.bonom, t.bonoh, t.hrs, t.cantidad 
from trabajos@pana t
where t.ano >= (select ano from desde_ano)
)

select t.instancia,
t.cia, t.planilla, t.locno,
t.nomec, t.clave, t.aplic,
case when t.aplic < 10 then 'COSTO' else 'DESCUENTO' end tipo_reg,
t.fecha fecha_finca,
t.codigo,
case 
  when t.ano >= 2016 then t.valor + t.ajuste + t.bonom
  when t.fecha >= to_date('21/09/2014','dd/mm/yyyy') or 
  (t.cia not in (2,3) and t.fecha < to_date('21/09/2014','dd/mm/yyyy')) then t.valor
  else 0 
end valor_prest_gtq,
case 
  when t.aplic in (1,3) then t.hrs/8 else 0 end jornales,--Solo para piezas y horas ordinarias
  t.valor valor_gtq, t.bonoh bonoh_gtq, t.bonom bonom_gtq, t.ajuste ajuste_gtq, 
  t.cantidad, t.hrs,
  1 registros, 
case when t.aplic in (1,2,3,4) then 1 else 0 end frecuencia
from trab t
;