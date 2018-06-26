set serveroutput on;
drop materialized view AGROSTG.STG_MANO_OBRA;
exec ETL_SCRIPTS.refresh_now('MDO','AGROSTG','STG_MANO_OBRA','MV');
--alter materialized view AGROSTG.stg_mano_obra compile;
--purge recyclebin;


select * 
from stg_mano_obra c
;

CREATE MATERIALIZED VIEW AGROSTG.STG_MANO_OBRA
NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED REFRESH COMPLETE ON DEMAND using trusted constraints
AS
with planilla_gt as (
select 
nomec, clave, aplic, fecha_finca, fecha, instancia, negocio, tc tasa_cambio,
actividad_cod, cc, tipo_oper, historico,
sum(jornales) jornales, sum(cantidad) cantidad, sum(hrs) hrs, sum(registros) registros, sum(frecuencia) frecuencia, 
sum(valor) valor, sum(bonoh) bonoh, sum(bonom) bonom, sum(ajuste) ajuste, 
sum(prestacion) prestacion, sum(vacaciones) vacaciones, sum(cuota_patronal) cuota_patronal
from stg_planilla_gt p
where p.valor_gtq+p.bonoh_gtq+p.bonom_gtq+p.ajuste_gtq != 0
and p.tipo_reg = 'COSTO'
group by 
nomec, clave, aplic, fecha_finca, fecha, instancia, negocio, tc,
actividad_cod, cc, tipo_oper, historico
)
SELECT
cast(a.negocio as varchar2(10)) negocio,
'PLA' tipo_costo, a.tipo_oper, a.historico,
'PLANILLA' descripcion,

nvl(valor + bonoh + bonom + ajuste + prestacion*3 + vacaciones + cuota_patronal, 0) mdo,
nvl( case when a.aplic in (1, 3)  then a.valor else 0 end, 0)  ordin,
nvl( case when a.aplic in (2, 4)  then a.valor else 0 end, 0)  extra,
nvl( case when a.aplic in (0)     then a.valor else 0 end, 0)  septi,
nvl( case when a.aplic in (9)     then a.valor else 0 end, 0)  feria,
nvl( case when a.aplic in (5, 6)  then a.valor else 0 end, 0)  asmin,
nvl( valor,0) valor,
nvl( bonom,0) bonom, 
nvl( bonoh,0) bonod,
nvl( ajuste,0) exced, 
nvl( prestacion *3 + vacaciones,0) prest,
nvl( cuota_patronal,0) patro,

nvl( case when a.aplic in (1, 3) then a.jornales else 0 end, 0) fuerza_jornal,
a.cantidad,a.hrs horas,a.frecuencia,a.registros,

a.tasa_cambio, a.clave, a.aplic, a.nomec, '-ND-' labor_join,
trunc(a.fecha) fecha, trunc(a.fecha_finca) fecha_finca,
a.cc, a.instancia, a.actividad_cod,
'-ND-' cuenta
  from planilla_gt a
  
  UNION ALL
  
  SELECT /*+ use_nl(a) NO_MERGE(a) PUSH_PRED(a) */
  a.negocio, tipo_costo, a.tipo_oper, 0 historico,
  a.descripcion, 
  valor mdo,
  0 ordin,0 extra, 0 septi,0 feria,0 asmin,
  0 valor, 0 bonom, 0 bonod, 0 exced,
  0 prest, 0 patro,
  0 fuerza_jornal,
  0 cantidad,0 horas,0 frecuencia,0 registros,
  
  tasa_cambio, 0 clave, 0 aplic, 0 nomec, '-ND-' labor_join,
  fecha, fecha fecha_finca,
    cc, 0 instancia, actividad_cod,
    a.cuenta
  from stg_costos_og a where tipo_costo='CDP'
  
  UNION ALL
  
  select
to_char('BANANO') negocio, 'PLA' tipo_costo,
case when l.inversion = 1 and ac.costo_locinv = 1 then 'INVERSION' else 'COSTO' end tipo_oper,
a.historico,
to_char('PLANILLA') descripcion,
monto_usd mdo,
ordinario_usd ordin,extraordinario_usd extra,0 septi,0 feria,0 asmin,
0 valor,
bonificaciones_usd bonom, --Se usa bonom para colocar las bonificaciones
0 bonod, 0 exced,
prestaciones_usd prest, 
seguro_social_usd patro,
0 fuerza_jornal,
cantidad,horas_trabajadas horas,0 frecuencia,0 registros,
1 tasa_cambio, 0 clave, to_number(a.tipo_pago) aplic, 0 nomec,  trim(a.labor) labor_join,
trunc(fecha) fecha, trunc(fecha_finca) fecha_finca,--Según chat con LEO, la semana finca inicia en domingo.
nvl(b.cc, '-ND-') cc, 5 instancia,
to_char(nvl(lpad(b.actividad_cod,10,' '),'-ND-')) actividad_cod,
a.numero_cuenta cuenta
from stg_planilla_ec a
left outer join stg_cuentas_contables b on (a.numero_cuenta=b.cuenta)
left outer join stg_locacion l on (nvl(b.cc, '-ND-') = l.cc and trunc(a.fecha) between l.fecha_ini and l.fecha_fin)
left outer join stg_actividad ac on (to_char(nvl(lpad(b.actividad_cod,10,' '),'-ND-')) = ac.actividad_cod)

;