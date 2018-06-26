set serveroutput on;
ALTER SESSION SET SQL_TRACE = TRUE;
drop MATERIALIZED VIEW AGROSTG.STG_MDO_PRR;
alter materialized view AGROSTG.STG_MDO_PRR compile;

exec ETL_SCRIPTS.refresh_now('MDO','AGROSTG','STG_MDO_PRR','MV');


select * from agrostg.stg_mdo_prr c 
left outer join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
where l.distrito = 'COSTOS FIJOS'
;

CREATE MATERIALIZED VIEW AGROSTG.STG_MDO_PRR
NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED USING INDEX REFRESH COMPLETE ON DEMAND USING TRUSTED CONSTRAINTS DISABLE QUERY REWRITE
AS 
select * from (

select c.tipo_costo, 0 prorrateo,
c.negocio, c.descripcion,  
c.tasa_cambio, c.clave, c.aplic, c.nomec, 
c.labor_join, c.fecha, c.fecha_finca,
c.cc, c.instancia, c.actividad_cod, 
c.cuenta, c.tipo_oper, c.historico,
c.mdo, c.ordin, c.extra, c.septi, c.feria, c.asmin, 
c.valor, c.bonom, c.bonod, c.exced, c.prest, c.patro, 
c.fuerza_jornal, 
c.cantidad, c.horas, c.frecuencia, c.registros
from stg_mano_obra c
join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
join agrostg.stg_actividad a on (c.actividad_cod=a.actividad_cod)
where 
case 
when l.distrito in ('COSTOS FIJOS','PLANTA DE BENEFICIO') and a.macro not in ('OVERHEAD') 
then 0
--when c.macro in ('OVERHEAD') then 0
else 1 end = 1

    UNION ALL
    
    select * from (
select 
a.tipo_costo, 1 prorrateo,
a.negocio, a.descripcion,  
a.tasa_cambio, a.clave, a.aplic, a.nomec,
a.labor_join, a.fecha, a.fecha_finca,
b.cc, a.instancia, a.actividad_cod,
a.cuenta, a.tipo_oper, a.historico,
round(a.mdo*b.factor,2), round(a.ordin*b.factor,2), round(a.extra*b.factor,2), round(a.septi*b.factor,2), 
round(a.feria*b.factor,2), round(a.asmin*b.factor,2), round(a.valor*b.factor,2), round(a.bonom*b.factor,2), 
round(a.bonod*b.factor,2), round(a.exced*b.factor,2), round(a.prest*b.factor,2), round(a.patro*b.factor,2), 
round(a.fuerza_jornal*b.factor,2), 
round(a.cantidad*b.factor,2), round(a.horas*b.factor,2), round(a.frecuencia*b.factor,2), round(a.registros*b.factor,2)
from (
--Q1 Valor a detalle de materiales, agregando region y periodo
select l.region_cod, f.ano, f.semana_cod, c.*
from agrostg.stg_mano_obra c
join agrostg.stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
join agrostg.stg_fecha f on (c.fecha=f.fecha)
join agrostg.stg_actividad a on (c.actividad_cod=a.actividad_cod)
where l.distrito in ('COSTOS FIJOS','PLANTA DE BENEFICIO') and a.macro not in ('OVERHEAD')
) a
join stg_prorrateo_fac_vw b on (a.region_cod=b.region_cod and a.ano=b.ano and a.semana_cod=b.semana_cod)
)
) u
;


