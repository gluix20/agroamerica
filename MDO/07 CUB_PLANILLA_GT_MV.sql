drop materialized view AGRODW.CUB_PLANILLA_GT_MV;
exec ETL_SCRIPTS.refresh_now('MDO','AGRODW','CUB_PLANILLA_GT_MV','MV');
--alter materialized view AGRODW.CUB_PLANILLA_GT_MV compile;
--purge recyclebin;

select min(fecha_finca)
from AGRODW.CUB_PLANILLA_GT_MV
--order by 1,2
;

CREATE MATERIALIZED VIEW AGRODW.CUB_PLANILLA_GT_MV
NOCOMPRESS NOLOGGING TABLESPACE "DATAWAREHOUSE" BUILD DEFERRED USING INDEX REFRESH COMPLETE ON DEMAND USING TRUSTED CONSTRAINTS DISABLE QUERY REWRITE
AS

with periodos_carga as (
select fecha from agrostg.stg_periodos_carga_vw
where tipo = 'CUB_PLANILLA_GT_MV'
)

select stg.*
, "DIM_ACTIVIDAD_TAB"."DK" DIM_ACTIVIDAD
, NVL(stg."FECHA", df."FECHA") DIM_FECHA
, l."DK" DIM_LOCACION
, b.dk dim_labor
, p.dk dim_persona
from
agrostg.stg_planilla_gt/*_s2*/ stg
left outer join   agrodw."DIM_ACTIVIDAD_TAB"  "DIM_ACTIVIDAD_TAB" ON ( stg."ACTIVIDAD_COD" = "DIM_ACTIVIDAD_TAB"."ACTIVIDAD_COD" )
left outer join   agrodw."DIM_FECHA_TAB" df ON ( stg."FECHA" = df."FECHA" )
left outer join   agrodw."DIM_LOCACION_TAB" l
on ( stg.cc = l.cc and stg.fecha between l.fecha_ini and l.fecha_fin )
left outer join   agrodw.dim_labor_tab b
on ( stg.instancia = b.instancia
    and stg.nomec = b.nomec
    and stg.aplic = b.aplic
    and stg.clave = b.clave
    and '-ND-' = b.labor_join) 
left outer join   agrodw.dim_persona_tab p
 on ( stg.instancia = p.instancia
    and stg.cia = p.cia
    and stg.codigo = p.codigo)
    
left outer join periodos_carga fc on (1=1)
where stg.fecha >= fc.fecha
and stg.tipo_reg= 'COSTO'
;


 
 select * from agrostg.stg_planilla_gt stg;
 
 select * from agrodw.dim_fecha_tab
 where semana_cod=201601;