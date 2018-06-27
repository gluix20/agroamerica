drop materialized view AGRODW.CUB_DESCUENTOS_GT_MV;
exec ETL_SCRIPTS.refresh_now('MDO','AGRODW','CUB_DESCUENTOS_GT_MV','MV');
--alter materialized view AGRODW.CUB_DESCUENTOS_GT_MV compile;
--purge recyclebin;

--150825----TIME: 

select count(*) from AGRODW.CUB_DESCUENTOS_GT_MV
--order by 1,2
;

create materialized view AGRODW.CUB_DESCUENTOS_GT_MV
NOCOMPRESS NOLOGGING TABLESPACE "DATAWAREHOUSE" BUILD DEFERRED REFRESH COMPLETE ON DEMAND using trusted constraints
AS
SELECT stg.*,
  "DIM_ACTIVIDAD_TAB"."DK" DIM_ACTIVIDAD,
  NVL(stg."FECHA"-1, df."FECHA") DIM_FECHA,
  l."DK" DIM_LOCACION,
  b.dk dim_labor,
  p.dk dim_persona
FROM
    agrostg.stg_planilla_gt/*_s2*/ stg
 left outer JOIN   agrodw."DIM_ACTIVIDAD_TAB"  "DIM_ACTIVIDAD_TAB" ON ( stg."ACTIVIDAD_COD" = "DIM_ACTIVIDAD_TAB"."ACTIVIDAD_COD" )
 left outer JOIN   agrodw."DIM_FECHA_TAB" df ON ( stg."FECHA"-1 = df."FECHA" )
 left outer JOIN   agrodw."DIM_LOCACION_TAB" l
 ON ( stg.cc = l.cc and stg.fecha-1 between l.fecha_ini and l.fecha_fin )
 left outer JOIN   agrodw.dim_labor_tab b
 ON ( stg.instancia = b.instancia
    and stg.nomec = b.nomec
    and stg.aplic = b.aplic
    and stg.clave = b.clave
    and '' = b.labor_join)
left outer join   agrodw.dim_persona_tab p
 ON ( stg.instancia = p.instancia
    and stg.cia = p.cia
    and stg.codigo = p.codigo)

where stg.tipo_reg= 'DESCUENTO'


 ;
 --and stg.tipo_carga = 'A'
 
 select * from agrostg.stg_planilla_gt stg;
 
 select * from agrodw.dim_fecha_tab
 where semana_cod=201601;