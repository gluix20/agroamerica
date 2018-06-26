drop materialized view AGRODW.CUB_MDO_MV;
exec ETL_SCRIPTS.refresh_now('MDO','AGRODW','CUB_MDO_MV','MV');
--alter materialized view AGRODW.CUB_MDO_MV compile;
--purge recyclebin;

select * from AGRODW.CUB_MDO_MV
--where dim_labor is null
;

create materialized view AGRODW.CUB_MDO_MV
NOCOMPRESS NOLOGGING TABLESPACE "DATAWAREHOUSE" BUILD DEFERRED REFRESH COMPLETE ON DEMAND using trusted constraints
AS
SELECT stg.*,
  "DIM_ACTIVIDAD_TAB"."DK" DIM_ACTIVIDAD,
  NVL(stg."FECHA", "DIM_FECHA_TAB"."FECHA") DIM_FECHA,
  l."DK" DIM_LOCACION,
  b.dk dim_labor
FROM
    agrostg.stg_mdo_dns_vw  stg
 left outer JOIN   agrodw."DIM_ACTIVIDAD_TAB"  "DIM_ACTIVIDAD_TAB" ON ( stg."ACTIVIDAD_COD" = "DIM_ACTIVIDAD_TAB"."ACTIVIDAD_COD" )
 left outer JOIN   agrodw."DIM_FECHA_TAB"  "DIM_FECHA_TAB" ON ( stg."FECHA" = "DIM_FECHA_TAB"."FECHA" )
 left outer JOIN   agrodw."DIM_LOCACION_TAB" l
 ON ( stg.cc = l.cc and stg.fecha between l.fecha_ini and l.fecha_fin )
 left outer JOIN   agrodw.dim_labor_tab b
 ON ( stg.instancia = b.instancia
    and stg.nomec = b.nomec
    and stg.aplic = b.aplic
    and stg.clave = b.clave
    and stg.labor_join = b.labor_join)
 ;