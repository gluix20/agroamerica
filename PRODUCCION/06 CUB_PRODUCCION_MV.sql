drop materialized view AGRODW.CUB_PRODUCCION_MV;
exec ETL_SCRIPTS.refresh_now('PROD','AGRODW','CUB_PRODUCCION_MV','MV');
--alter materialized view AGRODW.CUB_PRODUCCION_MV compile;
--purge recyclebin;

--150825----TIME: 

select * from AGRODW.CUB_PRODUCCION_MV
order by 1,2
;

create materialized view AGRODW.CUB_PRODUCCION_MV
NOCOMPRESS NOLOGGING TABLESPACE "DATAWAREHOUSE" BUILD DEFERRED REFRESH COMPLETE ON DEMAND using trusted constraints
AS
SELECT stg.*,
  NVL(stg."FECHA", "DIM_FECHA_TAB"."FECHA") DIM_FECHA,
  l."DK" DIM_LOCACION
FROM
    agrostg.stg_prod_prr_vw stg
 left outer JOIN   agrodw."DIM_FECHA_TAB"  "DIM_FECHA_TAB" ON ( stg."FECHA" = "DIM_FECHA_TAB"."FECHA" )
 left outer JOIN   agrodw."DIM_LOCACION_TAB" l
 ON ( stg.cc = l.cc and stg.fecha between l.fecha_ini and l.fecha_fin )
 --Diferente de COV Carry Over
 where medcod != 'COV' 
 ;