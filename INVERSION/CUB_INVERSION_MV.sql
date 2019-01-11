drop materialized view AGRODW.CUB_INVERSION_MV;
exec ETL_SCRIPTS.refresh_now('MATOG','AGRODW','CUB_INVERSION_MV','MV');
--alter materialized view AGRODW.CUB_INVERSION_MV compile;
--purge recyclebin;

--150825----TIME: 

select * from AGRODW.CUB_INVERSION_MV
order by 1,2
;

create materialized view AGRODW.CUB_INVERSION_MV
NOCOMPRESS NOLOGGING TABLESPACE "DATAWAREHOUSE" BUILD DEFERRED REFRESH COMPLETE ON DEMAND using trusted constraints
AS
SELECT stg.*,
  "DIM_ACTIVIDAD_TAB"."DK" DIM_ACTIVIDAD,
  "DIM_MATERIAL_TAB"."DK" DIM_MATERIAL,
  NVL(stg."FECHA", "DIM_FECHA_TAB"."FECHA") DIM_FECHA,
  l."DK" DIM_LOCACION,
  cu.dk DIM_CUENTA
FROM
    agrostg.stg_matog_prr_vw stg
 left outer JOIN   agrodw."DIM_ACTIVIDAD_TAB"  "DIM_ACTIVIDAD_TAB" ON ( stg."ACTIVIDAD_COD" = "DIM_ACTIVIDAD_TAB"."ACTIVIDAD_COD" )
 left outer JOIN   agrodw."DIM_MATERIAL_TAB"  "DIM_MATERIAL_TAB" ON ( stg."MATERIAL_JDE" = "DIM_MATERIAL_TAB"."MATERIAL_JDE" )
 left outer JOIN   agrodw."DIM_FECHA_TAB"  "DIM_FECHA_TAB" ON ( stg."FECHA" = "DIM_FECHA_TAB"."FECHA" )
 left outer JOIN   agrodw."DIM_LOCACION_TAB" l
 ON ( stg.cc = l.cc and stg.fecha between l.fecha_ini and l.fecha_fin )
 left outer JOIN   agrodw."DIM_CUENTA_TAB" cu ON ( stg.cuenta = cu.cuenta )
 where tipo_oper = 'INVERSION'
 ;