drop materialized view AGRODW.CUB_INVENTARIO_MV;
exec ETL_SCRIPTS.refresh_now('INVENTARIO','AGRODW','CUB_INVENTARIO_MV','MV');
--alter materialized view AGRODW.CUB_INVENTARIO_MV compile;
--purge recyclebin;

--150825----TIME: 

select * from AGRODW.CUB_INVENTARIO_MV
where tipo_documento='OV' and tipo_orden!='OP'
--order by 1,2
;

create materialized view AGRODW.CUB_INVENTARIO_MV
NOCOMPRESS NOLOGGING TABLESPACE "DATAWAREHOUSE" BUILD DEFERRED REFRESH COMPLETE ON DEMAND using trusted constraints
AS
SELECT stg.*,
  "DIM_MATERIAL_TAB"."DK" DIM_MATERIAL,
  NVL(stg."FECHA", "DIM_FECHA_TAB"."FECHA") DIM_FECHA,
  l."DK" DIM_LOCACION
FROM
    agrostg.stg_inventario_mv stg
 left outer JOIN   agrodw."DIM_MATERIAL_TAB"  "DIM_MATERIAL_TAB" ON ( stg."MATERIAL_JDE" = "DIM_MATERIAL_TAB"."MATERIAL_JDE" )
 left outer JOIN   agrodw."DIM_FECHA_TAB"  "DIM_FECHA_TAB" ON ( stg."FECHA" = "DIM_FECHA_TAB"."FECHA" )
 left outer JOIN   agrodw."DIM_LOCACION_TAB" l
 ON ( stg.cc = l.cc and stg.fecha between l.fecha_ini and l.fecha_fin )
 where stg.tipo_documento='OV'
 and stg.tipo_orden!='OP'
 and stg.cia not in ('00100') 
 /*Porque no quieren ver el cartón pues no ingresa realmente al inventario de BANASA*/
 
 ;
 
 -- and tipo_orden!='OP'
 
 desc stg_inventario_mv