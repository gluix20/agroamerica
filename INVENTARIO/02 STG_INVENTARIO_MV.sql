--drop materialized view stg_inventario_mv;
exec ETL_SCRIPTS.refresh_now('INVENTARIO','AGROSTG','STG_INVENTARIO_MV','MV');
--alter materialized view stg_inventario_mv compile;
--purge recyclebin;

--160517----NOTE: Se crea.

select * from stg_inventario_mv;

select distinct proveedor from stg_inventario_mv c
--left outer join stg_material m on (c.material_jde = m.material_jde)
left outer join stg_fecha f on (c.fecha=f.fecha)
where cia in ('00100')
and tipo_documento='OV'
 and tipo_orden!='OP'
 and f.ano in (2016,2017,2015)
;

create materialized view stg_inventario_mv
NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED REFRESH COMPLETE ON DEMAND using trusted constraints
AS
with fecha_carga as (
select fecha from stg_periodos_carga_vw
where tipo = 'STG_INVENTARIO_MV'
)
select /*+ PUSH_PRED(c) */ c.* 
from obi_inventario_vw@agricultura c
where c.fecha >= (select fecha from fecha_carga)
;
