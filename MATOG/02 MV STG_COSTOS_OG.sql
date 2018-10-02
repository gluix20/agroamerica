drop materialized view STG_COSTOS_OG;
exec ETL_SCRIPTS.refresh_now('MATOG','AGROSTG','STG_COSTOS_OG','MV');
--alter materialized view STG_COSTOS_OG compile;
--purge recyclebin;

select c.* from stg_costos_og c
left outer join stg_fecha f on (c.fecha=f.fecha)
left outer join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
left outer join stg_actividad a on (c.actividad_cod=a.actividad_cod)
where f.periodo_cod = 201806
--and tipo_costo = 'PLA'
;

create materialized view stg_costos_og
NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED REFRESH COMPLETE ON DEMAND using trusted constraints
AS
with fecha_carga as (
select fecha from stg_periodos_carga_vw
where tipo = 'STG_COSTOS_OG'
)
select /*+ PUSH_PRED(c) */
c.*
from obi_costos_og@agricultura c
where c.fecha >= (select fecha from fecha_carga)
;


select * from proddta.f0006@agricultura
where mcmcu in (select glmcu
from stg_costos_og
where tipo_oper='PRODUCCION'
and negocio= 'PALMA'
and valor != 0
group by glmcu)
order by 1
;

