drop materialized view AGRODW.CUB_INVERSION_RSM_MV;
exec ETL_SCRIPTS.refresh_now('MATOG','AGRODW','CUB_INVERSION_RSM_MV','MV');

--161125----NOTE: Se crea.

select * from agrodw.CUB_INVERSION_RSM_MV c
join agrodw.dim_fecha_tab f on (c.dim_fecha=f.fecha)
join agrodw.dim_locacion_tab l on (c.dim_locacion=l.dimension_key)
;

create materialized view AGRODW.CUB_INVERSION_RSM_MV
NOCOMPRESS NOLOGGING TABLESPACE "DATAWAREHOUSE" BUILD DEFERRED REFRESH COMPLETE ON DEMAND using trusted constraints
AS
with consolidado as (
      select dim_locacion, dim_actividad, dim_fecha, tipo_oper, valor mat, 0 mdo
      from agrodw.cub_inversion_mv c
)

select dim_locacion, dim_actividad, max(dim_fecha) dim_fecha, 
sum(mat) mat, sum(mdo) mdo, sum(mat+mdo) tot
from consolidado c
left outer join agrodw.dim_fecha_tab f on (c.dim_fecha=f.fecha)
left outer join agrodw.dim_actividad_tab a on (c.dim_actividad=a.dimension_key)
where c.tipo_oper in ('INVERSION')
and a.macro not in ('INGRESOS','OTROS')
group by dim_locacion,dim_actividad,semana_cod
;


select l.region, sum(tot) from agrodw.cub_costos_rsm_mv c
join agrodw.dim_fecha_tab f on (c.dim_fecha=f.fecha)
join agrodw.dim_locacion_tab l on (c.dim_locacion=l.dimension_key)
where f.periodo_cod in (201611)
group by l.region
;

select * from agrodw.cub_budget_val_mv;
