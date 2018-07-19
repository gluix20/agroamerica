drop materialized view AGRODW.CUB_COSTOS_PRR_SEMANAL_MV;
exec ETL_SCRIPTS.refresh_now('MATOG','AGRODW','CUB_COSTOS_PRR_SEMANAL_MV','MV');

--161125----NOTE: Se crea.

select * from agrodw.CUB_COSTOS_PRR_SEMANAL_MV c
join agrodw.dim_fecha_tab f on (c.dim_fecha=f.fecha)
join agrodw.dim_locacion_tab l on (c.dim_locacion=l.dimension_key)
join agrodw.dim_actividad_tab a on (c.dim_actividad = a.dimension_key)
;

select * from agrodw.dim_actividad_tab a;

create materialized view AGRODW.CUB_COSTOS_PRR_SEMANAL_MV
NOCOMPRESS NOLOGGING TABLESPACE "DATAWAREHOUSE" BUILD DEFERRED REFRESH COMPLETE ON DEMAND using trusted constraints
AS

with periodos as
(
select distinct periodo_cod from agrodw.dim_fecha_tab f
where periodo_num != 14
), periodos_lead as (
select periodo_cod, lead(periodo_cod,1) over (order by periodo_cod) periodo_cod_lead
from periodos p
), semanas_lead as (
select pl.periodo_cod, pl.periodo_cod_lead, f.semana_cod semana_cod_lead, max(f.fecha) fecha_lead
from periodos_lead pl
join agrodw.dim_fecha_tab f on (pl.periodo_cod_lead=f.periodo_cod)
group by pl.periodo_cod, pl.periodo_cod_lead, f.semana_cod
order by semana_cod desc
)

select dim_locacion, dim_actividad, dim_fecha, mat, mdo, tot
from agrodw.cub_costos_rsm_mv c
left outer join agrodw.dim_actividad_tab a on (c.dim_actividad = a.dimension_key)
where a.rep_sem != 1

union all

select c.dim_locacion, c.dim_actividad, sl.fecha_lead dim_fecha,-- sl.periodo_cod, sl.periodo_cod_lead, sl.semana_cod_lead,
c.mat, c.mdo, c.tot
from (
select c.dim_locacion, c.dim_actividad, f.periodo_cod,
sum(c.mat)/4 mat, sum(c.mdo)/4 mdo, sum(c.tot)/4 tot
from agrodw.cub_costos_rsm_mv c
left outer join agrodw.dim_fecha_tab f on (c.dim_fecha=f.fecha)
left outer join agrodw.dim_actividad_tab a on (c.dim_actividad = a.dimension_key)
where a.rep_sem = 1
group by c.dim_locacion, c.dim_actividad, f.periodo_cod
) c
left outer join semanas_lead sl on (c.periodo_cod=sl.periodo_cod)
order by dim_fecha desc
;
