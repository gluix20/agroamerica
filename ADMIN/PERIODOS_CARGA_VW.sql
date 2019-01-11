select * from stg_periodos_carga_vw;

select * from fro_obi_planilla
order by fecha_finca asc;

select * from fro_obi_planilla_h
order by fecha_finca desc;

create or replace view stg_periodos_carga_vw as 
with periodos as (
select periodo_cod, min(fecha) fecha, min(fecha_jde) fecha_jde 
from agrostg.stg_fecha
where fecha <= trunc(sysdate)
and periodo_num != 14
group by periodo_cod
order by 1 desc
)
select 'FRO_OBI_PLANILLA' tipo, min(fecha) fecha, min(fecha_jde) fecha_jde from periodos
where rownum <= 4
union all
select tipo, fecha, fecha_jde from tab_periodos_carga
;

select 'GENERICO-'||ano, min(fecha) fecha, min(fecha_jde) fecha_jde 
from agrostg.stg_fecha
group by ano
order by 1 desc
