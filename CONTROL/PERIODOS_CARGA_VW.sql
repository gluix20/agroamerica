select * from stg_periodos_carga_vw;

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
select 'STG_PLANILLA_GT' tipo, to_date('01/01/2017','dd/mm/yyyy') fecha, 117001 fecha_jde from dual
union all
select 'STG_PLANILLA_EC' tipo, to_date('01/01/2017','dd/mm/yyyy') fecha, 117001 fecha_jde from dual
union all
select 'STG_MANO_OBRA' tipo, to_date('01/01/2017','dd/mm/yyyy') fecha, 117001 fecha_jde from dual
union all
select 'CUB_PLANILLA_GT_MV' tipo, to_date('01/01/2018','dd/mm/yyyy') fecha, 118001 fecha_jde from dual
;



select * from stg_fecha where ano=2018;

