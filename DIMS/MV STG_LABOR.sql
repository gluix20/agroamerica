drop materialized view AGROSTG.STG_LABOR;
exec ETL_SCRIPTS.refresh_now('DIMS','AGROSTG','STG_LABOR','MV');
exec ETL_SCRIPTS.refresh_dim_labor_tab;
--alter materialized view AGROSTG.STG_LABOR compile;
purge recyclebin;

--170313----NOTE: Se agrega para poder hacer join con labores de conta directa.

select * from agrostg.stg_labor
where instancia = 4 and clave=274
;
select * from agrodw.dim_labor_tab;

create materialized view AGROSTG.STG_LABOR
NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED REFRESH COMPLETE ON DEMAND using trusted constraints
AS
select * from (

select instancia, nomec, clave, aplic, '-ND-' labor_join,
labor, unidad_medida, labor_cyd,
id_actividad, actividad_pla
from obi_labor_vw@frontera c

union all

select distinct 5 instancia, 0 nomec, 0 clave, to_number(c.tipo_pago) aplic, trim(labor) labor_join,
trim(labor) labor, cast('-ND-' as varchar2(4)) unidad_medida, trim(labor) labor_cyd,
0, cast('-ND-' as varchar2(4))
from stg_planilla_ec c

union all

select 0 instancia, 0 nomec, 0 clave, 0 aplic, '-ND-' labor_join,
'CONTA DIRECTA' labor, '-ND-' unidad_medida, 'CONTA DIRECTA' labor_cyd,
0 id_actividad, '-ND-' actividad_pla
from dual
)
;

select tipo_pago,labor,count(*)
from (
select tipo_pago,substr(labor,1,30) lab, labor
from stg_planilla_ec c
group by tipo_pago,substr(labor,1,30), labor

--having count(*)>1
)
group by tipo_pago,labor
having count(*)>1
;

select max(length(labor)) from stg_planilla_ec c;

select instancia,nomec,aplic,clave,labor_join,count(*) from agrodw.dim_labor_tab
where labor_join='-ND-'
group by instancia,nomec,aplic,clave,labor_join
--having count(*)>1
;