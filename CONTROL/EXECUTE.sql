set serveroutput on;
purge recyclebin;

exec ETL_SCRIPTS.REFRESH_ALL;

/*PARA RESETEAR LAS DIMS_KEYS*/
exec ETL_SCRIPTS.refresh_dims;
     exec ETL_SCRIPTS.refresh_budget('C');
     exec ETL_SCRIPTS.refresh_prod('C');
     exec ETL_SCRIPTS.refresh_matog('F');
     exec ETL_SCRIPTS.refresh_mdo('C');



exec ETL_SCRIPTS.status_constraints('ENABLED', 'DISABLE');
status_constraints('DISABLED', 'ENABLE');

exec ETL_SCRIPTS.REFRESH_dims;
exec ETL_SCRIPTS.refresh_now('AGROSTG.STG_FECHA','MV');
exec ETL_SCRIPTS.refresh_dim_fecha_tab;
exec ETL_SCRIPTS.refresh_now('DIMS','AGROSTG','STG_LOCACION','MV');
exec ETL_SCRIPTS.refresh_dim_locacion_tab;
exec ETL_SCRIPTS.refresh_now('DIMS','AGROSTG','STG_ACTIVIDAD','MV');
exec ETL_SCRIPTS.refresh_dim_actividad_tab;
exec ETL_SCRIPTS.refresh_now('DIMS','AGROSTG','STG_LABOR','MV');
exec ETL_SCRIPTS.refresh_dim_labor_tab;
exec ETL_SCRIPTS.refresh_now('DIMS','AGROSTG','STG_PERSONA','MV');
exec ETL_SCRIPTS.refresh_dim_persona_tab;
exec ETL_SCRIPTS.refresh_now('DIMS','AGROSTG','STG_CUENTAS_CONTABLES','MV');
exec ETL_SCRIPTS.refresh_dim_cuenta_tab;
select * from agrodw.dim_cuenta_tab;

exec ETL_SCRIPTS.refresh_now('DIMS','AGROSTG','STG_MATERIAL','MV');
exec ETL_SCRIPTS.refresh_dim_material_tab;


select * from agrodw.dim_fecha_tab order by fecha desc;
select * from stg_locacion where region_cod=1 and distrito_cod=5
order by cc;
select * from agrodw.dim_locacion_tab where region_cod=1 and distrito_cod=5
order by cc;
select * from agrostg.stg_labor;
select * from agrodw.dim_labor_tab;
select * from agrostg.stg_persona;
select * from agrodw.dim_persona_tab;
select * from agrodw.dim_material_tab;
select * from agrostg.stg_actividad;
select * from agrodw.dim_actividad_tab;

--REFRESH_PROD
  exec ETL_SCRIPTS.REFRESH_prod('C');
  exec ETL_SCRIPTS.REFRESH_prod('F');
  exec ETL_SCRIPTS.refresh_now('AGROSTG.STG_MEDPROD','MV');
  exec ETL_SCRIPTS.refresh_now('AGROSTG.STG_HAS','MV');
  alter materialized view AGROSTG.STG_LABOR compile;
  exec ETL_SCRIPTS.refresh_now('AGROSTG.STG_PRODUCCION','MV');
  exec ETL_SCRIPTS.refresh_now('AGRODW.CUB_PRODUCCION_MV','MV');
  exec ETL_SCRIPTS.refresh_now('AGRODW.CUB_HAS_MV','MV');
  exec ETL_SCRIPTS.refresh_now('AGRODW.CUB_HAS_SEM_MV','MV');
  exec ETL_SCRIPTS.refresh_now('AGRODW.CUB_HAS_PER_MV','MV');
  exec ETL_SCRIPTS.refresh_now('AGRODW.CUB_HAS_ANO_MV','MV');
--REFRESH_PROD
--REFRESH_BUDGET
exec ETL_SCRIPTS.refresh_budget('C');
exec ETL_SCRIPTS.refresh_budget('F');
exec ETL_SCRIPTS.refresh_now('AGROSTG.STG_BUDGET','MV');
exec ETL_SCRIPTS.refresh_now('AGROSTG.STG_BUD_HAS','MV');
exec ETL_SCRIPTS.refresh_now('AGRODW.CUB_BUDGET_VAL_MV','MV');
exec ETL_SCRIPTS.refresh_now('AGRODW.CUB_BUDGET_UNI_MV','MV');
exec ETL_SCRIPTS.refresh_now('AGRODW.CUB_BUD_HAS_SEM_MV','MV');
exec ETL_SCRIPTS.refresh_now('AGRODW.CUB_BUD_HAS_PER_MV','MV');
exec ETL_SCRIPTS.refresh_now('AGRODW.CUB_BUD_HAS_ANO_MV','MV');
--REFRESH_BUDGET
--REFRESH_MATOG
exec ETL_SCRIPTS.refresh_matog('C');
exec ETL_SCRIPTS.refresh_matog('F');
exec ETL_SCRIPTS.refresh_now('MATOG','AGROSTG','STG_COSTOS_OG','MV');
exec ETL_SCRIPTS.refresh_now('MATOG','AGRODW','CUB_MATOG_MV','MV');
--exec ETL_SCRIPTS.refresh_now('AGRODW.CUB_INVERSION_MV','MV');
exec ETL_SCRIPTS.refresh_now('MATOG','AGRODW','CUB_COSTOS_RSM_MV','MV');
--REFRESH_MATOG

--REFRESH_MDO
exec ETL_SCRIPTS.refresh_mdo('C');
exec ETL_SCRIPTS.refresh_mdo('F');
exec ETL_SCRIPTS.refresh_now('MDO','AGROSTG','STG_TASA_CAMBIO','MV');
exec ETL_SCRIPTS.refresh_now('MDO','AGROSTG','STG_CONF_CUENTAS_PLANILLA','MV');
exec ETL_SCRIPTS.refresh_now('MDO','AGROSTG','STG_PLANILLA_GT','MV');
exec ETL_SCRIPTS.refresh_now('MDO','AGROSTG','STG_PLANILLA_EC','MV');
exec ETL_SCRIPTS.refresh_now('MDO','AGROSTG','STG_MANO_OBRA','MV');
select count(*) from stg_mano_obra;
select count(*) from stg_mdo_prr;
select count(*) from stg_mdo_dns_vw;
exec ETL_SCRIPTS.refresh_now('MDO','AGROSTG','STG_MDO_PRR','MV');
--exec ETL_SCRIPTS.recreate_mv('AGROSTG','STG_MDO_PRR');
exec ETL_SCRIPTS.refresh_now('MDO','AGRODW','CUB_PLANILLA_GT_MV','MV');
--exec ETL_SCRIPTS.recreate_mv('AGRODW','CUB_MDO_MV');
exec ETL_SCRIPTS.refresh_now('MDO','AGRODW','CUB_MDO_MV','MV');
exec ETL_SCRIPTS.refresh_now('MDO','AGRODW','CUB_COSTOS_RSM_MV','MV');
exec ETL_SCRIPTS.refresh_now('MDO','AGRODW','CUB_DESCUENTOS_GT_MV','MV');
alter materialized view AGRODW.CUB_MDO_MV compile;
--REFRESH_MDO
--select * from agrostg.stg_mano_obra;

select c.*
from ctrl_SUCCESS c
where objeto like '%STG_FEC%'
order by 5 desc;

select * from ctrl_failure order by fecha_ts desc;
--truncate table ctrl_success;
--truncate table ctrl_failure;
exec DBMS_SESSION.RESET_PACKAGE;

EXEC DBMS_STATS.gather_schema_stats('AGRODW', estimate_percent => 100, cascade => TRUE);
EXEC DBMS_STATS.gather_schema_stats('AGROSTG', estimate_percent => 100, cascade => TRUE);

select to_char(sysdate,'dd/mm/yyyy hh:mi:ss') from dual;