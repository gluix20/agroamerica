drop materialized view STG_CUENTAS_CONTABLES;
exec ETL_SCRIPTS.refresh_now('DIMS','AGROSTG','STG_CUENTAS_CONTABLES','MV');

select * from STG_CUENTAS_CONTABLES
;

 CREATE MATERIALIZED VIEW "AGROSTG"."STG_CUENTAS_CONTABLES"
 NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED USING INDEX REFRESH COMPLETE ON DEMAND
 AS
select * from obi_cuentas_contables_vw@agricultura
;

desc agrostg.stg_cuentas_contables;

select obj,descripcion from stg_cuentas_contables
where trim(sub) is null
group by obj,descripcion
order by 1,2
;