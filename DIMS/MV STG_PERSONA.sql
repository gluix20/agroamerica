drop materialized view AGROSTG.STG_PERSONA;
exec ETL_SCRIPTS.refresh_now('DIMS','AGROSTG','STG_PERSONA','MV');
--alter materialized view AGROSTG.STG_PERSONA compile;
--purge recyclebin;

--161109----NOTE: Se crea.

select count(*) from agrostg.STG_PERSONA
--order by labor_cyd
;

create materialized view AGROSTG.STG_PERSONA
NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED REFRESH COMPLETE ON DEMAND using trusted constraints
AS
select * from obi_persona_vw@frontera
;