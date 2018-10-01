drop materialized view STG_MEDPROD;
exec ETL_SCRIPTS.refresh_now('PROD','AGROSTG','STG_MEDPROD','MV');

--150407----TIME: Refresh 3 segs?
--150408----TIME: Refresh 23 segs

select * from stg_medprod
where actcod in (600)
order by medcod, fecha desc
;

CREATE MATERIALIZED VIEW STG_MEDPROD
NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED USING INDEX  REFRESH COMPLETE ON DEMAND USING TRUSTED CONSTRAINTS
DISABLE QUERY REWRITE
AS
select m.*
from obi_medprod@agricultura m
;

