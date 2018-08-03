drop MATERIALIZED VIEW "AGROSTG"."STG_OBIACTIVIDA";
exec ETL_SCRIPTS.refresh_now('DIMS','AGROSTG','STG_OBIACTIVIDA','MV');

select * from stg_obiactivida;

CREATE MATERIALIZED VIEW STG_OBIACTIVIDA
NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD IMMEDIATE USING INDEX  REFRESH COMPLETE ON DEMAND USING TRUSTED CONSTRAINTS
AS
select 
actrmd,
lpad(to_char(DRKY),10,' ') drky
from obieqactivida@agricultura
;