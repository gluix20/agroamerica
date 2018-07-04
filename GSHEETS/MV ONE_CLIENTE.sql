--drop materialized view AGROSTG.ONE_CLIENTE_MV;
exec ETL_SCRIPTS.refresh_now('AGROSTG.ONE_CLIENTE_MV','MV');
--alter materialized view AGROSTG.ONE_CLIENTE_MV compile;
--purge recyclebin;


select * 
from ONE_CLIENTE_MV c
;
exec ETL_SCRIPTS.refresh_now('AGROSTG.ONE_CLIENTE_MV','MV');
--drop materialized view AGROSTG.ONE_CLIENTE_MV;
CREATE MATERIALIZED VIEW AGROSTG.ONE_CLIENTE_MV
NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED REFRESH COMPLETE ON DEMAND using trusted constraints
AS
select distinct cliente
from one_loads
;

----PROVISIONAL
exec ETL_SCRIPTS.refresh_now('AGROSTG.ONE_SEMANA_MV','MV');
--drop materialized view AGROSTG.ONE_SEMANA_MV;
CREATE MATERIALIZED VIEW AGROSTG.ONE_SEMANA_MV
NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED REFRESH COMPLETE ON DEMAND using trusted constraints
AS
select distinct SEMANA
from one_loads
;


exec ETL_SCRIPTS.refresh_now('AGROSTG.ONE_CONTENEDOR_MV','MV');
--drop materialized view AGROSTG.ONE_CONTENEDOR_MV;
CREATE MATERIALIZED VIEW AGROSTG.ONE_CONTENEDOR_MV
NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED REFRESH COMPLETE ON DEMAND using trusted constraints
AS
select distinct contenedor
from one_loads
union all
select '-ND-' from dual
;

exec ETL_SCRIPTS.refresh_now('AGROSTG.ONE_CLIENTE_MV','MV');
exec ETL_SCRIPTS.refresh_now('AGROSTG.ONE_SEMANA_MV','MV');
exec ETL_SCRIPTS.refresh_now('AGROSTG.ONE_CONTENEDOR_MV','MV');