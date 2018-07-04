
select * 
from ONE_CLIENTE_VW c
;

--drop view AGROSTG.ONE_CLIENTE_VW;
CREATE or replace VIEW AGROSTG.ONE_CLIENTE_VW
AS
select distinct cliente
from one_loads_vw
;

--drop view AGROSTG.ONE_SEMANA_VW;
CREATE or replace VIEW  AGROSTG.ONE_SEMANA_VW
AS
select distinct SEMANA
from one_loads_vw
;


--drop view AGROSTG.ONE_CONTENEDOR_VW;
CREATE or replace VIEW AGROSTG.ONE_CONTENEDOR_VW
AS
select distinct contenedor
from one_loads_vw
union all
select '-ND-' from dual
;

exec ETL_SCRIPTS.refresh_now('AGROSTG.ONE_CLIENTE_MV','MV');
exec ETL_SCRIPTS.refresh_now('AGROSTG.ONE_SEMANA_MV','MV');
exec ETL_SCRIPTS.refresh_now('AGROSTG.ONE_CONTENEDOR_MV','MV');