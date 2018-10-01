--drop materialized view AGROSTG.STG_DESPACHOS;
exec ETL_SCRIPTS.refresh_now('PROD','AGROSTG','STG_DESPACHOS','MV');
--alter materialized view AGROSTG.STG_DESPACHOS compile;
--purge recyclebin;

--150201----NOTE: 

select *
from AGROSTG.STG_DESPACHOS c
;

select *
from AGROSTG.STG_PRODUCCION c
--where contenedor != '-ND-'

where cajas != 0
and lower(tipo) like '%%'
;

select *
from AGROSTG.STG_MEDPROD c
where c.medcod = 'CAJ'
and capcod=127
;

CREATE MATERIALIZED VIEW AGROSTG.STG_DESPACHOS
NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED REFRESH COMPLETE ON DEMAND using trusted constraints
AS
select c.fecha,c.cc,c.tipo,c.contenedor, c.puerto, c.despachos cajas
from stg_produccion c
where c.contenedor != '-ND-'
order by c.fecha,c.cc,c.tipo,c.contenedor, c.puerto, c.despachos
;

select * from stg_medprod
where contenedor != '-ND-'
;

select *
--c.fecha,c.cc,c.tipo,c.contenedor,c.despachos cajas
from stg_produccion c
where c.contenedor != '-ND-'
--order by c.fecha,c.cc,c.tipo,c.contenedor,c.despachos
;