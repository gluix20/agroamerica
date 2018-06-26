drop MATERIALIZED VIEW AGROSTG.FRO_OBI_PLANILLA;
exec ETL_SCRIPTS.refresh_now('MDO','AGROSTG','FRO_OBI_PLANILLA','MV');

select count(*)
from FRO_OBI_PLANILLA
;

CREATE MATERIALIZED VIEW FRO_OBI_PLANILLA
NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED USING INDEX REFRESH COMPLETE ON DEMAND USING TRUSTED CONSTRAINTS DISABLE QUERY REWRITE
AS
with fecha_carga as (
select fecha from stg_periodos_carga_vw
where tipo = 'FRO_OBI_PLANILLA'
)
select /*+ PUSH_PRED(c) */
c.*
from obi_planilla_vw@frontera c
join fecha_carga fc on (1=1)
where c.fecha_finca >= fc.fecha
;
