set serveroutput on;
drop MATERIALIZED VIEW AGROSTG.STG_PLANILLA_EC;
exec ETL_SCRIPTS.refresh_now('MDO','AGROSTG','STG_PLANILLA_EC','MV');

select * from stg_planilla_ec
;

CREATE MATERIALIZED VIEW STG_PLANILLA_EC
NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED USING INDEX REFRESH COMPLETE ON DEMAND USING TRUSTED CONSTRAINTS
AS
with fecha_carga as (
select fecha from agrostg.stg_periodos_carga_vw
where tipo = 'STG_PLANILLA_EC'
)
, fecha_tasa as (
    select f.*, t.tasa_cambio tc
    from stg_fecha f
    left outer join stg_tasa_cambio t 
        on (f.fecha between t.fecha and t.fecha_lead)
    where moneda_origen = 'PEN' and moneda_destino = 'USD'
),
planilla as (
    select /*+ PUSH_PRED(c) */
    c.*, 'EC' pais, 'USD' moneda_origen
    from agro.planilla_agroamerica_ec@ecuador c
    where fecha_finca >= (select fecha from fecha_carga)
    union all
    select /*+ PUSH_PRED(c) */
    c.*, 'PE' pais, 'PEN' moneda_origen
    from agro.planilla_agroamerica_pe@ecuador c
    where fecha_finca >= (select fecha from fecha_carga)
)
--Segun chat con LEO, la semana de fincas EC empienza en DOMINGO igual que JDE.
--Por lo que hay que unirlo con FECHA y no con FECHA_FINCA en STG_FECHA
select /*+ ORDERED*/ c.*, 
decode(f.periodo_num,14,f.periodo_fecha_ini-1,f.fecha) fecha,
f.h_mdoec historico,
decode(pais,'PE', c.monto / f.tc, c.monto) monto_usd,
decode(pais,'PE', c.ordinario / f.tc, c.ordinario) ordinario_usd,
decode(pais,'PE', c.extraordinario / f.tc, c.extraordinario) extraordinario_usd,
decode(pais,'PE', c.bonificaciones / f.tc, c.bonificaciones) bonificaciones_usd,
decode(pais,'PE', c.seguro_social / f.tc, c.seguro_social) seguro_social_usd,
decode(pais,'PE', c.prestaciones / f.tc, c.prestaciones) prestaciones_usd
from planilla c
left outer join fecha_tasa f on (trunc(c.fecha_finca)=f.fecha)
;

select *
from stg_planilla_ec
where codigo_empresa=1 and codigo_locacion in (8,9)
;


select * from ecuador.planilla_agroamerica@panama;

select * from stg_planilla_gt;




