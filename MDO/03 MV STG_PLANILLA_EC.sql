set serveroutput on;
drop MATERIALIZED VIEW AGROSTG.STG_PLANILLA_EC;
exec ETL_SCRIPTS.refresh_now('MDO','AGROSTG','STG_PLANILLA_EC','MV');

--150505----NOTE: Se traen datos a partir de 2015
               -- La vista origen si tiene group by.
--150506----TIME: Refresh 15 segs.
--150518----TIME: Refresh 36 segs.

select * from stg_planilla_ec
where fecha<'02/01/2017'
order by fecha desc

;

CREATE MATERIALIZED VIEW STG_PLANILLA_EC
NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED USING INDEX REFRESH COMPLETE ON DEMAND USING TRUSTED CONSTRAINTS
AS
with fecha_tasa as (
    select f.*, t.tasa_cambio tc
    from stg_fecha f
    left outer join stg_tasa_cambio t 
        on (f.fecha between t.fecha and t.fecha_lead)
    where moneda_origen = 'PEN' and moneda_destino = 'USD'
),

planilla as (
    select c.*, 'EC' pais, 'USD' moneda_origen
    from agro.planilla_agroamerica_ec@ecuador c
    union all
    select c.*, 'PE' pais, 'PEN' moneda_origen
    from agro.planilla_agroamerica_pe@ecuador c
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




