drop MATERIALIZED VIEW AGROSTG.STG_PLANILLA_GT;
exec ETL_SCRIPTS.refresh_now('MDO','AGROSTG','STG_PLANILLA_GT','MV');

select l.locacion, c.locno, f.semana, b.labor_cyd
from STG_PLANILLA_GT c
left outer join stg_fecha f on (c.fecha=f.fecha)
left outer join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
left outer join stg_labor b on ( c.instancia = b.instancia
    and c.nomec = b.nomec
    and c.aplic = b.aplic
    and c.clave = b.clave
    and '-ND-' = b.labor_join)
where b.id_actividad = 31
--and l.cc = '   140112002'
and f.ano = 2018
group by l.locacion, c.locno, f.semana, b.labor_cyd
order by 1,2,3,4
;

CREATE MATERIALIZED VIEW STG_PLANILLA_GT
NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED USING INDEX REFRESH COMPLETE ON DEMAND USING TRUSTED CONSTRAINTS DISABLE QUERY REWRITE
AS
with periodos_carga as (
select fecha from agrostg.stg_periodos_carga_vw
where tipo = 'STG_PLANILLA_GT'
)
, obi_planilla_vw as (
select a.* from fro_obi_planilla a
union all
select h.* from fro_obi_planilla_h h
left outer join periodos_carga fc on (1=1)
where h.fecha_finca >= fc.fecha
),

fecha_tasa as (
    select f.*, t.tasa_cambio tc
    from stg_fecha f
    left outer join stg_tasa_cambio t 
        on (f.fecha between t.fecha and t.fecha_lead)
    where moneda_origen = 'GTQ' and moneda_destino = 'USD'
)

select
a.*, 
decode(f.periodo_num,14,f.periodo_fecha_ini-1,f.fecha) fecha,
case when a.instancia=1 and a.cia=25 then 'PALMA' else 'BANANO' end negocio,
case when a.aplic in ('1','3') then valor_gtq+ajuste_gtq else 0 end sal_prom_persona,
a.valor_gtq/ f.tc valor,    a.bonoh_gtq/ f.tc bonoh, 
a.bonom_gtq/ f.tc bonom,    a.ajuste_gtq/ f.tc ajuste,

((valor_prest_gtq + valor_prest_gtq/24)/12)/ f.tc prestacion,
(valor_prest_gtq/24)/ f.tc vacaciones,
(valor_prest_gtq*0.1067)/ f.tc cuota_patronal,

((valor_prest_gtq + valor_prest_gtq/24)/12) prestacion_gtq,
(valor_prest_gtq/24) vacaciones_gtq,
(valor_prest_gtq*0.1067) cuota_patronal_gtq,
 f.tc,
nvl(c.ccf, '-ND-') cc, 
nvl(to_char(qa.drky), '-ND-') actividad_cod,
case when l.inversion = 1 and ac.costo_locinv = 0 then 'INVERSION' else 'COSTO' end tipo_oper,
case when a.tipo_reg = 'COSTO' and la.id_actividad is null then 1
else 0 end ausentismo,
/*DE CONF CUENTAS PLANILLA (POLIZA) --161108 Se quitan y se deja un campo para JOIN*/
lpad(a.instancia,3,'0') || lpad(a.cia,3,'0') || lpad(a.locno,3,'0') || lpad(la.id_actividad,3,'0') config_planilla_join
  from obi_planilla_vw a
  left outer join fecha_tasa f on (a.fecha_finca = f.fecha_finca)
  left outer join bi_equivcc@agricultura c on (a.instancia=c.instancia and a.cia=c.cia and a.locno=c.locacion_cod)
  left outer join agrodw.dim_labor_tab la on (a.instancia=la.instancia and a.nomec=la.nomec and a.clave=la.clave and a.aplic=la.aplic and la.labor_join='-ND-')
  left outer join stg_obiactivida qa on (la.id_actividad=qa.actrmd)--EQUIVALENCIA DE ACTIVIDADES
  left outer join stg_locacion l on (nvl(c.ccf, '-ND-') = l.cc and f.fecha between l.fecha_ini and l.fecha_fin)
  left outer join stg_actividad ac on (nvl(to_char(qa.drky), '-ND-') = ac.actividad_cod)
  ;
 


select * from stg_planilla_gt c
where aplic in (1,3)
and valor_gtq+ ajuste_gtq != sal_prom_persona
;

select * from stg_tasa_cambio;

select * from stg_planilla_gt c
left outer join stg_tarifas_labores t on (c.nomec=t.nomec and c.clave=t.clave and c.aplic=t.aplic and c.locno=t.locacion) ;


select * from stg_planilla_gt c
left outer join stg_labor la on (c.instancia=la.instancia and c.nomec=la.nomec and c.clave=la.clave and c.aplic=la.aplic and la.labor='-ND-')
  left outer join stg_obiactivida qa on (la.id_actividad=qa.actrmd)--EQUIVALENCIA DE ACTIVIDADES  
;

select * from stg_labor
where instancia=1 and nomec=1 and clave=244
;

select distinct ausentismo
from STG_PLANILLA_GT c
;