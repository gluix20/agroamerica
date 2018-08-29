--alter view AGROSTG.STG_MDO_DNS_VW compile;

select * from agrostg.STG_MDO_DNS_VW c
;

--drop view agrostg.STG_MDO_DNS_VW ;
CREATE OR REPLACE FORCE VIEW agrostg.STG_MDO_DNS_VW 
AS

select c.tipo_costo, c.prorrateo,0 densificacion, c.negocio, c.descripcion,  
c.tasa_cambio, c.clave, c.aplic, c.nomec, c.labor_join, c.fecha, c.fecha_finca, 
c.cc, c.instancia,
c.actividad_cod, c.cuenta, c.tipo_oper,
c.mdo, c.ordin, c.extra, c.septi, c.feria, c.asmin, 
c.valor, c.bonom, c.bonod, c.exced, c.prest, c.patro, 
c.fuerza_jornal, 
c.cantidad, c.horas, c.frecuencia, c.registros
from stg_mdo_prr c

    UNION ALL
    
select 
'-ND-' tipo_costo, 0 prorrateo,1 densificacion, '-ND-' negocio,
'DENSE' descripcion,
0 tasa_cambio, c.clave, c.aplic, c.nomec, c.labor_join, max(f.fecha) fecha, max(f.fecha+1) fecha_finca, 
c.cc, c.instancia,
c.actividad_cod, '-ND-' cuenta, c.tipo_oper,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
from (
with combimdo as (
        select c.actividad_cod,c.instancia,c.nomec,c.aplic,c.clave, c.labor_join,
        c.tipo_oper,f.periodo_cod,l.region
        from stg_mdo_prr c
        left outer join stg_fecha f on (c.fecha=f.fecha)
        left outer join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
        group by c.actividad_cod,c.instancia,c.nomec,c.aplic,c.clave, c.labor_join,
        c.tipo_oper,f.periodo_cod,l.region
        order by 1,2,3,4
), loc as (
        select l.region, l.ccf,f.periodo_cod
        from stg_locacion l 
        left outer join stg_fecha f on (f.fecha between l.fecha_ini and l.fecha_fin)
        where l.distrito not in ('OVERHEAD','COSTOS FIJOS','NO DEFINIDO')
        group by l.region, l.ccf,f.periodo_cod
        order by 1,2,3
        
), fec as (
        select f.periodo_cod,f.semana_cod,max(f.fecha) fecha
        from stg_fecha f
        group by f.periodo_cod,f.semana_cod
        
), combiloc as (
        select c.actividad_cod,c.instancia,c.nomec,c.aplic,c.clave, c.labor_join,
        c.tipo_oper,c.periodo_cod,l.region,l.ccf cc
        from combimdo c 
        left outer join loc l on (c.region=l.region and c.periodo_cod=l.periodo_cod)
        --order by 1,2,3,4,5
        
)
select * from (
select c.actividad_cod,c.instancia,c.nomec,c.aplic,c.clave, c.labor_join,
c.tipo_oper,c.cc,f.semana_cod
from combiloc c
left outer join fec f on (c.periodo_cod=f.periodo_cod)
--order by 1,2,3,4
minus 
select c.actividad_cod,c.instancia,c.nomec,c.aplic,c.clave, c.labor_join,
c.tipo_oper,l.ccf cc,f.semana_cod 
from stg_mdo_prr c
        left outer join stg_fecha f on (c.fecha=f.fecha)
        left outer join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
        group by c.actividad_cod,c.instancia,c.nomec,c.aplic,c.clave, c.labor_join,
        c.tipo_oper,l.ccf,f.semana_cod
        order by 1,2,3,4
)) c 
left outer join stg_fecha f on (c.semana_cod=f.semana_cod)
group by 'DENSE',
c.actividad_cod,c.instancia,c.nomec,c.aplic,c.clave, c.labor_join,
c.tipo_oper,c.cc,c.semana_cod

;