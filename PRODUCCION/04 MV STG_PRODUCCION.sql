drop materialized view STG_PRODUCCION;
exec ETL_SCRIPTS.refresh_now('PROD','AGROSTG','STG_PRODUCCION','MV');

--1503------NOTE: Se cambió la lógica de STG_PRODUCCION, agregando STG_HAS para generar el merge join entre CCs y todas las fechas del año
--1503------NOTE: Agregar los CCs de Palma a OBCencos para que ingresen los datos de producción.
--150325----NOTE: Se agregaron las métricas de PALMA.
--150331----NOTE: Se validaron las HAS de Palma, ya están en reporte OBI.
--150331----NOTE: Se agregó where fecha >= '01/01/2014' a los nulos de Medprod
--150407----NOTE: Se agregó cpko y harina a las métrica de producción de PALMA.
--151102----NOTE: Se agregó capdes tipo y contenedor.
--150407----TIME: Refresh 1 seg


select * from stg_produccion c
      join stg_locacion l on (c.cc = l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
      join stg_fecha f on (c.fecha = f.fecha)
      where l.negocio='BANANO' and l.nivel ='ATLA'
      --and l.distrito='PLANTA DE BENEFICIO'
order by f.fecha
;

select * from stg_locacion
where nivel = 'ATLA';

  CREATE MATERIALIZED VIEW "AGROSTG"."STG_PRODUCCION"
 NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD IMMEDIATE USING INDEX  REFRESH COMPLETE ON DEMAND USING trusted CONSTRAINTS
 AS 
select c.cc,c.fecha, capcod tipo_cod, capdes tipo, contenedor, puerto, c.comercializadora, c.medcod,
sum(case when medcod='CAJ' then nvl(cantidad,0) else 0 end) cajas,
sum(case when medcod='CAJ' then nvl(cantidad2,0) else 0 end) exportadas,
sum(case when medcod='CAJ' then nvl(cantidad3,0) else 0 end) locales,
sum(case when medcod='RAC' then nvl(cantidad,0) else 0 end) racimos,
sum(case when medcod='RA2' then nvl(cantidad,0) else 0 end) embolsados,
sum(case when medcod='RA2' then nvl(cantidad2,0) else 0 end) identificados,
sum(case when medcod='TO01' then nvl(cantidad,0) else 0 end) cpo,
sum(case when medcod='TO01' then nvl(cantidad2,0) else 0 end) cpko,
sum(case when medcod='TO02' then nvl(cantidad,0) else 0 end) procesada,
sum(case when medcod='TO02' then nvl(cantidad2,0) else 0 end) harina,
sum(case when medcod='TO03' then nvl(cantidad,0) else 0 end) cosechada,
sum(case when actcod='600' then nvl(cantidad3,0) else 0 end) despachos
from stg_medprod c
left outer join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
--where l.distrito != 'OVERHEAD'
group by c.cc,c.fecha,capcod,capdes,contenedor, puerto, c.comercializadora, c.medcod
union all
select distinct '-ND-', fecha, 0,'NO DEFINIDO','-ND-','-ND-','NO DEFINIDO','-ND-',0,0,0,0,0,0,0,0,0,0,0,0 
from stg_medprod
where fecha >= to_date('01/01/2014','dd/mm/yyyy')
;

select * from stg_medprod
;

select cc,max(cantidad) from stg_medprod
where cc like '%390%'
and medcod = 'HA1'
group by cc
--order by 1,2,3,4,5
;
  
   select region_cod,distrito_cod,locacion_cod,cc,fecha from stg_medprod
   group by region_cod,distrito_cod,locacion_cod,cc,fecha
   order by 1,2,3,4,5
   ;

   
select region_cod,distrito_cod,locacion_cod,cc,min(fecha) fecha_ini, max(fecha) fecha_fin from stg_medprod
group by region_cod,distrito_cod,locacion_cod,cc
order by 1,2,3,4;

select min(fecha), max(fecha) from stg_produccion
;
