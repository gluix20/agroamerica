--drop VIEW AGROSTG.STG_PROD_PRR_VW;

--150427----NOTE: Si está bien la distribución aunque cae en otras actividades cuando se hace el prorrateo.
--150909----NOTE: Se agregan los FILTROS SOLICITADOS.
--160127----NOTE: Se cambia la ref a ctrl_historico por la nueva forma SCD de Locaciones.

select * from agrostg.STG_PROD_PRR_VW c
left outer join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
where l.nivel = 'POIC'
;

CREATE or replace VIEW AGROSTG.STG_PROD_PRR_VW
AS 
select * from (

select c.cc, c.fecha, c.tipo_cod, c.tipo, c.contenedor, c.puerto, c.comercializadora, c.medcod,
c.cajas, c.exportadas, c.locales, c.racimos, c.embolsados, c.identificados, c.cpo,
c.cpko, c.procesada, c.harina, c.cosechada, c.despachos
from agrostg.stg_produccion c
left outer join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
where l.distrito != 'PLANTA DE BENEFICIO'
    UNION ALL
    
    select * from (
select p.cc, c.fecha, c.tipo_cod, c.tipo, c.contenedor, c.puerto, c.comercializadora, c.medcod,
c.cajas, c.exportadas, c.locales, c.racimos, c.embolsados, c.identificados, round(c.cpo*p.factor,2),
round(c.cpko*p.factor,2), round(c.procesada*p.factor,2), round(c.harina*p.factor,2), 
c.cosechada, c.despachos
from (
select l.region_cod, f.ano, f.semana_cod, c.*
from agrostg.stg_produccion c
join agrostg.stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
join agrostg.stg_fecha f on (c.fecha=f.fecha)
where l.distrito = 'PLANTA DE BENEFICIO'
) c
join stg_prorrateo_palma_fac_vw p on (c.region_cod=p.region_cod and c.ano=p.ano and c.semana_cod=p.semana_cod)
)
)
;


