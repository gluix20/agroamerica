drop materialized view STG_COSTOS_OG;
exec ETL_SCRIPTS.refresh_now('MATOG','AGROSTG','STG_COSTOS_OG','MV');
--alter materialized view STG_COSTOS_OG compile;
--purge recyclebin;

select * from stg_costos_og
;

create materialized view stg_costos_og
NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED REFRESH COMPLETE ON DEMAND using trusted constraints
AS
select * from obi_costos_og@agricultura
;

select sum(valor) from (
select cc,glmcu,sum(valor) valor
from stg_costos_og
--where cia not in ('00910','00920')--SI PEGA CON EL DATO REAL DESDE LA F0911
where cc='-ND-' 
--and glmcu='          70' and descripcion not like '%Cur%'
group by cc,glmcu
order by 1,2
)
;

select * from proddta.f0006@agricultura
where mcmcu in (select glmcu
from stg_costos_og
where tipo_oper='PRODUCCION'
and negocio= 'PALMA'
and valor != 0
group by glmcu)
order by 1
;


select distinct proceso, actividad
from stg_costos_og c
left outer join stg_actividad a on (c.actividad_cod=a.actividad_cod)
left outer join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
where negocio= 'PALMA'
--and glmcu= '    38011103' --CD 1Tr Valor 0 Drenajes
--and glmcu= '    38013301' --CD 5Tr Valor 1565.49 Drenajes
--and glmcu= '    39013301' --CD 94Tr Valor 0 Drenajes
--and glmcu= '    39010501'
--and actividad = '.'
order by 1,2
;


select tipo_costo,
sum(valor) 
from stg_costos_og
--where tipo_costo not in ('MO','CDP')
group by tipo_costo
;

select tipo_costo,sum(valor) from stg_materiales
--where cia='00070'
group by tipo_costo
;

select tipo_costo,sum(valor) from stg_mano_obra
--where cia='00070'
group by tipo_costo
;

select distinct cuenta_desc from stg_costos_og
where tipo_costo ='PLA'
;



select * from stg_costos_og c
left outer join stg_locacion l on (c.cc=l.cc)
left outer join stg_fecha f on (c.fecha=f.fecha)
left outer join stg_actividad a on (c.actividad_cod=a.actividad_cod)
where l.distrito = 'OVERHEAD'
and c.nivel = 'BTC1'
and l.region = 'FRONTERA'
and lower(a.PROCESO) like '%depre%'
order by c.fecha desc
;

