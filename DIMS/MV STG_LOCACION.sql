drop materialized view STG_LOCACION;
exec ETL_SCRIPTS.refresh_now('DIMS','AGROSTG','STG_LOCACION','MV');

select * from STG_LOCACION
where trim(cc) = '140112003'
order by 1,2,3,4,5
;

select * from agrodw.dim_locacion_tab l;
select * from stg_nivel;

select * from locacion@frontera;

 CREATE MATERIALIZED VIEW "AGROSTG"."STG_LOCACION"
 NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED USING INDEX REFRESH COMPLETE ON DEMAND
 AS
 with cias as (
    select g.gbco cia, g.nivcod
    , trim(to_char(g.nivdes)) nivel
    , bcc.negocio
    , bcc.pais
    from infodb.relemprlevel1@agricultura g
    join bi_carga_cia@agricultura bcc on (g.nivcod = bcc.nivcod and g.nivdes = bcc.nivdes)
    order by 1
 )
 select 
 trim(c.ccname) cia_nombre,
 trim(f.mcstyl) tipo_cc,
 ci.negocio,
 ci.nivel,
 to_char(f.mcco) cia, 
 decode(trim(r.oregdes),'NO DEFINIDO', to_char(f.mcmcu), to_char(l.ccf)) ccf,
 to_char(f.mcmcu) cc,
 to_char(trim(f.mcdl01)) centro_costo,
 to_char(trim(f.mcdc)) cc_nombre,
 l.inversion,
 l.comercializadora,
 nvl(decode(trim(r.oregdes),'NO DEFINIDO',rr.region_cod,l.region_cod),0) region_cod,
 nvl(decode(trim(r.oregdes),'NO DEFINIDO',rr.region,trim(r.oregdes)),'NO DEFINIDO') region,
 decode(trim(r.oregdes),'NO DEFINIDO',101,l.distrito_cod) distrito_cod,
 l.locacion_cod,
 decode(trim(r.oregdes),'NO DEFINIDO',to_char(trim(f.mcdl01)),trim(l.locacion)) locacion,
 decode(trim(r.oregdes),'NO DEFINIDO','OVERHEAD',trim(d.odisdes)) distrito,
 ci.pais,
 decode(trim(r.oregdes),'NO DEFINIDO',rr.grupo,trim(r.grupo)) grupo
 , nvl(l.fecha_ini, to_date('01/01/2013','dd/mm/yyyy')) fecha_ini
 , nvl(l.fecha_fin, to_date('31/12/2050','dd/mm/yyyy')) fecha_fin
 , decode(l.activo,1,'ACTIVO','INACTIVO') estado
 
 from proddta.f0010@agricultura c
 join cias ci on (c.ccco = ci.cia)
 join proddta.f0006@agricultura f on (c.ccco = f.mcco)
 left outer join bi_locacion_vw@agricultura l on (f.mcmcu=l.cc)
 left outer join obdistritos@agricultura d ON (l.distrito_cod = d.odiscod and l.region_cod = d.oregcod)
 left outer join obregiones@agricultura r ON (d.oregcod = r.oregcod)
 left outer join bi_ref_region@agricultura rr on (f.mcco=rr.cia)
 --where r.negocio is not null
 order by ccf
 ;

select * from proddta.f0006@agricultura f
where mcco = '00165'
;

select c.ccname company, f.*
from proddta.f0010@agricultura c
join proddta.f0006@agricultura f on (c.ccco = f.mcco)
;

select * from infodb.relemprlevel1@agricultura
where nivdes = 'BTC4'
;

select g.gbco cia, g.nivcod, g.nivdes nivel 
from infodb.relemprlevel1@agricultura g
join bi_carga_cia@agricultura bcc on (g.nivcod = bcc.nivcod and g.nivdes = bcc.nivdes)
order by 1
;

select negocio, pais, nivel, cia, grupo, region, distrito, locacion, cc_nombre, centro_costo, tipo_cc, cc
from stg_locacion
order by 3,5,6,7,8
;

