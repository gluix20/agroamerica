drop materialized view STG_LOCACION;
exec ETL_SCRIPTS.refresh_now('DIMS','AGROSTG','STG_LOCACION','MV');

select * from STG_LOCACION
where cia = '00165'
order by 1,2,3,4,5
;

select * from agrodw.dim_locacion_tab l
where trim(cc)  in ('140115006');

select * from locacion@frontera;

 CREATE MATERIALIZED VIEW "AGROSTG"."STG_LOCACION"
 NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED USING INDEX REFRESH COMPLETE ON DEMAND
 AS
 with cias as (
    select g.gbco cia, g.nivcod, trim(to_char(g.nivdes)) nivel 
    from infodb.relemprlevel1@agricultura g
    join bi_carga_cia@agricultura bcc on (g.nivcod = bcc.nivcod and g.nivdes = bcc.nivdes)
    order by 1
 )
 select 
 trim(c.ccname) cia_nombre,
 trim(f.mcstyl) tipo_cc,
 case 
 when ci.nivel in ('POIC','palcon') then 'PALMA'
 else 'BANANO' end negocio,
 ci.nivel,
 to_char(f.mcco) cia, 
 decode(trim(r.oregdes),'NO DEFINIDO', to_char(f.mcmcu), to_char(l.ccf)) ccf,
 to_char(f.mcmcu) cc,
 to_char(trim(f.mcdl01)) centro_costo,
 l.inversion,
 l.comercializadora,
 decode(trim(r.oregdes),'NO DEFINIDO',rr.region_cod,l.region_cod) region_cod,
 decode(trim(r.oregdes),'NO DEFINIDO',101,l.distrito_cod) distrito_cod,
 l.locacion_cod,
 decode(trim(r.oregdes),'NO DEFINIDO',to_char(trim(f.mcdl01)),trim(l.locacion)) locacion,
 decode(trim(r.oregdes),'NO DEFINIDO','OVERHEAD',trim(d.odisdes)) distrito,
 decode(trim(r.oregdes),'NO DEFINIDO',rr.region,trim(r.oregdes)) region,
 decode(trim(r.oregdes),'NO DEFINIDO',rr.pais,trim(r.pais)) pais,
 decode(trim(r.oregdes),'NO DEFINIDO',rr.grupo,trim(r.grupo)) grupo,
 l.fecha_ini,
 l.fecha_fin,
 decode(l.activo,1,'ACTIVO','INACTIVO') estado
 from proddta.f0010@agricultura c
 join cias ci on (c.ccco = ci.cia)
 join proddta.f0006@agricultura f on (c.ccco = f.mcco)
 left outer join bi_locacion_vw@agricultura l on (f.mcmcu=l.cc)
 left outer join obdistritos@agricultura d ON (l.distrito_cod = d.odiscod and l.region_cod = d.oregcod)
 left outer join obregiones@agricultura r ON (d.oregcod = r.oregcod)
 left outer join bi_ref_region@agricultura rr on (f.mcco=rr.cia)
 order by ccf
 where r.negocio is not null
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
