drop materialized view STG_LOCACION;
exec ETL_SCRIPTS.refresh_now('DIMS','AGROSTG','STG_LOCACION','MV');

--150407----NOTE: Se agrega negocio.
--150506----NOTE: Se cambió por completo la lógica del query. El anterior se encuentra en MV STG_LOCACION v1504.sql
--150508----NOTE: Trim a nivel.
--150518----NOTE: Se elimina el union all de NO DEFINIDO.
--151022----NOTE: Se agrega el campo ccf al select.
--160127----NOTE: Se agrega cia 00520. Se convierte en la fuente de la DIM_LOCACION con SCD.

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
 select 
 case 
 when g.nivdes in ('POIC','palcon') then 'PALMA'
 else 'BANANO' end negocio,
 trim(to_char(g.nivdes)) nivel,
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
 from proddta.f0006@agricultura f
 left outer join infodb.relemprlevel1@agricultura g on (f.mcco=g.gbco)
 join bi_carga_cia@agricultura bcc on (g.nivcod=bcc.nivcod and g.nivdes=bcc.nivdes)
 left outer join bi_locacion_vw@agricultura l on (f.mcmcu=l.cc)
 left outer join obdistritos@agricultura d ON (l.distrito_cod = d.odiscod and l.region_cod = d.oregcod)
 left outer join obregiones@agricultura r ON (d.oregcod = r.oregcod)
 left outer join bi_ref_region@agricultura rr on (f.mcco=rr.cia)
 where r.negocio is not null
 ;

select * from proddta.f0006@agricultura f
where mcco = '00165'
;

select * from infodb.relemprlevel1@agricultura
where nivdes = 'BTC4'
;
