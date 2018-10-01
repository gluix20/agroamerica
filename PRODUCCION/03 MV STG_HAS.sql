drop materialized view STG_HAS;
exec ETL_SCRIPTS.refresh_now('PROD','AGROSTG','STG_HAS','MV');

--160114----NOTE: Reingeniería completa.

select * from stg_has
where trim(cc) like '910%' 
order by fecha desc
;
select * from stg_fecha;

CREATE MATERIALIZED VIEW STG_HAS
NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED USING INDEX  REFRESH COMPLETE ON DEMAND USING TRUSTED CONSTRAINTS
DISABLE QUERY REWRITE
as
with fecha_leading as (
      select m.*,   f.semana_cod, f.fecha,
      max(fecha) over (partition by m.ccf, m.medcod, f.semana_cod) fecha_sem_fin
      from (
            select region_cod,distrito_cod,locacion_cod,ccf, cc,medcod,fecha fecha_ini,
            nvl(lead(fecha,1) over (partition by ccf,cc,medcod order by fecha)-1,fecha_fin) fecha_lead,
            sum(decode(medcod,'HA1',cantidad,null)) hectareas, 
            sum(decode(medcod,'HEC',cantidad,null)) ha_inv
            from stg_medprod
            where medcod in ('HA1','HEC')
            --and cc='   140113002'
            group by region_cod,distrito_cod,locacion_cod,ccf,cc,medcod,fecha,fecha_fin
            --order by ccf,cc,medcod,fecha desc
      ) m join stg_fecha f
      on (f.fecha between m.fecha_ini and m.fecha_lead or (f.fecha>=m.fecha_ini and m.fecha_lead is null))
      order by ccf, medcod, fecha desc
), has as (
      select s.*, f.fecha from (
            select m.region_cod, m.distrito_cod, m.locacion_cod, m.ccf cc, m.semana_cod, 
            sum(hectareas) hectareas, sum(ha_inv) ha_inv
                  from (
                  select m.region_cod, m.distrito_cod, m.locacion_cod, m.ccf, m.semana_cod, m.fecha_sem_fin, m.fecha,
                  sum(hectareas) hectareas, 
                  sum(ha_inv) ha_inv 
                  from fecha_leading m
                  group by m.region_cod, m.distrito_cod, m.locacion_cod, m.ccf, m.semana_cod, m.fecha_sem_fin, m.fecha
            ) m
            where m.fecha_sem_fin = m.fecha
            group by m.region_cod, m.distrito_cod, m.locacion_cod, m.ccf, m.semana_cod
      ) s left outer join stg_fecha f on (s.semana_cod=f.semana_cod)
            --where cc = '   140112003'
      order by cc, fecha desc
), has_sem as (

       select substr(semana_cod,1,4) ano, semana_cod,cc,
       avg(hectareas) ha_sem, avg(ha_inv) ha_inv_sem
       from has
       group by semana_cod, cc, substr(semana_cod,1,4)

), has_ytd as (

       select 
       hs.semana_cod, hs.cc, 
       avg(nullif(hy.ha_sem,0)) has_prd_ytd,
       avg(nullif(hy.ha_inv_sem,0)) has_inv_ytd 
       from has_sem hs
       left outer join has_sem hy on (hs.cc=hy.cc and hs.ano=hy.ano and hs.semana_cod >= hy.semana_cod)
       --where hs.cc='   140112003'
       group by hs.semana_cod, hs.cc
       --order by hs.cc,hs.semana_cod desc,hy.semana_cod desc
   
   )
   
   select h.*,y.has_prd_ytd,has_inv_ytd
   from has h
   left outer join has_ytd y on (h.cc=y.cc and h.semana_cod=y.semana_cod)
   where 
   case when nvl(y.has_prd_ytd,0) + nvl(has_inv_ytd,0) = 0 then 0
   else 1 end = 1
   --and h.cc like '%140112003%'
   order by 1,2 desc,3 desc
   
   ;
   
   