select * from palm_produccion_vw;

create or replace view palm_produccion_vw
as
select nvl(a.extractora,b.extractora) extractora , nvl(a.fecha,b.fecha) fecha,
nvl(a.harina,0) harina, nvl(a.cpo,0) cpo, nvl(a.cpko,0) cpko, 
nvl(b.harina,0) bud_harina, nvl(b.cpo,0) bud_cpo, nvl(b.cpko,0) bud_cpko
from (

select 'AGROACEITE' extractora, 
max(f.fecha) fecha, sum(harina) harina, sum(cpo) cpo, sum(cpko) cpko 
from stg_produccion c
left outer join stg_fecha f on (c.fecha=f.fecha)
left outer join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
where l.region='PACIFICO'
and f.ano>=2016
group by f.semana_cod
order by 1,2
) a
full outer join (
select 'AGROACEITE' extractora, 
max(f.fecha) fecha, sum(harina) harina, sum(cpo) cpo, sum(cpko) cpko  from stg_budget c
left outer join stg_fecha f on (c.fecha=f.fecha)
left outer join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
where l.region='PACIFICO'
and f.ano>=2016
and c.budget_tipo='JD'
group by f.semana_cod
order by 1,2
) b on (a.extractora=b.extractora and a.fecha=b.fecha)


;

