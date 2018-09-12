select * from stg_locacion
where cc in (
select c.cc
from stg_costos_og c
left outer join stg_fecha f on (c.fecha=f.fecha)
left outer join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
left outer join stg_actividad a on (c.actividad_cod=a.actividad_cod)
where c.nivel = 'ATLA'
group by c.cc
--order by c.cc
)
;


select *
from STG_MATOG_PRR_VW c
left outer join stg_fecha f on (c.fecha=f.fecha)
left outer join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
left outer join stg_actividad a on (c.actividad_cod=a.actividad_cod)
where c.nivel = 'ATLA'
and a.proceso = '08 Costos fijos dnv ovh'
and f.ano = 2018
and f.periodo = 'PERIODO 08'
--group by c.cc
--order by c.cc
;

select *
from stg_bud_has
where substr(trim(cc),1,3) in ('700','710')
;

select *
from stg_budget
where substr(trim(cc),1,3) in ('700','710')
and hectareas != 0
;

