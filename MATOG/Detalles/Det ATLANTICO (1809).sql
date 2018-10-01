select *
from stg_costos_og c
left outer join stg_fecha f on (c.fecha=f.fecha)
left outer join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
left outer join stg_actividad a on (c.actividad_cod=a.actividad_cod)
where l.nivel = 'ATLA'
and f.ano = 2018
and f.semana = 33
and trim(a.actividad_cod) = '225' 
and c.tipo_costo= 'PLA'
order by 1
;

select c.*, b.*
from stg_mano_obra c
left outer join stg_fecha f on (c.fecha=f.fecha)
left outer join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
left outer join stg_actividad a on (c.actividad_cod=a.actividad_cod)

left outer join stg_labor b on ( c.instancia = b.instancia
    and c.nomec = b.nomec
    and c.aplic = b.aplic
    and c.clave = b.clave
    and '-ND-' = b.labor_join)
where l.nivel = 'ATLA'
and f.ano = 2018
and f.semana = 33
and b.id_actividad= 0
order by 1
;


select nvl(a.actividad_cod, b.actividad_cod) actividad_cod
, nvl(a.actividad, b.actividad) actividad
, a.mdo valor_planilla, b.valor valor_jde
from (
select a.actividad_cod
, a.actividad
, round(sum(c.mdo),2) mdo
from stg_mano_obra c
left outer join stg_fecha f on (c.fecha=f.fecha)
left outer join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
left outer join stg_actividad a on (c.actividad_cod=a.actividad_cod)
left outer join stg_labor b on ( c.instancia = b.instancia
    and c.nomec = b.nomec
    and c.aplic = b.aplic
    and c.clave = b.clave
    and '-ND-' = b.labor_join)
where l.nivel = 'ATLA'
and f.ano = 2018
and f.periodo = 'PERIODO 09'
and c.tipo_costo = 'PLA'
group by a.actividad_cod
, a.actividad
order by 1
) a
full outer join (
select a.actividad_cod
, a.actividad
, sum(c.valor) valor
from stg_costos_og c
left outer join stg_fecha f on (c.fecha=f.fecha)
left outer join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
left outer join stg_actividad a on (c.actividad_cod=a.actividad_cod)
where l.nivel = 'ATLA'
and f.ano = 2018
and f.periodo = 'PERIODO 09'
and c.tipo_costo= 'PLA'
group by a.actividad_cod
, a.actividad
order by 1
) b
on (a.actividad_cod = b.actividad_cod)
;


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

