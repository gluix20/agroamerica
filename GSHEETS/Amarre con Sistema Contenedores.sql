with despachos as (
select f.semana_cod,f.fecha,l.locacion,l.cc,c.tipo,c.contenedor,c.despachos 
from stg_produccion c
left outer join stg_fecha f on (c.fecha=f.fecha)
left outer join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
where c.despachos != 0
and f.fecha is not null
and f.ano=2016
order by f.semana_cod desc,f.fecha desc,l.locacion
)
select * from one_loads_vw c
left outer join despachos d on (c.contenedor = d.contenedor)
where c.contenedor is not null
--and d.despachos is null

;


select contenedor
from stg_produccion
group by contenedor
having count(*)=1
;

jouuni1='MNBU901049-3' or jouuni1 ='GESU933141-0' or jouuni1='SMLU544827-4'--este ultimo viene de 2 emps


select * from stg_despachos where contenedor like '%CAIU5421600%';

