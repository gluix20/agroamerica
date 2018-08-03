--161109----NOTE: Se crea en instancia FRONTERA.

select * from obi_labor_vw;

CREATE OR REPLACE FORCE VIEW OBI_LABOR_VW 
AS
select distinct 1 instancia, 
l.nomec, l.aplic, l.clave, to_char(trim(l.descrip)) labor, to_char(trim(u.descrip)) unidad_medida, 
lpad(l.clave,4,'0') || ' - ' || to_char(trim(l.descrip)) labor_cyd,
a.id_actividad, 
to_char(trim(a.descripcion_actividad)) actividad_pla
from trabajos t
join activida l on (t.nomec = l.nomec and t.clave = l.clave and t.aplic = l.aplic)
left outer join unidades u on (l.unidad=u.unidad)
left outer join costo.actividades a on (l.id_actividad = a.id_actividad)
where t.ano >= 2014
union all
select distinct 2 instancia, 
l.nomec, l.aplic, l.clave, to_char(trim(l.descrip)) labor, to_char(trim(u.descrip)) unidad_medida, 
lpad(l.clave,4,'0') || ' - ' || to_char(trim(l.descrip)) labor_cyd,
a.id_actividad, 
to_char(trim(a.descripcion_actividad)) actividad_pla
from trabajos@sierra t
join activida@sierra l on (t.nomec = l.nomec and t.clave = l.clave and t.aplic = l.aplic)
left outer join unidades@sierra u on (l.unidad=u.unidad)
left outer join costo.actividades@sierra a on (l.id_actividad = a.id_actividad)
where t.ano >= 2014
union all
select distinct 3 instancia, 
l.nomec, l.aplic, l.clave, to_char(trim(l.descrip)) labor, to_char(trim(u.descrip)) unidad_medida, 
lpad(l.clave,4,'0') || ' - ' || to_char(trim(l.descrip)) labor_cyd,
a.id_actividad, 
to_char(trim(a.descripcion_actividad)) actividad_pla
from trabajos@vegas t
join activida@vegas l on (t.nomec = l.nomec and t.clave = l.clave and t.aplic = l.aplic)
left outer join unidades@vegas u on (l.unidad=u.unidad)
left outer join costo.actividades@vegas a on (l.id_actividad = a.id_actividad)
where t.ano >= 2014
union all
select distinct 4 instancia, 
l.nomec, l.aplic, l.clave, to_char(trim(l.descrip)) labor, to_char(trim(u.descrip)) unidad_medida, 
lpad(l.clave,4,'0') || ' - ' || to_char(trim(l.descrip)) labor_cyd,
a.id_actividad, 
to_char(trim(a.descripcion_actividad)) actividad_pla
from trabajos@pana t
join activida@pana l on (t.nomec = l.nomec and t.clave = l.clave and t.aplic = l.aplic)
left outer join unidades@pana u on (l.unidad=u.unidad)
left outer join costo.actividades@pana a on (l.id_actividad = a.id_actividad)
where t.ano >= 2014
;

