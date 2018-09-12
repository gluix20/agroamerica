
select --*
f.ano,f.periodo,a.proceso,a.actividad,a.actividad_cod, l.grupo,l.locacion, c.cc, l.centro_costo,c.cuenta, c.descripcion,c.tipo, c.nodocto, c.batch, sum(valor) valor 
from stg_costos_og c
left outer join stg_fecha f on (c.fecha=f.fecha)
left outer join stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
left outer join stg_actividad a on (c.actividad_cod=a.actividad_cod)
where f.ano=2016
and f.periodo_cod = 201608
--and a.actividad_cod = '       250'
and l.nivel in ('BTC1')
--and l.locacion in ('87 EL ROSARIO','AJUSTES')
and a.macro not in ('OTROS', 'INGRESOS')
--and grupo_mat = 'CONTA DIRECTA'
--and valor < 0
and batch in ('497443','496207','497401','496207','497450','497386')
group by f.ano,f.periodo,a.proceso,a.actividad,a.actividad_cod, l.grupo,l.locacion, c.cc, l.centro_costo,c.cuenta, c.descripcion,c.tipo, c.nodocto, c.batch
order by valor
;

select * from stg_costos_og

;