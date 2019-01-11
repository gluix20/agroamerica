select * from tmp.dtj_detalle_jornales_Asueto@evolution;
select * from tmp.dtj_detalle_jornales_Extraordinario@evolution;
select * from tmp.dtj_detalle_jornales_Ordinario@evolution;
select * from tmp.dtj_detalle_jornales_Septimo@evolution;
select * from tmp.dtj_detalle_jornales_Estructura_Salarial@evolution;
select * from tmp.dtj_detalle_jornales_Bonificacion_Rendimiento@evolution;

select * from exp.emp_empleados@evolution;

select "inn_codemp", "inn_fecha_grabacion", "inn_tiempo", sum("inn_valor") from sal.inn_ingresos@evolution
where "inn_usuario_grabacion" = 'jarchila' and "inn_codemp"=4102
group by "inn_codemp", "inn_fecha_grabacion", "inn_tiempo"
order by "inn_fecha_grabacion" desc
;

select "inn_codemp", "inn_fecha_grabacion", sum("inn_valor") sueldo
from sal.inn_ingresos@evolution
where "inn_usuario_grabacion" = 'jarchila' and "inn_tiempo" = 28
group by "inn_codemp", "inn_fecha_grabacion"
having sum("inn_valor") > 20000
order by "inn_fecha_grabacion" desc
;


select * from sal.inn_ingresos@evolution
where "inn_usuario_grabacion" = 'jarchila' and "inn_codemp"=4102
order by "inn_fecha_grabacion" desc
;

select * from sal.emp_empleados@evolution
;

select *
from gt.dtj_detalle_jornales@evolution cf
left join gt.lab_labores@evolution l on (c."dtj_codlabor" = l."lab_codiea")
;

select "dtj_cantidadordinario" 
, "dtj_valorordinario"
, "dtj_totalordinario"
from gt.dtj_detalle_jornales@evolution c
left join gt.lab_labores@evolution l on (c."dtj_codlabor" = l."lab_codiea")
;

select * from gt.lab_labores@evolution l;
select * from gt.act_actividades@evolution a;
--Not
select * from gt.emp_empleado@evolution e;