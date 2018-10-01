--150505----NOTE: Creación de vista
--160316----NOTE: Comment sobre ST_PLANILLA_EC en lo que se reestablece.

select * from vw_insert_newloc
;

create or replace view vw_insert_newloc 
as
select instancia,cia,locacion_cod,'-ND-' ccf from 
(

select instancia,cia,locno locacion_cod from stg_planilla_gt
group by instancia,cia,locno

minus

select instancia,cia,locacion_cod from bi_equivcc@agricultura
)
;

insert into bi_equivcc@agricultura (instancia,cia,locacion_cod,ccf)
select instancia,cia,locacion_cod,ccf from vw_insert_newloc
;
commit;

select * from stg_mano_obra
where instancia=5 and locno =4
;


select instancia,cia,locno locacion_cod from stg_planilla_gt
where locno = 5
group by instancia,cia,locno
;
