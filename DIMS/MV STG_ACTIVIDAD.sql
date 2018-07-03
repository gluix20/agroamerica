drop MATERIALIZED VIEW "AGROSTG"."STG_ACTIVIDAD";
exec ETL_SCRIPTS.refresh_now('DIMS','AGROSTG','STG_ACTIVIDAD','MV');

--150318----NOTE: Se agrega las líneas comentadas para refrescamiento de MV.
--150318----NOTE: La MV ya incluye todas las actividades que están presentes en JDE, 
------------------pero hay que asegurarse que las que vienen de planilla esten presentes 
------------------en la tabla de equivalencias OBIEQACTIVIDA.
--150318----TIME: 1 seg
--150611----NOTE: Se define el case de proceso y proceso order by.
--151104----NOTE: Se agrega join con la tabla stage de proceso.
--160125----NOTE: Se agrega case en el join para desglosar EXPORTACIÓN.

select * from stg_actividad
order by 2,1,4,3
;

CREATE MATERIALIZED VIEW "AGROSTG"."STG_ACTIVIDAD"
 NOCOMPRESS NOLOGGING TABLESPACE "STAGE" 
  BUILD IMMEDIATE USING INDEX REFRESH COMPLETE ON DEMAND USING TRUSTED CONSTRAINTS
  AS 
  select 
  to_char(drky) actividad_cod, 
  trim(to_char(drdl01)) actividad,
  to_char(trim(a.drdl02)) proceso_jde,
  p.proceso,  
  p.proceso_ordby,
  p.macro,
  p.macro_ordby,
  case when p.rep_sem = 1 then p.proceso
  else trim(to_char(a.drdl01)) end actividad_rep_sem,
  case when p.rep_sem = 1 then to_char(p.proceso_ordby)
  else to_char(drky) end actividad_rep_sem_ordby,
  p.costo_locinv
from prodctl.f0005@agricultura a
left outer join agrostg.proceso p on (
case when trim(a.drky) in ('450','588') then '07 car' else substr(to_char(a.drdl02),1,6) end  =  p.proceso_match
)
where a.drsy = '09' and a.drrt = '16'
union all
select '-ND-', 'NO DEFINIDO', 'NO DEFINIDO', 'NO DEFINIDO', 99, 'NO DEFINIDO',99,'NO DEFINIDO','-ND-',1 from dual
;


/*Estos dos añadidos son para el prorrateo del reporte semanal*/
union all
select 'DEPRE', 'DEPRECIACIONES', '02 depreciaciones', 'DEPRECIACIONES', 2, 'CULTIVO',1 from dual
union all
select 'FIJOS', 'FIJOS FINCA', '06 costos fijos finca', 'FIJOS FINCA', 5, 'FIJOS',3 from dual

;
select * from stg_actividad
where proceso = 'FINANCIEROS'
;

select * from   PRODCTL.F0005@agricultura a
where a.DRSY = '09' and a.DRRT = '16'
;

select * from   PRODdta.F0901@agricultura
;
--No se puede incluir obieqactivida@agricultura a la dimensión porque se duplican
--los códigos de JDE por varios diferentes de la planilla.

select *
from   PRODCTL.F0005@agricultura a
left outer join agrostg.proceso p on (substr(a.drdl02,1,6)=p.proceso_match)
where a.DRSY = '09' and a.DRRT = '16'
and p.macro = 'EXPORTACIÓN'
order by proceso_ordby
;

select * from obieqactivida@agricultura;