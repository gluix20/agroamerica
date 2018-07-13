drop MATERIALIZED VIEW "AGROSTG"."STG_ACTIVIDAD";
exec ETL_SCRIPTS.refresh_now('DIMS','AGROSTG','STG_ACTIVIDAD','MV');

select * from stg_actividad
order by 1,2,3
;

CREATE MATERIALIZED VIEW "AGROSTG"."STG_ACTIVIDAD"
 NOCOMPRESS NOLOGGING TABLESPACE "STAGE" 
  BUILD IMMEDIATE USING INDEX REFRESH COMPLETE ON DEMAND USING TRUSTED CONSTRAINTS
  AS 
  select 
  to_char(a.drky) actividad_cod, 
  trim(to_char(a.drdl01)) actividad,
  to_char(trim(a.drdl02)) proceso_jde,
  to_char(trim(a.drdl02)) proceso,  
  p.proceso_ordby,
  p.macro,
  p.macro_ordby,
  case when p.rep_sem = 1 then to_char(trim(a.drdl02))
  else trim(to_char(a.drdl01)) end actividad_rep_sem,
  
  case when p.rep_sem = 1 then to_char(trim(substr(a.drdl02,1,6)))
  else trim(to_char(a.drky)) end actividad_rep_sem_ordby,
  
  p.costo_locinv
from prodctl.f0005@agricultura a
left outer join agrostg.proceso p on ( substr(to_char(a.drdl02),1,6) =  p.proceso_match )
where a.drsy = '09' and a.drrt = '16'
union all
select '-ND-', 'NO DEFINIDO', 'NO DEFINIDO', 'NO DEFINIDO', 99, 'NO DEFINIDO',99,'NO DEFINIDO','-ND-',1 from dual
;


select * from   PRODCTL.F0005@agricultura a
where a.DRSY = '09' and a.DRRT = '16'
and trim(a.drky) in ('450','588')
;
