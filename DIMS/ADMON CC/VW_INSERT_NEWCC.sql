--150505----NOTE: Creación de vista
--150505----MEMO: Recordar hacer una limpieza periódica de CCs que por error fueron ingresados al JDE y luego borrados.

select * from vw_insert_newcc
;
alter view vw_insert_newcc compile;

create or replace view vw_insert_newcc as --20150416
with faltantes as (select cc from stg_costos_og
group by cc
minus
select cc from bi_consolidcc@agricultura)

select 0 region_cod,'-ND-' ccf, f.cc, trim(c.mcdl01) centro_costo 
from faltantes f join proddta.f0006@agricultura c
on (f.cc=c.mcmcu)
;

--Prueba para colocar en el archivo de dimensiones_pf01.sql
insert into bi_consolidcc@agricultura (region_cod,ccf,cc)
select region_cod,ccf,cc from vw_insert_newcc
;
commit;