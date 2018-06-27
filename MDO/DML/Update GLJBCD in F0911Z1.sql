/*----UPDATE PLANILLA ADMINISTRATIVA 27/06/2018----*/
update proddta.f0911 a
set gljbcd = '      '
where trim(gljbcd) = '0'
;
update proddta.f0911z1 a
set vnjbcd = '      '
where trim(vnjbcd) = '0'
;
commit;

/*----UPDATE ECUADOR----*/
select distinct glicu from proddta.f0911
where trim(gljbcd) is null
and glkco in ('00910','00920')
and substr(glexa,1,4)='PLA '
;

update proddta.f0911 a
set gljbcd = '1     '
where trim(gljbcd) is null
and glkco in ('00910','00920')
and case when substr(glexa,1,4)='PLA ' and substr(glexa,1,6)!='PLA AD' then 1 else 0 end = 1
;

commit;
--rollback;

/*RESTAURAR*/
update proddta.f0911 a
set gljbcd = '1     '
where trim(gljbcd) is null
and glicu in ('357547')--,,')
and substr(glexa,1,4)='PLA '
;
commit;
--('357550','363861','357558','357547')      
--'357544','357545'

----UPDATE Z1----
update proddta.f0911 a
set gljbcd = '1     '
where exists (
select 1 from proddta.f0911z1 z 
where vndct='NM' and z.vnicu=a.glicu and z.vndct=a.gldct and z.vndoc=a.gldoc
)
and trim(a.gljbcd) is null
;
commit;
----FIN UPDATE Z1----

select *--glexa,trim(gljbcd) pla
--sum(glaa/100)
from proddta.f0911 a
where trim(gljbcd) = '1'
and glkco in ('00910','00920')
and case when substr(glexa,1,4)='PLA ' and substr(glexa,1,6)='PLA AD' then 1 else 0 end = 1
;


update proddta.f0911 a
set gljbcd = '      '
where trim(gljbcd) = '1'
and glkco in ('00910','00920')
and case when substr(glexa,1,4)='PLA ' and substr(glexa,1,6)='PLA AD' then 1 else 0 end = 1
;
commit;