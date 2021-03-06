/*----UPDATE PLANILLA 12/11/2018----*/
select distinct gljbcd from proddta.f0911 a 
where a.glicu in ('775944')
;

select distinct vnjbcd 
from proddta.f0911z1 a 
where a.vnicu in ('775944')
;

update proddta.f0911 a
set gljbcd = '1     '
where a.glicu in ('775944')
;
update proddta.f0911z1 a
set vnjbcd = '1     '
where a.vnicu in ('775944')
;
commit;

/*----UPDATE PLANILLA 17/10/2018----*/
select distinct gljbcd from proddta.f0911 a 
where a.glicu in ('759524', '761652', '763603', '766869')
;

select distinct vnjbcd 
from proddta.f0911z1 a 
where a.vnicu in ('759524', '761652', '763603', '766869')
;

update proddta.f0911 a
set gljbcd = '1     '
where a.glicu in ('759524', '761652', '763603', '766869')
;
update proddta.f0911z1 a
set vnjbcd = '1     '
where a.vnicu in ('759524', '761652', '763603', '766869')
;
commit;

/*----REVISON BATCHES PLANILLA ADMINISTRATIVA 23/07/2018----*/ 
--Cuenta de 22 batches en la Z1 --Count Z1 1659 --Count Diario 3,318 por ambas monedas.
--Ya se hizo update, ahorita no hay ning�n registro con "cero".
select count(*) 
from proddta.f0911z1 a 
where trim(vnjbcd) = '0'
;

select count(*)
from proddta.f0911 a 
where trim(gljbcd) = '0'
;

/*----REVISON BATCHES PLANILLA ADMINISTRATIVA 03/07/2018----*/ --Todo oK
select distinct gljbcd from proddta.f0911 a 
where a.glicu in ('682567', '682571', '682573', '682577', '682457', '682447', '682591', '682164', '682695', '682594', '689573', '690705', '689681', '690707', '689700', '690708', '689593', '690709', '690003', '690710', '690043', '690203', '691247', '691255', '691261', '691263', '690691', '690694', '690026', '690692', '701484', '701666', '701547', '701671', '701552', '701687', '701640', '701700', '701562', '701714', '701894', '701991', '701432', '701470', '695100', '701451', '695152', '701480', '701445', '701482', '706505', '709613', '706511', '709731', '707468', '709621', '707485', '709626', '707603', '709736', '706722', '709723', '706211', '709750', '706180', '709487', '706228', '709483', '706384', '709486', '715075', '718763', '715081', '718781', '715088', '718828', '715091', '718832', '715094', '718836', '718046', '718843', '718982', '719027', '715098', '718852', '715109', '718857', '715106', '718861', '724273', '728596', '724299', '728610', '724385', '728633', '724065', '728641', '704331', '728647', '724546', '728252', '723804', '728547', '723690', '728164', '723721', '728183', '723771', '728218')
;

select distinct vnjbcd 
from proddta.f0911z1 a 
where a.vnicu in ('682567', '682571', '682573', '682577', '682457', '682447', '682591', '682164', '682695', '682594', '689573', '690705', '689681', '690707', '689700', '690708', '689593', '690709', '690003', '690710', '690043', '690203', '691247', '691255', '691261', '691263', '690691', '690694', '690026', '690692', '701484', '701666', '701547', '701671', '701552', '701687', '701640', '701700', '701562', '701714', '701894', '701991', '701432', '701470', '695100', '701451', '695152', '701480', '701445', '701482', '706505', '709613', '706511', '709731', '707468', '709621', '707485', '709626', '707603', '709736', '706722', '709723', '706211', '709750', '706180', '709487', '706228', '709483', '706384', '709486', '715075', '718763', '715081', '718781', '715088', '718828', '715091', '718832', '715094', '718836', '718046', '718843', '718982', '719027', '715098', '718852', '715109', '718857', '715106', '718861', '724273', '728596', '724299', '728610', '724385', '728633', '724065', '728641', '704331', '728647', '724546', '728252', '723804', '728547', '723690', '728164', '723721', '728183', '723771', '728218')
;

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