--1503----NOTE: Se agrega al query los campos de region, distrito y locacion para poder agrupar por CC en STG_PRODUCCION
--1503----NOTE: Se agregó case para no hacer SUM cuando son hectareas el mismo día.
--150407--TIME: Select * 20 segs
--150506--NOTE: Se quitan las referencias a OBCENCOS.
--150831--NOTE: Se agrega cc.inversion para poder diferencias hectáreas.
--151028--NOTE: Se agrega campos de tipo de captura CAPCOD y CAPDES.

select * from obi_medprod
where trim(ccf) = '9100103'
and medcod = 'HEC'
order by fecha desc
;

CREATE OR REPLACE FORCE VIEW "AGRICULTURA"."OBI_MEDPROD"
AS


select /*+ use_nl(j) NO_MERGE(j) */
l.region_cod, l.distrito_cod, l.locacion_cod, l.ccf, l.inversion, l.fecha_ini, l.fecha_fin,
lpad(trim(to_char(j.mcmcu)),12,' ') cc, trunc(joufecha) fecha, 
trim(to_char(c.medcod)) medcod,
trim(to_char(j.actcod)) actcod,
trim(to_char(a.actdes)) actdes,
j.capcod, --NUMBER(6,0)
trim(to_char(c.capdes)) capdes,
nvl(c.comercializadora,'NO DEFINIDO') comercializadora, 
decode(j.actcod,600,replace(replace(to_char(j.jouuni1),' '),'-'),'-ND-') contenedor,
decode(j.actcod,600,replace(replace(to_char(j.jouuni2),' '),'-'),'-ND-') puerto,

case when trim(to_char(c.medcod)) in ('HA1','HEC') then 
avg(to_number(decode(TRIM(TRANSLATE(jouuni1, ' .0123456789',' ')),null,nvl(trim(jouuni1),0),0)))
when trim(to_char(j.actcod)) not in ('600') then
sum(to_number(decode(TRIM(TRANSLATE(jouuni1, ' .0123456789',' ')),null,nvl(trim(jouuni1),0),0)))
else sum(0) end cantidad,
case when trim(to_char(c.medcod)) in ('HA1','HEC') then 
avg(to_number(decode(TRIM(TRANSLATE(jouuni2, ' .0123456789',' ')),null,nvl(trim(jouuni2),0),0)))
when trim(to_char(j.actcod)) not in ('600') then
sum(to_number(decode(TRIM(TRANSLATE(jouuni2, ' .0123456789',' ')),null,nvl(trim(jouuni2),0),0)))
else sum(0) end cantidad2,
case when trim(to_char(c.medcod)) in ('HA1','HEC') then 
avg(to_number(decode(TRIM(TRANSLATE(jouuni3, ' .0123456789',' ')),null,nvl(trim(jouuni3),0),0)))
else sum(to_number(decode(TRIM(TRANSLATE(jouuni3, ' .0123456789',' ')),null,nvl(trim(jouuni3),0),0)))
end cantidad3

from journallevel1 j
left outer join proddta.f0006 f on (j.mcmcu=f.mcmcu)
left outer join captura c on (j.capcod=c.capcod)
left outer join actagri a on (j.actcod=a.actcod and f.mcco=a.gbco)
left outer join bi_locacion_vw l on (j.mcmcu=l.cc and trunc(joufecha) between l.fecha_ini and l.fecha_fin)--Se une para obtener el grupo de CCs (REG,DIS,LOC)
where trunc(joufecha) >= to_date(to_char(joufecha,'YYYY') || '0101','YYYYMMDD')
and trunc(joufecha) >= to_date('20131220','YYYYMMDD')
group by l.region_cod, l.distrito_cod, l.locacion_cod, l.ccf, l.inversion, l.fecha_ini, l.fecha_fin,
lpad(trim(to_char(j.mcmcu)),12,' '), trunc(joufecha), trim(to_char(c.medcod)),trim(to_char(j.actcod)),
trim(to_char(a.actdes)),
j.capcod,
trim(to_char(c.capdes)),
nvl(c.comercializadora,'NO DEFINIDO'),
decode(j.actcod,600,replace(replace(to_char(j.jouuni1),' '),'-'),'-ND-'),
decode(j.actcod,600,replace(replace(to_char(j.jouuni2),' '),'-'),'-ND-')
;

select * from captura
where medcod='EMB'
order by 1
;
select * from actagri
;

select * from bi_locacion_vw;
