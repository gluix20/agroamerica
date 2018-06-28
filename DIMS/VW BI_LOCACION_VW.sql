--161021--NOTE: Se crea.

select * from bi_locacion_vw
where (region_cod=8) and distrito_cod=1) or substr(locacion,1,2) in ('80','85','86','87')
order by ccf, fecha_ini
;

CREATE OR REPLACE FORCE VIEW "AGRICULTURA".bi_locacion_vw
AS
select l.comercializadora, l.region_cod, l.distrito_cod, 
l.locacion_cod, l.locacion,
decode(l.cccf,l.ccf,l.ccf,l.cccf) ccf, c.cc,
l.inversion, l.fecha_ini, l.fecha_fin, l.activo
from bi_consolidcc c 
left outer join bi_locacion l on (c.ccf=l.ccf)
;