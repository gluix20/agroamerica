--150506----NOTE: Se comenta para que pasen todas las cuentas no solo costos y cualquier nivel.
               -- Por lo tanto se quita también los joins para obtener nivel 6 y 5.
--150709----NOTE: Se ordenan los comentarios del código.

select * from obi_cuentas_contables_vw;

create or replace force view obi_cuentas_contables_vw
as
select /*+ ordered */
to_char(a.gmco) cia, to_char(a.gmaid) aid, to_char(trim(a.gmmcu)||decode(trim(a.gmobj),null,'','.'||trim(a.gmobj))
||decode(trim(a.gmsub),null,'','.'||trim(a.gmsub))) cuenta,
to_char(a.gmmcu) cc, to_char(a.gmobj) obj, to_char(a.gmsub) sub, to_char(a.gmdl01) descripcion,
to_char(o.obj_desc) obj_desc,
--b.gmdl01 nivel6,
--c.gmdl01 nivel5,
to_char(lpad(a.gmr016,10,' ')) actividad_cod, to_char(c16.drdl01) actividad, to_char(c16.drdl02) proceso,
to_char(a.gmr017) cat17, to_char(c17.drdl01) cat17_desc,
to_char(a.gmr018) cat18, to_char(c18.drdl01) cat18_desc,
to_char(a.gmr019) cat19, to_char(a.gmr020) cat20
from proddta.f0901 a

join infodb.relemprlevel1 g on (a.gmco=g.gbco)
join bi_carga_cia bcc on (g.nivcod=bcc.nivcod and g.nivdes=bcc.nivdes)/*CONFIGURAR BI_CARGA_CIA*/

left outer join prodctl.f0005 c16 on (trim(a.gmr016)=trim(c16.drky) and c16.drsy='09' and c16.drrt='16')
left outer join prodctl.f0005 c17 on (trim(a.gmr017)=trim(c17.drky) and c17.drsy='09' and c17.drrt='17')
left outer join prodctl.f0005 c18 on (trim(a.gmr017)=trim(c18.drky) and c18.drsy='09' and c18.drrt='18')
left outer join (
            select trim(to_char(gmobj)) obj,trim(to_char(gmdl01)) obj_desc from proddta.f0901  
            where gmco='00001' and trim(gmmcu)='MD' and trim(gmsub) is null) o on (trim(to_char(gmobj))=o.obj)
;


select trim(obj) obj,descripcion from obi_cuentas_contables
where trim(sub) is null
group by trim(obj),descripcion;


select trim(to_char(gmobj)) obj,trim(to_char(gmdl01)) obj_desc from proddta.f0901
where gmco='00001' and trim(gmmcu)='MD' and trim(gmsub) is null
;