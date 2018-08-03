--161109----NOTE: Se crea en instancia FRONTERA.

select * from obi_persona_vw;

drop view OBI_PERSONA_VW;
CREATE OR REPLACE FORCE VIEW OBI_PERSONA_VW 
AS
select /*+ USE_NL(t) NO_MERGE(t) PUSH_PRED(t) */
distinct 1 instancia, p.cia, p.codigo, 
trim(p.apellido1) || ' ' || trim(p.apellido2) || ', ' || trim(p.nombre1) || ' ' || trim(p.nombre2) persona 
from trabajos t join persona p on (t.cia=p.cia and t.codigo=p.codigo)
where t.ano >= 2014
union all
select /*+ USE_NL(t) NO_MERGE(t) PUSH_PRED(t) */
distinct 2 instancia, p.cia, p.codigo, 
trim(p.apellido1) || ' ' || trim(p.apellido2) || ', ' || trim(p.nombre1) || ' ' || trim(p.nombre2) persona 
from trabajos@sierra t join persona@sierra p on (t.cia=p.cia and t.codigo=p.codigo)
where t.ano >= 2014
union all
select /*+ USE_NL(t) NO_MERGE(t) PUSH_PRED(t) */
distinct 3 instancia, p.cia, p.codigo, 
trim(p.apellido1) || ' ' || trim(p.apellido2) || ', ' || trim(p.nombre1) || ' ' || trim(p.nombre2) persona 
from trabajos@vegas t join persona@vegas p on (t.cia=p.cia and t.codigo=p.codigo)
where t.ano >= 2014
union all
select /*+ USE_NL(t) NO_MERGE(t) PUSH_PRED(t) */
distinct 4 instancia, p.cia, p.codigo, 
trim(p.apellido1) || ' ' || trim(p.apellido2) || ', ' || trim(p.nombre1) || ' ' || trim(p.nombre2) persona 
from trabajos@pana t join persona@pana p on (t.cia=p.cia and t.codigo=p.codigo)
where t.ano >= 2014
;

