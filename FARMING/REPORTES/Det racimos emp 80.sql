with data as (
select s.numero_semana semana, c.codigo_motor cable, pd.peso_fruto
from pesajes_det pd
left outer join semanas s on (pd.semana_id = s.semana_id)
left outer join fincas f on (pd.finca_id = f.finca_id)
left outer join cables c on (pd.cable_id = c.cable_id)
where substr(s.codigo_externo,1,4) = 2018
and f.finca_id = 20
and c.codigo_motor in ('53','54') --nombre de cable
)
select cable, semana, round(avg(peso_fruto),2) avg, count(*) cantidad
from data
group by semana, cable
order by 1,2
;

select * from semanas;
select * from fincas;
select * from cables
where finca_id = 20;