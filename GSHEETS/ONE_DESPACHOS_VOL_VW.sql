select * from agrostg.one_loads_vols_vw
where semana=201604
;


create or replace view agrostg.one_loads_vols_vw
as
select 

order by nvl(a.pais_origen,b.pais_origen) pais_origen, 
nvl(a.cliente,b.cliente) cliente, 
nvl(a.semana,b.semana) semana,
nvl(a.tipo_fruta,b.tipo_fruta) tipo_fruta,
nvl(vol_loads,0) vol_loads, 
nvl(loads,0) loads
from (
    select pais_origen,cliente,semana,tipo_fruta,
    sum(loads) vol_loads
    from one_volumen
    group by pais_origen,cliente,semana,tipo_fruta
) a
full outer join (
    select pais_origen,cliente,semana,tipo_fruta,
    count (distinct contenedor) loads 
    from one_loads
    group by pais_origen,cliente,semana,tipo_fruta
) b 
on (trim(a.cliente)=trim(b.cliente) and a.semana=b.semana)
;

