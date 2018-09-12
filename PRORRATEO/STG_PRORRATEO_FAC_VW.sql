--drop view STG_PRORRATEO_FAC_VW;

--150415----NOTE: Se comenta WHERE en el que se restringía el prorrateo solo a las regiones 1 y 2. Se quita el WHERE para que solo pase la cia 00070.
----------------- Se cambia el nivel del prorrateo de periodo a semana. Se comentan los Order By para que no afecten al Sum Over Partition.
--150415----NOTE: Agrega decode en hectáreas a Q1 para que no pasen las de producción.
--150824----NOTE: Se agrega el usuario a STG_PRODUCCION porque antes había un sinónimo.
--150901----NOTE: Se cambia STG_PRODUCCION por STG_HAS.
--150915----NOTE: Se agrega el campo inversion.
--151022----NOTE: Se agrega el campo ccf.
--160127----PEND: Agregar filtro para que no se prorratee por OVERHEAD.
--160302----NOTE: Se agrega cambio de regla en CCs Inversion para PALMA, BANANO queda igual.

select * from STG_PRORRATEO_FAC_VW
where nivel='BTC1' and ano=2017
order by ano, semana_cod
;

CREATE OR REPLACE FORCE VIEW agrostg.STG_PRORRATEO_FAC_VW
as

with has_consolidado as --Promedio de hectáreas semanal y consolidado real y budget. 
(
select nvl(r.cc,b.cc) cc, nvl(r.ano,b.ano) ano, 
nvl(r.semana_cod,b.semana_cod) semana_cod, nvl(r.fecha,b.fecha) fecha,
nvl(r.has_cult,0) has_cult, nvl(r.has_prod,0) has_prod, nvl(b.bud_has_prod,0) bud_has_prod
from
(select c.cc, f.ano, f.semana_cod, min(f.fecha) fecha,
      avg(c.ha_inv) has_cult, avg(c.hectareas) has_prod
      from agrostg.stg_has c
      join stg_fecha f on (c.fecha = f.fecha)
      group by c.cc, f.ano, f.semana_cod
      ) r full outer join 
(select c.cc, f.ano, f.semana_cod, min(f.fecha) fecha,
avg(hectareas) bud_has_prod 
from stg_bud_has c
      join stg_fecha f on (c.fecha = f.fecha)
      where c.budget_tipo = 'JD'
      group by c.cc, f.ano, f.semana_cod
) b on (r.cc=b.cc and r.semana_cod=b.semana_cod)      
),

 has_total as ( --Se totalizan las hectáreas a nivel.
  select l.nivel, l.region_cod, l.region, l.distrito, l.locacion, c.*,
  sum(c.has_cult) over (partition by l.region_cod,c.ano,c.semana_cod) as has_cult_region,
  sum(c.has_cult) over (partition by l.nivel,c.ano,c.semana_cod) as has_cult_nivel,
  sum(c.has_prod) over (partition by l.region_cod,c.ano,c.semana_cod) as has_prod_region,
  sum(c.has_prod) over (partition by l.nivel,c.ano,c.semana_cod) as has_prod_nivel,
  sum(c.bud_has_prod) over (partition by l.region_cod,c.ano,c.semana_cod) as bud_has_prod_region,
  sum(c.bud_has_prod) over (partition by l.nivel,c.ano,c.semana_cod) as bud_has_prod_nivel
  from has_consolidado c
  join stg_locacion l on (c.cc = l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
)
--Q3 Saca el factor por finca (CC) y semana segun su proporción de HA a la región.
select c.*,
decode(c.has_cult_region,0,0,c.has_cult/c.has_cult_region) factor,
decode(c.has_cult_nivel,0,0,c.has_cult/c.has_cult_nivel) factor_nivel,
decode(c.bud_has_prod_region,0,0,c.bud_has_prod/c.bud_has_prod_region) factor_bud,
decode(c.bud_has_prod_nivel,0,0,c.bud_has_prod/c.bud_has_prod_nivel) factor_nivel_bud
from has_total c

;



