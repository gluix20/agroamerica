select to_char(b.cia) cia,to_number(b.ano) azo, to_number(b.periodo) cper, trim(to_char(b.cat16)) cat16, trim(to_char(b.cat17)) cat17
, b.descripcion, b.saldo
--, sum(acumulado) cacumulado, 0 aacumulado, 0 pre_acum
from infodb.mvw_libros b, proddta.f0006 e, infodb.relemprlevel1 x, infodb.niveles y
where b.cc=e.mcmcu and e.mcco = x.gbco and x.nivcod = :p_nivcod and rtrim(x.nivdes) = rtrim(:p_nivdes)  -- extracto cias
and x.nivcoda = y.nivcod and x.nivdesa = y.nivdes -- Detalle
and b.ano = :p_azo and b.periodo=:p_per and libro='AC'
and substr(b.objeto,1,1)>=4
and trim(b.objeto) = '8050'
and trim(b.sub) = '0108'
--group by b.cia, b.ano, b.periodo, b.cat16, b.cat17, b.descripcion
;


select a.*, trim(c.drky) || ' - ' || drdl01 Cat16ds1, drdl02 cat16ds2, tdedes cat17ds1 from 
(
select to_char(b.cia) cia,to_number(b.ano) azo, to_number(b.periodo) cper, trim(to_char(b.cat16)) cat16, trim(to_char(b.cat17)) cat17
, b.descripcion
, sum(saldo) csaldo
, sum(acumulado) cacumulado, 0 aacumulado, 0 pre_acum
from infodb.mvw_libros b, proddta.f0006 e, infodb.relemprlevel1 x, infodb.niveles y
where b.cc=e.mcmcu and e.mcco = x.gbco and x.nivcod = :p_nivcod and rtrim(x.nivdes) = rtrim(:p_nivdes)  -- extracto cias
and x.nivcoda = y.nivcod and x.nivdesa = y.nivdes -- Detalle
and b.ano = :p_azo and b.periodo=:p_per and libro='AC'
and substr(b.objeto,1,1)>=4
group by b.cia, b.ano, b.periodo, b.cat16, b.cat17, b.descripcion

union all

select to_char(b.cia) cia,to_number(b.ano) azo, to_number(b.periodo) cper, trim(to_char(b.cat16)) cat16, trim(to_char(b.cat17)) cat17
,b.descripcion
, 0
, 0 cacumulado, sum(acumulado) aacumulado, 0 pre_acum
from infodb.mvw_libros b, proddta.f0006 e, infodb.relemprlevel1 x, infodb.niveles y
where b.cc=e.mcmcu and e.mcco = x.gbco and x.nivcod = :p_nivcod and rtrim(x.nivdes) = rtrim(:p_nivdes)  -- extracto cias
and x.nivcoda = y.nivcod and x.nivdesa = y.nivdes -- Detalle
and b.ano+1 = :p_azo and b.periodo=:p_per and libro='AC'
and substr(b.objeto,1,1)>=4
group by b.cia, b.ano, b.periodo, b.cat16, b.cat17, b.descripcion

) a,  
prodctl.f0005 c, PRESUPUESTOS.tipsdet d 
where c.drsy = '09' and c.drrt='16' and a.cat16=trim(c.drky) and trim(a.cat17) = d.tdecod 
and (cacumulado <> 0 or aacumulado <> 0 or csaldo <> 0)
and substr(a.descripcion,1,9) = '9060.0101'
;

