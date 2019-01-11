
desc obi_costos_og;

select * from obi_costos_og
where tipo_oper='INGRESO'
;

CREATE OR REPLACE FORCE VIEW "AGRICULTURA"."OBI_COSTOS_OG"
AS
with cias as (
select trim(g.gbco) gbco
, trim(g.nivcod) nivcod
, trim(g.nivdes) nivdes
, bcc.negocio, bcc.pais from infodb.relemprlevel1 g
join bi_carga_cia bcc on (g.nivcod=bcc.nivcod and g.nivdes=bcc.nivdes)
)

select /*+ ORDERED PARALLEL(8)*/
nvl(ci.negocio,'BANANO') negocio
, trim(to_char(glkco)) cia
, trim(to_char(gldct)) tipo
, trim(to_char(gldoc)) nodocto
, trim(to_char(glicu)) batch
, to_date(substr(gldgj,2,5),'YYDDD') fecha
, to_date(substr(glupmj,2,5),'YYDDD') fecha_modificacion
, to_char(a.glmcu) cc
, lpad(to_char(decode(e.gmr016,'116','164','585','586',e.gmr016)),10,' ') actividad_cod
, trim(to_char(glani)) cuenta
, nvl(trim(to_char(ilitm)),'-ND-') material_jde
, nvl(to_char(b.ilmcu),'-ND-') bodega
, decode(ilitm,null,to_char(trim(glexa)),'BODEGA') proveedor
, to_char(trim(glexa) || ' - ' ||trim(glexr)) descripcion
, cast(case
    when trim(gljbcd) is not null then 'PLA' --Planilla Fincas--Esto no va a ningun cubo porque ya se trae directamente del modulo
    when substr(trim(e.gmr017),1,2) in ('LC')-- or substr(trim(e.gmr017),1,3) in ('FRP','FSP','SAS','BEE','FGV','FSI','FAP','FMV','FTO') 
    then 'CDP' --Contadirecta Planilla
    when ilitm is not null then 'MAT' --Materiales del modulo de inventarios
  else 'CDM' end as varchar2(3)) tipo_costo --Contadirecta Inventario y Otros Gastos
, 'OP' tipo_registro
, nvl(ci.nivdes,'NO DEFINIDO') nivel
, case when substr(trim(e.gmobj),1,2) in ('15') then 'INVERSION'
    when substr(trim(e.gmobj),1,1) in ('1') then 'ACTIVO'
    when substr(trim(e.gmobj),1,1) in ('2') then 'PASIVO'
    when substr(trim(e.gmobj),1,1) in ('3') then 'CAPITAL'
    when substr(trim(e.gmobj),1,1) in ('4','8') then 'INGRESO'
    else 'COSTO' end tipo_oper
, case when glcrr != 0 and glcrr is not null then 1/glcrr 
    else 0 end tasa_cambio
, sum(nvl(iltrqt,0)/1000000) cantidad
, sum(glaa/100) valor
from proddta.f0911 a
left outer join proddta.f4111 b on (b.ilkco = a.glkco AND b.ildoc = a.gldoc AND b.ildct = a.gldct AND b.iljeln = a.gljeln - 1 AND b.ilitm = a.glitm AND b.ildgl = a.gldgj)  
join proddta.f0901 e ON (a.glaid = e.gmaid)-- and ( to_number(substr(trim(e.gmobj),1,1)) >= 4 or to_number(substr(trim(e.gmobj),1,2)) = 15 ))
join proddta.f0010 f ON (e.gmco = f.ccco)--PARA OBTENER EL LIBRO EN DOLARES.
  left outer join cias ci on (a.glco=ci.gbco)
  
  where gldgj >= 116003 --Mayor o igual a 2016
  and a.gllt = DECODE(f.cccrcd,'USD','AA','XA') --Si la empresa esta en dolares emparejar con AA sino con XA (00070 esta en GTQ entonces busca en XA)  
  and glpost <> ' ' --SI ESTA VACIO ES QUE NO SE HA POSTEADO EN EL BALANCE DE SALDOS (MAYOR)
  
  
group by 
nvl(ci.negocio,'BANANO')
, trim(to_char(glkco)),
    trim(to_char(gldct)),
    trim(to_char(gldoc)),
    trim(to_char(glicu)),
    to_date(substr(gldgj,2,5),'YYDDD')
    , to_date(substr(glupmj,2,5),'YYDDD')
    , to_char(a.glmcu),
    lpad(to_char(decode(e.gmr016,'116','164','585','586',e.gmr016)),10,' '),
    trim(to_char(glani)),
    
    nvl(trim(to_char(ilitm)),'-ND-'),
    nvl(to_char(b.ilmcu),'-ND-'),
    decode(ilitm,null,to_char(trim(glexa)),'BODEGA'),
    to_char(trim(glexa) || ' - ' ||trim(glexr)),
    
    cast(case
    when trim(gljbcd) is not null then 'PLA' --Planilla Fincas--Esto no va a ningun cubo porque ya se trae directamente del modulo
    when substr(trim(e.gmr017),1,2) in ('LC')-- or substr(trim(e.gmr017),1,3) in ('FRP','FSP','SAS','BEE','FGV','FSI','FAP','FMV','FTO') 
    then 'CDP' --Contadirecta Planilla
    when ilitm is not null then 'MAT' --Materiales del modulo de inventarios
    else 'CDM' end as varchar2(3)), --Contadirecta Inventario y Otros Gastos
    'OP' /*tipo_registro*/
, nvl(ci.nivdes,'NO DEFINIDO') /*Nivel*/
, case when substr(trim(e.gmobj),1,2) in ('15') then 'INVERSION'
    when substr(trim(e.gmobj),1,1) in ('1') then 'ACTIVO'
    when substr(trim(e.gmobj),1,1) in ('2') then 'PASIVO'
    when substr(trim(e.gmobj),1,1) in ('3') then 'CAPITAL'
    when substr(trim(e.gmobj),1,1) in ('4','8') then 'INGRESO'
    else 'COSTO' end
, case when glcrr != 0 and glcrr is not null then 1/glcrr else 0 end
;

select * from proddta.f0911;

select * from proddta.f0010;