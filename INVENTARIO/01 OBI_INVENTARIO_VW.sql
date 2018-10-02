--ilan8 códigode proveedor. join f0101 abban8
--ildoco que guarda el numero de orden de compra
--ildcto que guarda el tipo de orden
--ilkco que guarda el numero de compania.
--ilitm
--illitm
--LLAVE: ilkco, ildcto, ildoco, ilitm
--iltrum: unidad de medida
--iltrqt: cantidad por unidad /1000000
--iluncs: costo unitario por cada unidad /10000
--ilpaid: costo de la transacción total /100
--ilmcu: bodega

select * from obi_inventario_vw;
--drop view OBI_INVENTARIO_VW;
CREATE OR REPLACE FORCE VIEW "AGRICULTURA"."OBI_INVENTARIO_VW"
AS 
select ilkco cia
, lpad(to_char(to_number(ilkco)),12,' ') cc
, ildct tipo_documento
, ildcto tipo_orden
, ildoco orden, 
to_date(SUBSTR(ildgl,2,5),'YYDDD') fecha,
case when ildct = 'OV' then 'INGRESO' else 'OTRO' end tipo,
ilmcu bodega, to_char(trim(ilitm)) material_jde, iltrum unidad_medida, max(abalph) proveedor,--porque en los ajustes por redondeo el proveedor es null.
sum(iltrqt)/1000000 cantidad, sum(iluncs)/10000 valor_unitario, sum(ilpaid)/100 valor
from proddta.f4111 i
left outer join proddta.f4101 m ON (i.ilitm = m.imitm)
left outer join proddta.f0101 p ON (i.ilan8 = p.aban8)
left outer join infodb.relemprlevel1 g on (i.ilkco=g.gbco)
join bi_carga_cia bcc on (g.nivcod=bcc.nivcod and g.nivdes=bcc.nivdes)
where ildgl >= 116003 --Mayor o igual a 2016
--and ildoco=1854 and ilkco='00390'
group by ilkco, ildct, ildcto, ildoco, 
ilmcu, ilitm, iltrum, ildgl
;

select *
from proddta.f4111 i
left outer join proddta.f4101 m ON (i.ilitm = m.imitm)
left outer join proddta.f0101 p ON (i.ilan8 = p.aban8)
--where ildct= 'OV'
--and ildoco=1854 and ilkco='00390'
;

select * from proddta.f0101;