select * from one_loads_vw
where naviera='MAERSK'
and cliente='FYFFES'
;

select * 
from ONE_CLIENTE_MV c
;
create or replace view AGROSTG.one_loads_vw
as
with loads as (
select * from one_loads
union all
select * from one_loads_1601
)
select 
c.PAIS_ORIGEN,
c.ZONA_ORIGEN,
c.SEMANA,
c.FECHA_EMPAQUE,
c.CONTENEDOR,
c.MARCHAMO,
c.TERMOGRAFO,
trim(c.CLIENTE) cliente,
trim(c.NAVIERA) naviera,
trim(c.PTO_DESTINO) pto_destino,
c.dia_zarpe,
c.EMPACADORA,
c.PLANTA,
c.TIPO_CAJA,
c.CAJAS,
c.CABEZAL,
c.HORA_LLEGADA,
c.HORA_SALIDA,
c.HOROMETRO,
c.ZONA_DESTINO,
c.ORIGEN_FRUTA,
c.PROVEEDOR,
c.PTO_ORIGEN,
c.TIPO_FRUTA,
c.BOOKING,
c.BARCO_VIAJE,
c.ETA,
c.BL,
c.FACTURA,
c.FECHA_FACTURA,
c.FAC_SHIPPER,
c.DEPEX,
c.POLIZA,
c.ESTADO,
c.issues,
c.COMENTARIO,

cajas/sum(cajas) over (partition by c.fecha_empaque,c.contenedor) loads,
count(*) over (partition by c.fecha_empaque,c.contenedor) repet,
nvl(r2.rate,r.rate) shipping_rate, 
nvl(r2.thc,r.thc) thc, 
nvl(r2.dias_transito,r.dias_transito) dias_transito, 
nvl(r2.dia_zarpe,r.dia_zarpe) dia_zarpe_tarif, 
nvl(r2.dia_arribo,r.dia_arribo) dia_arribo_tarif 
from loads c
left outer join one_ship_rates r 
on (c.pais_origen=r.pais_origen and c.naviera=r.naviera and c.pto_destino=r.pto_destino  
and substr(c.semana,1,4)= r.ano and r.cliente is null)
left outer join one_ship_rates r2
on (c.pais_origen=r2.pais_origen and c.naviera=r2.naviera and c.pto_destino=r2.pto_destino 
and substr(c.semana,1,4)= r2.ano and c.cliente=r2.cliente)
;


select fecha_empaque, contenedor, count(*) from one_loads
group by fecha_empaque, contenedor
having count(*) > 1
;

MWCU6637780