drop view AGROSTG.STG_MATOG_PRR_VW;

--150320----NOTE: Se le cambia el nombre al archivo para que cumpla con el nuevo estandar y se traslada a BACKUP el historial.
--150320----NOTE: Se agrega bodega.
--150728----NOTE: Se agrega obj_desc y proveedor para conta directa.
--150903----NOTE:*** Se debe agregar los nuevos campos que vienen de STG_COSTOS_OG.
--150909----NOTE: Se agregan los FILTROS SOLICITADOS.
--150302----NOTE: Se agrega prorrateo para PLANTA DE BENEFICIO.
--160304----NOTE: Se agrega campo prorrateo, y se cambia la clasificacion de prorrateo. 
               -- Se quitan los FILTROS_SOLICITADOS para homologar la info con JDE.
--160306----NOTE: Se introduce el concepto de _apr: ANTES DE PRORRATEAR.
--160412----NOTE: Se cambia f.ano a ano_pr porque había ambiguedad con el ano que viene en stg_costos.

desc stg_matog_prr_vw;
select count(*) from agrostg.stg_matog_prr_vw

;


CREATE OR REPLACE FORCE VIEW AGROSTG.STG_MATOG_PRR_VW
AS
with mog as
( select c.*,
l.region_cod,l.distrito, f.ano ano_pr, f.semana_cod, a.macro
from agrostg.stg_costos_og c
join agrostg.stg_locacion l on (c.cc=l.cc and c.fecha between l.fecha_ini and l.fecha_fin)
join agrostg.stg_fecha f on (c.fecha=f.fecha)
join agrostg.stg_actividad a on (c.actividad_cod=a.actividad_cod)
)

select 
c.negocio, c.cia, c.tipo, c.nodocto, c.batch, c.fecha, 
c.actividad_cod, c.cuenta, c.material_jde, c.bodega,
c.cc cc_apr,  
c.proveedor, c.descripcion, c.tipo_costo, c.tipo_registro,
c.nivel, c.tipo_oper, c.tasa_cambio, 
c.cc_pr cc, c.prorrateo,
c.cantidad_pr cantidad, c.valor_pr valor 
from (

select 
c.*, c.cc cc_pr, 0 prorrateo,
c.valor valor_pr, c.cantidad cantidad_pr
from mog c
where 
case 
when c.distrito in ('COSTOS FIJOS','PLANTA DE BENEFICIO') and c.macro not in ('OVERHEAD') then 0
when c.macro in ('OVERHEAD') then 0
else 1 end = 1

    UNION ALL

select 
c.*, f.cc cc_pr, 1 prorrateo,
c.valor*f.factor valor_pr, c.cantidad*f.factor cantidad_pr
from mog c
join stg_prorrateo_fac_vw f on (c.region_cod=f.region_cod and c.ano_pr=f.ano and c.semana_cod=f.semana_cod)
where c.distrito in ('COSTOS FIJOS','PLANTA DE BENEFICIO') and c.macro not in ('OVERHEAD')

    UNION ALL

select 
c.*, f.cc cc_pr, 1 prorrateo,
c.valor*f.factor_nivel valor_pr, c.cantidad*f.factor_nivel cantidad_pr
from mog c
join stg_prorrateo_fac_vw f on (c.nivel=f.nivel and c.ano_pr=f.ano and c.semana_cod=f.semana_cod)
where c.macro in ('OVERHEAD')
) c
;



