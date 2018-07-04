/**PALM_CLIENT_VW**/
select * 
from AGROSTG.PALM_CLIENTE_VW c
;

drop view AGROSTG.PALM_CLIENTE_VW;
CREATE or replace VIEW AGROSTG.PALM_CLIENTE_VW
AS
select distinct cliente
from palm_despachos_vw
;

select * 
from AGROSTG.PALM_PRODUCTO_VW c
;

drop view AGROSTG.PALM_PRODUCTO_VW;
CREATE or replace VIEW AGROSTG.PALM_PRODUCTO_VW
AS
select distinct PRODUCTO
from palm_despachos_vw
;

/**PALM_ORDEN_VW**/
select * 
from AGROSTG.PALM_ORDENES_VW_DIM c
;

--drop view AGROSTG.PALM_ORDENES_VW_DIM;
CREATE or replace VIEW AGROSTG.PALM_ORDENES_VW_DIM
AS
select distinct ano ano_contrato, mes mes_contrato, tipo_contrato, codigo
from palm_ordenes_vw
where codigo is not null
;
select * from palm_ordenes_vw;
