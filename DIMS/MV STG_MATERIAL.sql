drop materialized view STG_MATERIAL;
exec ETL_SCRIPTS.refresh_now('DIMS','AGROSTG','STG_MATERIAL','MV');

--150507----NOTE: Se le agrega Trim a MATERIAL_COD.

select * from stg_material;

CREATE MATERIALIZED VIEW AGROSTG.STG_MATERIAL
  NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD IMMEDIATE
  REFRESH COMPLETE ON DEMAND USING ENFORCED CONSTRAINTS
  AS 
  with activos as (
    select distinct glitm
    from proddta.f0911@agricultura a
    where gldgj >= 114001
  )  
select 
to_char(imitm) material_jde, 
trim(to_char(imlitm)) material_cod, 
trim(to_char(imdsc1)) material, 
to_char(trim(imlitm)||' '||imdsc1) material_cyd,
nvl(trim(to_char(m.imsrtx)),'NO DEFINIDO') grupo_mat,
nvl(trim(to_char(m.imuom1)),'-ND-') um_principal --Puede ser diferente a la de la transacción.
from proddta.f4101@agricultura m
join activos a on (m.imitm = a.glitm)
union all
select '-ND-', '-ND-', 'CONTA DIRECTA', 'CONTA DIRECTA', 'CONTA DIRECTA', '-ND-' from dual;

select * from proddta.f4101
where upper(imsrtx) like '%FISICA%'
;

select distinct glitm
from proddta.f0911@agricultura a
where gldgj >= 114001
;