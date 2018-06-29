drop materialized view STG_NIVEL;
exec ETL_SCRIPTS.refresh_now('DIMS','AGROSTG','STG_NIVEL','MV');

select * from STG_NIVEL
;


select * from locacion@frontera;

 CREATE MATERIALIZED VIEW "AGROSTG"."STG_NIVEL"
 NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED USING INDEX REFRESH COMPLETE ON DEMAND
 AS
    select trim(g.nivcod) ||'-'|| trim(to_char(g.nivdes)) nivel, g.gbco cia 
    from infodb.relemprlevel1@agricultura g
    join bi_carga_cia@agricultura bcc on (g.nivcod = bcc.nivcod and g.nivdes = bcc.nivdes)
    order by 1
;