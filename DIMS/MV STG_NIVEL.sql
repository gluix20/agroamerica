drop materialized view STG_NIVEL;
exec ETL_SCRIPTS.refresh_now('DIMS','AGROSTG','STG_NIVEL','MV');

select * from STG_NIVEL
order by 2,3,4
;


select * from locacion@frontera;

 CREATE MATERIALIZED VIEW "AGROSTG"."STG_NIVEL"
 NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED USING INDEX REFRESH COMPLETE ON DEMAND
 AS
    select trim(g.nivcod) ||'-'|| trim(to_char(g.nivdes)) nivel
    , trim(g.nivcod) nivcod
    , trim(to_char(g.nivdes)) nivdes
    , trim(g.gbco) cia
    , case when bcc.nivcod is not null then 1
        else 0 end allowed
    from infodb.relemprlevel1@agricultura g
    left outer join bi_carga_cia@agricultura bcc on (g.nivcod = bcc.nivcod and g.nivdes = bcc.nivdes)
    order by 1
;

select * from bi_carga_cia@agricultura;

select * from infodb.relemprlevel1@agricultura g
where gbco = '00910'
order by 3,1
;