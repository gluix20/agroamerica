drop materialized view "AGROSTG"."STG_FECHA";
exec ETL_SCRIPTS.refresh_now('DIMS','AGROSTG','STG_FECHA','MV');

--150105----NOTE: Actualizar manualmente la dimensión para que carguen los períodos de años siguientes.
--150521----NOTE: Se hicieron cambios de forma y formato al código.
               -- Y se cambiaron todas las referencias de semana y periodo a ano_dole
--150910----NOTE: Se intercambia ano por ano_calendario y ano_dole por ano.
--170103----NOTE: Se corrió exitósamente para 2017.

select * from stg_fecha
order by fecha desc
;

CREATE MATERIALIZED VIEW "AGROSTG"."STG_FECHA"
NOCOMPRESS NOLOGGING TABLESPACE "STAGE" BUILD DEFERRED USING INDEX  REFRESH COMPLETE ON DEMAND USING TRUSTED CONSTRAINTS
AS
with merged as (
select fecha,
      fecha+1 fecha_finca,
      fecha_jde,
      infodb.traesemana@agricultura('A', cdfy, fecha_jde) semana,
      to_number('20'||lpad(cdfy,2,'0')||lpad(to_char(infodb.traesemana@agricultura('A', cdfy, fecha_jde)),2,'0')) semana_cod,
      trunc(infodb.traesemana@agricultura('A', cdfy, fecha_jde) / 2) + 
             mod(infodb.traesemana@agricultura('A', cdfy, fecha_jde),2) pago,
      case        
        when fecha_jde between cddfyj and cdd01j then to_number(to_char('20'||lpad(cdfy,2,'0'))||'01')
        when fecha_jde between cdd01j+1 and cdd02j then to_number(to_char('20'||lpad(cdfy,2,'0'))||'02')
        when fecha_jde between cdd02j+1 and cdd03j then to_number(to_char('20'||lpad(cdfy,2,'0'))||'03')
        when fecha_jde between cdd03j+1 and cdd04j then to_number(to_char('20'||lpad(cdfy,2,'0'))||'04')
        when fecha_jde between cdd04j+1 and cdd05j then to_number(to_char('20'||lpad(cdfy,2,'0'))||'05')
        when fecha_jde between cdd05j+1 and cdd06j then to_number(to_char('20'||lpad(cdfy,2,'0'))||'06')
        when fecha_jde between cdd06j+1 and cdd07j then to_number(to_char('20'||lpad(cdfy,2,'0'))||'07')
        when fecha_jde between cdd07j+1 and cdd08j then to_number(to_char('20'||lpad(cdfy,2,'0'))||'08')
        when fecha_jde between cdd08j+1 and cdd09j then to_number(to_char('20'||lpad(cdfy,2,'0'))||'09')
        when fecha_jde between cdd09j+1 and cdd10j then to_number(to_char('20'||lpad(cdfy,2,'0'))||'10')
        when fecha_jde between cdd10j+1 and cdd11j then to_number(to_char('20'||lpad(cdfy,2,'0'))||'11')
        when fecha_jde between cdd11j+1 and cdd12j then to_number(to_char('20'||lpad(cdfy,2,'0'))||'12')
        when fecha_jde between cdd12j+1 and cdd13j then to_number(to_char('20'||lpad(cdfy,2,'0'))||'13')
        when fecha_jde between cdd13j+1 and cdd14j then to_number(to_char('20'||lpad(cdfy,2,'0'))||'14')
      end periodo_cod,
              mes_num,
              
      case
        when mes_num = 1  then '01 ENERO'
        when mes_num = 2  then '02 FEBRERO'
        when mes_num = 3  then '03 MARZO'
        when mes_num = 4  then '04 ABRIL'
        when mes_num = 5  then '05 MAYO'
        when mes_num = 6  then '06 JUNIO'
        when mes_num = 7  then '07 JULIO'
        when mes_num = 8  then '08 AGOSTO'
        when mes_num = 9  then '09 SEPTIEMBRE'
        when mes_num = 10 then '10 OCTUBRE'
        when mes_num = 11 then '11 NOVIEMBRE'
        when mes_num = 12 then '12 DICIEMBRE'
      end mes,
      mes_cod,
      trimestre_num,
      trimestre_cod,
      ano ano_calendario,
      '20'||lpad(cdfy,2,'0') ano,
      ano_span,
      ano_fecha_ini
    from
      (select day_date fecha,
        to_number(lpad(TO_CHAR(day_date,'YYDDD'),6,'1')) fecha_jde,--OJO Codificada como en JDE!!
        month_of_year mes_num,
        calendar_month_cal_month_code mes_cod,
        quarter_of_year trimestre_num,
        calendar_quart_cal_quarter_co trimestre_cod,
        calendar_year_name ano,
        calendar_year_time_span ano_span
      from stgdim_fecha_tab
      where calendar_year_name >= 2014
      ) f
    join
      ( select cddtpn, cdfy,
        to_date(SUBSTR(cddfyj,2,5),'YYDDD') ano_fecha_ini,
        cddfyj,
        cdd01j, cdd02j, cdd03j, cdd04j, cdd05j, cdd06j, cdd07j, 
        cdd08j, cdd09j, cdd10j, cdd11j, cdd12j, cdd13j, cdd14j
      from proddta.f0008@agricultura
      where cdfy<50
      and cddtpn='A'
      order by 1,2
      ) a
    on (f.fecha_jde between a.cddfyj and a.cdd14j)
)
  select m.*, 
  'PERIODO ' || substr(to_char(m.periodo_cod),5,2) periodo,
  to_number(substr(to_char(m.periodo_cod),5,2)) periodo_num,
  min(m.fecha) over (partition by m.periodo_cod) as periodo_fecha_ini,
  max(m.fecha) over (partition by m.periodo_cod) as periodo_fecha_fin,
  case when ano >= 2016 then 0 else 1 end h_mdoec  
  from merged m
  where periodo_cod is not null
  ;
  
  
  select * from proddta.f0008@agricultura;