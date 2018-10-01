CREATE OR REPLACE PACKAGE ETL_SCRIPTS AS 

init_time DATE;
cuenta NUMBER;
historico NUMBER;
errores BOOLEAN;
repeat_err BOOLEAN;
size_mb NUMBER;
dim_reset VARCHAR2(1);

  /* TODO enter package declarations (types, exceptions, methods etc) here */ 
  PROCEDURE recreate_mv(esquema VARCHAR2, objeto VARCHAR2);
  
  PROCEDURE refresh_all;
  PROCEDURE refresh_semanal;
  PROCEDURE refresh_dims;
  PROCEDURE reset_seqs;
  PROCEDURE refresh_evolution(tipo VARCHAR2);
  PROCEDURE refresh_prod(tipo VARCHAR2);
  PROCEDURE refresh_matog(tipo VARCHAR2);
  PROCEDURE refresh_mdo(tipo VARCHAR2);
  PROCEDURE refresh_budget(tipo VARCHAR2);
  PROCEDURE refresh_now(proceso VARCHAR2, esquema VARCHAR2, objeto VARCHAR2, tipo VARCHAR2);
  PROCEDURE refresh_repeat(proceso VARCHAR2, esquema VARCHAR2, objeto VARCHAR2, tipo VARCHAR2);
  
  PROCEDURE refresh_dim_actividad_tab;
  PROCEDURE refresh_dim_locacion_tab;
  PROCEDURE refresh_dim_material_tab;
  PROCEDURE refresh_dim_fecha_tab;
  PROCEDURE refresh_dim_labor_tab;
  PROCEDURE refresh_dim_persona_tab;
  PROCEDURE refresh_dim_cuenta_tab;
  
  
  PROCEDURE status_constraints(actual VARCHAR2,accion VARCHAR2);
  
  FUNCTION exec_Time RETURN NUMBER;

END ETL_SCRIPTS;
/


CREATE OR REPLACE PACKAGE BODY ETL_SCRIPTS AS

  --execute immediate 'ALTER SESSION SET NLS_LANGUAGE= 'AMERICAN' NLS_TERRITORY= 'AMERICA' NLS_CURRENCY= '$' NLS_ISO_CURRENCY= 'AMERICA' NLS_NUMERIC_CHARACTERS= '.,' NLS_CALENDAR= 'GREGORIAN' NLS_DATE_FORMAT= 'DD-MON-RR' NLS_DATE_LANGUAGE= 'AMERICAN' NLS_SORT= 'BINARY'';

/***************************
Buscar dim_reset             para resetear las secuencias.
***************************/

  PROCEDURE refresh_all AS
  BEGIN
     --ETL_SCRIPTS.refresh_dims;
     ETL_SCRIPTS.refresh_budget('C');
     ETL_SCRIPTS.refresh_prod('C');
     ETL_SCRIPTS.refresh_matog('C');
     ETL_SCRIPTS.refresh_mdo('C');
     execute immediate 'alter view vw_insert_newcc compile';
  END refresh_all;
  
  PROCEDURE refresh_semanal AS
  BEGIN
    refresh_repeat('DIMS','AGROSTG','STG_NIVEL','MV');
    refresh_repeat('DIMS','AGROSTG','STG_LABOR','MV');
    refresh_repeat('DIMS','AGROSTG','STG_PERSONA','MV');
    refresh_repeat('MDO','AGROSTG','FRO_OBI_PLANILLA_H','MV');
     
  END refresh_semanal;

  PROCEDURE refresh_prod(tipo VARCHAR2) AS
  BEGIN  
    errores := FALSE;
    dbms_output.put_line(to_char(SYSDATE,'DD/MM/YYYY HH:MI:SS')||' Inicia REFRESH_PROD');
    IF tipo='C' THEN
      refresh_now('PROD','AGROSTG','STG_MEDPROD','MV');
      refresh_now('PROD','AGROSTG','STG_HAS','MV');
      refresh_now('PROD','AGROSTG','STG_PRODUCCION','MV');
      refresh_now('PROD','AGROSTG','STG_DESPACHOS','MV');
      
    END IF;
    
    IF NOT errores THEN
      refresh_now('PROD','AGRODW','CUB_PRODUCCION_MV','MV');
      refresh_now('PROD','AGRODW','CUB_HAS_MV','MV');
      refresh_now('PROD','AGRODW','CUB_HAS_SEM_MV','MV');
      refresh_now('PROD','AGRODW','CUB_HAS_PER_MV','MV');
      refresh_now('PROD','AGRODW','CUB_HAS_ANO_MV','MV');
    END IF;    
  END refresh_prod;
  
  PROCEDURE refresh_matog(tipo VARCHAR2) AS
  --Tipo puede ser C: Complete o F: Fast
  BEGIN  
  execute immediate 'alter session set optimizer_index_cost_adj=100';
    errores := FALSE;
    dbms_output.put_line(to_char(SYSDATE,'DD/MM/YYYY HH:MI:SS')||' Inicia REFRESH_MATOG');
    IF tipo='C' THEN
      refresh_now('MATOG','AGROSTG','STG_COSTOS_OG','MV');
      refresh_now('INVENTARIO','AGROSTG','STG_INVENTARIO_MV','MV');
    END IF;
    
    IF NOT errores THEN
      refresh_now('MATOG','AGRODW','CUB_MATOG_MV','MV');
      refresh_now('MATOG','AGRODW','CUB_INVERSION_MV','MV');
      refresh_now('MATOG','AGRODW','CUB_COSTOS_RSM_MV','MV');
      refresh_now('MATOG','AGRODW','CUB_COSTOS_PRR_SEMANAL_MV','MV');      
      refresh_now('INVENTARIO','AGRODW','CUB_INVENTARIO_MV','MV');      
    END IF;
  END refresh_matog;
  
  PROCEDURE refresh_mdo(tipo VARCHAR2) AS
  --Tipo puede ser C: Complete o F: Fast
  BEGIN  
    errores := FALSE;
    historico := 0;
    
    dbms_output.put_line(to_char(SYSDATE,'DD/MM/YYYY HH:MI:SS')||' Inicia REFRESH_MDO');
    IF tipo='C' THEN
    --refresh_now('EVO','AGROSTG','STG_LABOR_EVO','MV');
    --refresh_now('EVO','AGROSTG','STG_DETALLE_JORNALES_EVO','MV');
    
      refresh_now('MDO','AGROSTG','STG_TASA_CAMBIO','MV');
      --refresh_now('MDO','AGROSTG','STG_CONF_CUENTAS_PLANILLA','MV');
      
      refresh_repeat('MDO','AGROSTG','FRO_OBI_PLANILLA','MV');
      refresh_now('MDO','AGROSTG','STG_PLANILLA_GT','MV');
      refresh_now('MDO','AGROSTG','STG_PLANILLA_EC','MV');
      refresh_now('MDO','AGROSTG','STG_MANO_OBRA','MV');
      /*Prorrateo*/
      refresh_now('MDO','AGROSTG','STG_MDO_PRR','MV');
    END IF;
    
    IF NOT errores THEN
    
      execute immediate 'select count(*) from bi_carga_fecha@agricultura
      where proceso=''MDOGT'' and tipo_carga= ''H''' into historico;
      
      refresh_now('MDO','AGRODW','CUB_PLANILLA_GT_MV','MV');
      refresh_now('MDO','AGRODW','CUB_MDO_MV','MV');
      refresh_now('MDO','AGRODW','CUB_COSTOS_RSM_MV','MV');
      refresh_now('MDO','AGRODW','CUB_DESCUENTOS_GT_MV','MV');
      
      IF historico > 0 then 
        refresh_now('MDO','AGRODW','CUB_PLANILLA_GT_H_MV','MV');
        refresh_now('MDO','AGRODW','CUB_MDO_H_MV','MV');
      END IF;
      
    END IF;
  END refresh_mdo;
  
  PROCEDURE refresh_evolution(tipo VARCHAR2) AS
  --Tipo puede ser C: Complete o F: Fast
  BEGIN  
    errores := FALSE;
    historico := 0;
    
    dbms_output.put_line(to_char(SYSDATE,'DD/MM/YYYY HH:MI:SS')||' Inicia REFRESH_MDO');
    IF tipo='C' THEN
      refresh_now('EVO','AGROSTG','STG_LABOR_EVO','MV');
      refresh_now('EVO','AGROSTG','STG_DETALLE_JORNALES_EVO','MV');
    END IF;
    
    IF NOT errores THEN
      null;
    END IF;
  END refresh_evolution;
  
  PROCEDURE refresh_budget(tipo VARCHAR2) AS
  BEGIN  
    errores := FALSE;
    dbms_output.put_line(to_char(SYSDATE,'DD/MM/YYYY HH:MI:SS')||' Inicia REFRESH_BUDGET');
    IF tipo='C' THEN
      refresh_now('BUDGET','AGROSTG','STG_BUDGET','MV');
      refresh_now('BUDGET','AGROSTG','STG_BUD_HAS','MV');
    END IF;
    
    IF NOT errores THEN
      refresh_now('BUDGET','AGRODW','CUB_BUDGET_VAL_MV','MV');
      refresh_now('BUDGET','AGRODW','CUB_BUDGET_RSM_MV','MV');
      refresh_now('BUDGET','AGRODW','CUB_BUDGET_UNI_MV','MV');
      refresh_now('BUDGET','AGRODW','CUB_BUD_HAS_SEM_MV','MV');
      refresh_now('BUDGET','AGRODW','CUB_BUD_HAS_PER_MV','MV');
      refresh_now('BUDGET','AGRODW','CUB_BUD_HAS_ANO_MV','MV');
      
    END IF;
  END refresh_budget;
  
  PROCEDURE recreate_mv(esquema VARCHAR2, objeto VARCHAR2) AS
  query_txt LONG;
  header_txt VARCHAR2(4000);
  ts VARCHAR2(20);
  
  BEGIN
  IF (esquema = 'AGRODW') THEN
    ts := 'DATAWAREHOUSE';
  ELSE
    ts := 'STAGE';
  END IF;
  
  header_txt := 'CREATE MATERIALIZED VIEW '||esquema||'.'||objeto||'
NOCOMPRESS NOLOGGING TABLESPACE '|| ts ||' BUILD DEFERRED USING INDEX REFRESH COMPLETE ON DEMAND 
USING TRUSTED CONSTRAINTS DISABLE QUERY REWRITE
AS ';

execute immediate 'SELECT query FROM DBA_MVIEWS WHERE owner = '''||esquema||''' and mview_name= '''||objeto||'''' into query_txt;
execute immediate 'drop materialized view '||esquema||'.'||objeto;
execute immediate header_txt||query_txt;

  END recreate_mv;
  
  PROCEDURE refresh_now(proceso VARCHAR2, esquema VARCHAR2, objeto VARCHAR2, tipo VARCHAR2) AS
  err_code VARCHAR2(20);
  err_msg VARCHAR2(200);
  compuesto VARCHAR2(100);
  
  BEGIN
    begin
        repeat_err := FALSE;
      init_time := sysdate;
      compuesto := esquema||'.'||objeto;
      
      IF  (tipo != 'DIM') THEN
        execute immediate 'truncate table '||compuesto;            
      END IF;
      
      IF (tipo = 'MV') THEN
        recreate_mv(esquema,objeto);
        dbms_mview.refresh(compuesto,'C', ATOMIC_REFRESH=>FALSE);
        
      ELSIF (tipo in ('CUB','DIM','DIMR')) THEN --DIMR es para cuando se quiere reiniciar la secuencia.
        execute immediate ('begin ETL_SCRIPTS.REFRESH_'||objeto||'; end;');
        
      END IF;
      
      execute immediate 'select count(*) from '||compuesto into cuenta;
      
      execute immediate 'select trunc(bytes/1024/1024) from dba_segments
                          where segment_type = ''TABLE'' 
                          and owner = '''||esquema||'''
                          and segment_name = '''||objeto||'''' into size_mb;
      
      
      
      dbms_output.put_line(to_char(init_time,'DD/MM/YYYY HH:MI:SS')||' '||compuesto||': '||exec_time||' segs. '||
      cuenta||' rows. '||size_mb||' MB.');
      insert into CTRL_SUCCESS (fecha,proceso,esquema,objeto,tiempo,cuenta,fecha_ts,fecha_trunc,size_mb) 
      values (init_time,proceso,esquema,objeto,exec_time,cuenta,systimestamp,trunc(sysdate),size_mb); 
      commit;
      
      --repeat_err := FALSE;
    
    exception
      when others then 
        err_code := SQLCODE();
        err_msg := SUBSTR( SQLERRM(), 1, 200);
        dbms_output.put_line(to_char(init_time,'DD/MM/YYYY HH:MI:SS')||' '||compuesto||': '||SQLERRM());
        insert into CTRL_FAILURE (fecha,proceso,codigo,error,fecha_ts,esquema,objeto,tipo,job) 
        values (init_time,compuesto,err_code,err_msg,systimestamp,esquema,objeto,tipo,'DIARIO'); commit;
        errores := TRUE;
        
        repeat_err := TRUE;
        
    end;
  END refresh_now;

  PROCEDURE refresh_repeat(proceso VARCHAR2, esquema VARCHAR2, objeto VARCHAR2, tipo VARCHAR2) AS
    repeat_count NUMBER := 1;
  BEGIN
    refresh_now(proceso,esquema,objeto,tipo);
    while (repeat_err and repeat_count <= 3)
    loop
        repeat_count := repeat_count + 1;
        refresh_now(proceso,esquema,objeto,tipo);
    end loop;  
    
  END refresh_repeat;

  PROCEDURE refresh_dims AS
  BEGIN
    errores := FALSE;
    --Ingresa los nuevos CCs que vengan de STG_COSTOS_OG
    dbms_output.put_line('**Insert into bi_consolidcc**');
    EXECUTE IMMEDIATE 'alter view vw_insert_newcc compile';
    insert into bi_consolidcc@agricultura (region_cod,ccf,cc,centro_costo)
    select region_cod,ccf,cc,centro_costo from vw_insert_newcc
    ;
    commit;
    
    --Ingresa las nuevas locaciones que vengan de Mano de Obra.
    dbms_output.put_line('**Insert into bi_equivcc**');
    EXECUTE IMMEDIATE 'alter view vw_insert_newloc compile';
    insert into bi_equivcc@agricultura (instancia,cia,locacion_cod,ccf)
    select instancia,cia,locacion_cod,ccf from vw_insert_newloc
    ;
    commit;
    
    refresh_now('DIMS','AGROSTG','STG_LOCACION','MV');
    refresh_now('DIMS','AGROSTG','STG_ACTIVIDAD','MV');
    refresh_now('DIMS','AGROSTG','STG_MATERIAL','MV');    
    refresh_now('DIMS','AGROSTG','STG_CUENTAS_CONTABLES','MV');

    IF NOT errores THEN
    
      dim_reset:=''; --Cambiar a 'R' si se quiere resetear las secuencias.
      
      IF dim_reset = 'R' then
      
        reset_seqs;
      
      END IF;
      
      --status_constraints('ENABLED', 'DISABLE');--Porque los cubos son MVs y no tienen constraints.

      refresh_now('DIMS','AGRODW','DIM_LOCACION_TAB','DIM'||dim_reset);
      refresh_now('DIMS','AGRODW','DIM_ACTIVIDAD_TAB','DIM'||dim_reset);
      refresh_now('DIMS','AGRODW','DIM_MATERIAL_TAB','DIM'||dim_reset);
      refresh_now('DIMS','AGRODW','DIM_FECHA_TAB','DIM'||dim_reset);
      refresh_now('DIMS','AGRODW','DIM_LABOR_TAB','DIM'||dim_reset);
      refresh_now('DIMS','AGRODW','DIM_PERSONA_TAB','DIM'||dim_reset);      
      refresh_now('DIMS','AGRODW','DIM_CUENTA_TAB','DIM'||dim_reset);
      
      --status_constraints('DISABLED', 'ENABLE');--Porque los cubos son MVs y no tienen constraints.
      
    end if;

    refresh_now('DIMS','AGROSTG','STG_OBIACTIVIDA','MV');
    
    insert into ctrl_carga_ordby (proceso,esquema,objeto)
    select proceso,esquema,objeto from vw_insert_newobj
    ;
    commit;
    
    
  END refresh_dims;

  

  PROCEDURE refresh_dim_actividad_tab
  AS 
  BEGIN

  MERGE /*+ APPEND PARALLEL ("DIM") */
  INTO agrodw."DIM_ACTIVIDAD_TAB"  dim
  USING ( 
        SELECT * FROM "STG_ACTIVIDAD" 
        ) stg
  ON ( "DIM"."ACTIVIDAD_COD" = "STG"."ACTIVIDAD_COD" )
    
    WHEN MATCHED THEN
      UPDATE
      SET
      "ACTIVIDAD" = stg."ACTIVIDAD", 
      "PROCESO" = stg."PROCESO",
      "PROCESO_ORDBY" = stg."PROCESO_ORDBY",
      "MACRO" = stg."MACRO",
      "MACRO_ORDBY" = stg."MACRO_ORDBY",
      actividad_rep_sem = stg.actividad_rep_sem,
      actividad_rep_sem_ordby = stg.actividad_rep_sem_ordby,
      proceso_jde = stg.proceso_jde,
      rep_sem = stg.rep_sem
         
    WHEN NOT MATCHED THEN
      INSERT
        (dim."DK", dim."ACTIVIDAD_COD", dim."ACTIVIDAD", dim."PROCESO", dim."PROCESO_ORDBY",dim."MACRO",dim."MACRO_ORDBY",
        dim."DIMENSION_KEY", dim.actividad_rep_sem, dim.actividad_rep_sem_ordby, dim.proceso_jde, dim.rep_sem)
      VALUES
        (agrodw."DIM_ACTIVIDAD_SEQ".NEXTVAL, stg."ACTIVIDAD_COD", stg."ACTIVIDAD", 
        stg."PROCESO", stg."PROCESO_ORDBY", stg."MACRO", stg."MACRO_ORDBY", agrodw."DIM_ACTIVIDAD_SEQ".CURRVAL,
        stg.actividad_rep_sem, stg.actividad_rep_sem_ordby, stg.proceso_jde, stg.rep_sem)
    ;
    commit;
  END refresh_dim_actividad_tab;
  
  PROCEDURE refresh_dim_locacion_tab AS 
  BEGIN
  MERGE /*+ APPEND PARALLEL (dim) */
  INTO agrodw.dim_locacion_tab dim
  USING ( select * from stg_locacion ) stg
  ON ( dim.cc = stg.cc and dim.fecha_ini = stg.fecha_ini )
    
    WHEN MATCHED THEN
      UPDATE
      SET
      locacion = stg.locacion, distrito_cod = stg.distrito_cod,
      "DISTRITO" = stg."DISTRITO",            "REGION_COD" = stg."REGION_COD",
      "REGION" = stg."REGION",                "CIA" = stg."CIA", 
      "LOCACION_COD" = stg."LOCACION_COD",    "CENTRO_COSTO" = stg."CENTRO_COSTO",
      "INVERSION" = stg."INVERSION",          "NEGOCIO" = stg."NEGOCIO",
      nivel = stg.nivel, fecha_fin = stg.fecha_fin,
      estado = stg.estado, pais = stg.pais, 
      grupo = stg.grupo, comercializadora = stg.comercializadora,
      cia_nombre = stg.cia_nombre, tipo_cc = stg.tipo_cc, 
      cc_nombre = stg.cc_nombre
           
    WHEN NOT MATCHED THEN
      INSERT
        (dim."DK", dim."LOCACION", dim."DISTRITO_COD", dim."DISTRITO", dim."REGION_COD",
        dim."REGION", dim."CIA", dim."LOCACION_COD", dim."CENTRO_COSTO", dim."INVERSION",
        dim."NEGOCIO", dim."CC", dim."NIVEL", dim."DIMENSION_KEY", dim.fecha_ini, dim.fecha_fin,
        dim.estado, dim.pais, dim.grupo, dim.comercializadora, dim.cia_nombre, dim.tipo_cc,
        dim.cc_nombre)
      VALUES
        (agrodw."DIM_LOCACION_SEQ".NEXTVAL,
        stg."LOCACION", stg."DISTRITO_COD", stg."DISTRITO", stg."REGION_COD", stg."REGION",
        stg."CIA", stg."LOCACION_COD", stg."CENTRO_COSTO", stg."INVERSION", stg."NEGOCIO",
        stg."CC", stg."NIVEL", agrodw."DIM_LOCACION_SEQ".CURRVAL, stg.fecha_ini, stg.fecha_fin,
        stg.estado, stg.pais, stg.grupo, stg.comercializadora, stg.cia_nombre, stg.tipo_cc,
        stg.cc_nombre)
    ;
    commit;
  END refresh_dim_locacion_tab;

  PROCEDURE refresh_dim_material_tab
  AS 
  BEGIN
    MERGE /*+ APPEND PARALLEL (dim) */
    INTO agrodw."DIM_MATERIAL_TAB" dim
    USING
      ( SELECT * FROM "STG_MATERIAL" ) stg
    ON ( dim."MATERIAL_JDE" = stg."MATERIAL_JDE" )
    
    WHEN MATCHED THEN
      UPDATE
      SET
    "MATERIAL_COD" = stg."MATERIAL_COD", "MATERIAL" = stg."MATERIAL",
    "MATERIAL_CYD" = stg."MATERIAL_CYD", "GRUPO_MAT" = stg.grupo_mat,
    "UM_PRINCIPAL" = stg.um_principal
         
    WHEN NOT MATCHED THEN
      INSERT
        (dim."DK", dim."MATERIAL_JDE", dim."MATERIAL_COD", dim."MATERIAL",
        dim."MATERIAL_CYD", dim."DIMENSION_KEY", dim.grupo_mat, dim.um_principal)
      VALUES
        (agrodw."DIM_MATERIAL_SEQ".NEXTVAL, stg."MATERIAL_JDE", stg."MATERIAL_COD",
        stg."MATERIAL", stg."MATERIAL_CYD", agrodw."DIM_MATERIAL_SEQ".CURRVAL, 
        stg.grupo_mat, stg.um_principal)
    ;
    commit;
  END refresh_dim_material_tab;
  
  PROCEDURE refresh_dim_labor_tab
  AS 
  BEGIN
    MERGE /*+ APPEND PARALLEL (dim) */
    INTO agrodw."DIM_LABOR_TAB"  dim
    USING
      ( SELECT * FROM "STG_LABOR" ) stg
    ON ( dim.instancia = stg.instancia
    and dim.nomec = stg.nomec
    and dim.aplic = stg.aplic
    and dim.clave = stg.clave
    and dim.labor_join = stg.labor_join
    )
    
    WHEN MATCHED THEN
      UPDATE
      SET
      dim.labor = stg.labor,
    dim.unidad_medida = stg.unidad_medida,
    dim.labor_cyd = stg.labor_cyd,
    dim.id_actividad = stg.id_actividad,
    dim.actividad_pla = stg.actividad_pla
         
    WHEN NOT MATCHED THEN
      INSERT
        (dim.dk, dim.instancia, dim.nomec, dim.aplic,
        dim.clave, dim.labor,dim.unidad_medida,dim.dimension_key,dim.labor_cyd,
        dim.id_actividad,dim.actividad_pla,dim.labor_join)
      VALUES
        (agrodw.dim_labor_seq.NEXTVAL, stg.instancia, stg.nomec, stg.aplic,
        stg.clave, stg.labor,stg.unidad_medida, agrodw.dim_labor_seq.CURRVAL,stg.labor_cyd,
        stg.id_actividad,stg.actividad_pla,stg.labor_join)
    ;
    commit;
  END refresh_dim_labor_tab;
  
  PROCEDURE refresh_dim_persona_tab
  AS 
  BEGIN
    MERGE /*+ APPEND PARALLEL (dim) */
    INTO agrodw.DIM_persona_TAB  dim
    USING
      ( SELECT * FROM STG_persona ) stg
    ON ( dim.instancia = stg.instancia
    and dim.cia = stg.cia
    and dim.codigo = stg.codigo
    )
    
    WHEN MATCHED THEN
      UPDATE
      SET
    dim.persona = stg.persona
         
    WHEN NOT MATCHED THEN
      INSERT
        (dim.dk, dim.instancia, dim.cia,
        dim.codigo, dim.persona,dim.dimension_key)
      VALUES
        (agrodw.dim_persona_seq.NEXTVAL, stg.instancia, stg.cia,
        stg.codigo, stg.persona, agrodw.dim_persona_seq.CURRVAL)
    ;
    commit;
  END refresh_dim_persona_tab;
  
  PROCEDURE refresh_dim_cuenta_tab
  AS 
  BEGIN
    MERGE /*+ APPEND PARALLEL (dim) */
    INTO agrodw.DIM_cuenta_TAB  dim
    USING
      ( SELECT * FROM STG_cuentas_contables ) stg
    ON ( dim.cuenta = stg.cuenta
    )
    
    WHEN MATCHED THEN
      UPDATE
      SET
    
dim.cia=stg.cia,
dim.aid=stg.aid,
dim.cc=stg.cc,
dim.obj=stg.obj,
dim.sub=stg.sub,
dim.descripcion=stg.descripcion,
dim.obj_desc=stg.obj_desc,
dim.actividad_cod=stg.actividad_cod,
dim.actividad=stg.actividad,
dim.proceso=stg.proceso,
dim.cat17=stg.cat17,
dim.cat17_desc=stg.cat17_desc,
dim.cat18=stg.cat18,
dim.cat18_desc=stg.cat18_desc,
dim.cat19=stg.cat19,
dim.cat20=stg.cat20
         
    WHEN NOT MATCHED THEN
      INSERT
        (dim.dk,dim.cia,dim.aid,dim.cuenta,dim.cc,
          dim.obj,dim.sub,dim.descripcion,dim.obj_desc,dim.actividad_cod,
          dim.actividad,dim.proceso,dim.cat17,dim.cat17_desc,dim.cat18,
          dim.cat18_desc,dim.cat19,dim.cat20,dim.dimension_key)
      VALUES
        (agrodw.dim_cuenta_seq.NEXTVAL, stg.cia,stg.aid,stg.cuenta,stg.cc,
          stg.obj,stg.sub,stg.descripcion,stg.obj_desc,stg.actividad_cod,
          stg.actividad,stg.proceso,stg.cat17,stg.cat17_desc,stg.cat18,
          stg.cat18_desc,stg.cat19,stg.cat20, agrodw.dim_cuenta_seq.CURRVAL)
    ;
    commit;
  END refresh_dim_cuenta_tab;
  
  PROCEDURE refresh_dim_fecha_tab
    AS 
    BEGIN
    
    MERGE 
    /*+ APPEND PARALLEL ("DIM") */
    INTO   agrodw."DIM_FECHA_TAB"  "DIM"
    USING (
    select * from "AGROSTG"."STG_FECHA"
      WHERE NOT "STG_FECHA"."FECHA" IS NULL
      ) "STG"
  ON ( "DIM"."FECHA" = "STG"."FECHA" )
    WHEN MATCHED THEN
    UPDATE
    SET "FECHA_JDE" = "STG"."FECHA_JDE", "SEMANA_COD" = "STG"."SEMANA_COD", "SEMANA" = "STG"."SEMANA",
    "PERIODO_COD" = "STG"."PERIODO_COD", "PERIODO" = "STG"."PERIODO", "MES_COD" = "STG"."MES_COD",
    "MES_NUM" = "STG"."MES_NUM", "MES" = "STG"."MES", "TRIMESTRE_COD" = "STG"."TRIMESTRE_COD",
    "TRIMESTRE_NUM" = "STG"."TRIMESTRE_NUM", "ANO" = "STG".ano, "ANO_SPAN" = "STG"."ANO_SPAN",
    "ANO_FECHA_INI" = "STG"."ANO_FECHA_INI", "ANO_CALENDARIO" = "STG".ano_calendario, 
    FECHA_FINCA = STG.fecha_finca, pago=stg.pago, periodo_num=stg.periodo_num
       
  WHEN NOT MATCHED THEN
    INSERT
      ("DIM"."FECHA", "DIM"."FECHA_JDE", "DIM"."SEMANA_COD", "DIM"."SEMANA", "DIM"."PERIODO_COD",
      "DIM"."PERIODO", "DIM"."MES_COD", "DIM"."MES_NUM", "DIM"."MES", "DIM"."TRIMESTRE_COD",
      "DIM"."TRIMESTRE_NUM", "DIM"."ANO", "DIM"."ANO_SPAN", "DIM"."ANO_FECHA_INI", "DIM"."ANO_CALENDARIO",
      DIM.fecha_finca, dim.pago, dim.periodo_num)
    VALUES
      ("STG"."FECHA", "STG"."FECHA_JDE", "STG"."SEMANA_COD", "STG"."SEMANA", "STG"."PERIODO_COD",
      "STG"."PERIODO", "STG"."MES_COD", "STG"."MES_NUM", "STG"."MES", "STG"."TRIMESTRE_COD",
      "STG"."TRIMESTRE_NUM", "STG".ano, "STG"."ANO_SPAN", "STG"."ANO_FECHA_INI", "STG".ano_calendario,
      STG.fecha_finca, stg.pago, stg.periodo_num)
  ;
  
  commit;
  END refresh_dim_fecha_tab;

FUNCTION Exec_Time RETURN NUMBER
  IS
  BEGIN
    RETURN round((SYSDATE-init_time)*24*60*60,2);
  END Exec_Time;

  PROCEDURE status_constraints(actual VARCHAR2, accion VARCHAR2) AS
  BEGIN
    begin
      for i IN (select table_name, constraint_name --disable first the foreign key
      from user_constraints
      where constraint_type ='R'
      and status = actual
      and owner = 'AGRODW')
      loop
      EXECUTE IMMEDIATE 'alter table agrodw.' ||i.table_name|| ' '||accion||' constraint ' ||i.constraint_name;
      end loop i;
    end;
  END status_constraints;

  PROCEDURE reset_seqs AS
  s_val number;
  BEGIN
    execute immediate 'alter session set current_schema= agrodw';
    FOR s IN
        (SELECT s.SEQUENCE_NAME FROM dba_SEQUENCES s where s.sequence_owner='AGRODW')
    LOOP
        execute immediate 'select agrodw.' || s.SEQUENCE_NAME || '.nextval from dual' INTO s_val;
        execute immediate 'alter sequence agrodw.' || s.SEQUENCE_NAME || ' increment by -' || s_val || ' minvalue 0';
        execute immediate 'select agrodw.' || s.SEQUENCE_NAME || '.nextval from dual' INTO s_val;
        execute immediate 'alter sequence agrodw.' || s.SEQUENCE_NAME || ' increment by 1 minvalue 0';
    END LOOP;
    execute immediate 'alter session set current_schema= agrostg';
  END reset_seqs;




END ETL_SCRIPTS;
/
