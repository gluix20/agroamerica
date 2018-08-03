select * from AGROSTG.PALM_CONTRATOS;

/*DROP*/
drop table AGROSTG.PALM_CONTRATOS;
/*CREATE*/
CREATE TABLE AGROSTG.PALM_CONTRATOS(
  ANO NUMBER,
	CLIENTE VARCHAR2(100 BYTE), 
  CONTRATO_COD VARCHAR2(100 BYTE),
  TIPO_CONTRATO VARCHAR2(100 BYTE),
  PRODUCTO VARCHAR2(100 BYTE),
  TOTAL_CONTRATO NUMBER,
  DIAS_CREDITO NUMBER,
  OPERACION VARCHAR2(100 BYTE),
  FECHA_CONTRATO DATE,
  REGLA_PRECIO VARCHAR2(100 BYTE)
)
ORGANIZATION EXTERNAL (
  TYPE ORACLE_LOADER
  DEFAULT DIRECTORY google_sheets
  ACCESS PARAMETERS (
    RECORDS DELIMITED BY NEWLINE
    SKIP 1
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
    MISSING FIELD VALUES ARE NULL
    (
      ANO,
      CLIENTE,
      CONTRATO_COD,
      TIPO_CONTRATO,
      PRODUCTO,
      TOTAL_CONTRATO,
      DIAS_CREDITO,
      OPERACION,
      FECHA_CONTRATO DATE "DD/MM/YYYY",
      REGLA_PRECIO
    )
  )
  --LOCATION ('Countries1.txt','Countries2.txt')
  LOCATION ('CONTRATOS.csv')
)
--PARALLEL 5
REJECT LIMIT UNLIMITED;