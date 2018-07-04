CREATE TABLE "AGROSTG"."ONE_COSTOS" 
   (	
   SEMANA NUMBER,  
	"CLIENTE" VARCHAR2(100 BYTE),
	"CONTENEDOR" VARCHAR2(100 BYTE), 
	"BL" VARCHAR2(100 BYTE), 
	"FACTURA" VARCHAR2(100 BYTE),
   "FECHA_FACTURA" DATE,  
   "FECHA_EMPAQUE" DATE, 
	"CAJAS" NUMBER, 
	"TIPO_CAJA" VARCHAR2(100 BYTE), 
	"ORIGEN" VARCHAR2(100 BYTE), 
	"FINCA" VARCHAR2(100 BYTE), 
	"DESTINO" VARCHAR2(100 BYTE), 
  "NAVIERA" VARCHAR2(100 BYTE),
	"PRECIO_NORMAL" NUMBER(7,2), 
	"PRECIO_PAGADO" NUMBER(7,2), 
	"PROVISION" NUMBER(7,2), 
  "FECHA_PAGO" DATE
   )
  TABLESPACE "STAGE" ;