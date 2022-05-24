CREATE TABLE "TableroSistemaPenal_4"(
  "AGRUPADOR" varchar(55) NOT NULL,
  "AÑO CAUSA" smallint NOT NULL,
  "CAUTELAR" decimal(1, 0),
  "COD_CTE" smallint NOT NULL,
  "COD_MATERIA" smallint NOT NULL,
  "COD_TRIB" smallint NOT NULL,
  "CORTE" varchar(17) NOT NULL,
  "DELITO" varchar(64),
  "FECHA AUDIENCIA" timestamp NOT NULL,
  "ID_EVENTO" integer NOT NULL,
  "ILEGALIDAD_DETENCION" decimal(1, 0),
  "Number of Records" smallint NOT NULL,
  "PAÍS" varchar(5) NOT NULL,
  "PRISION_PREVENTIVA" decimal(4, 0),
  "PRISION_PREVENTIVA_DECRETADA" decimal(4, 0),
  "PRISION_PREVENTIVA_RECHAZADA" decimal(4, 0),
  "RIT" smallint NOT NULL,
  "Rechaza Prisión Preventiva (copia)" varchar(27),
  "SENTENCIA" decimal(1, 0),
  "TIPO" smallint NOT NULL,
  "TRIBUNAL" varchar(30) NOT NULL
);

CREATE TABLE "TableroSistemaPenal_6"(
  "AUDIENCIA" varchar(52) NOT NULL,
  "AÑO CAUSA" smallint NOT NULL,
  "COD_MATERIA" smallint NOT NULL,
  "COD_TRIB" smallint NOT NULL,
  "CORTE" varchar(24) NOT NULL,
  "DELITO" varchar(64),
  "FECHA AUDIENCIA" timestamp NOT NULL,
  "Number of Records" smallint NOT NULL,
  "PAÍS" varchar(5) NOT NULL,
  "RIT" smallint NOT NULL,
  "TIP TRIB" varchar(9) NOT NULL,
  "TIPO" smallint NOT NULL,
  "TRIBUNAL" varchar(51) NOT NULL
);

CREATE TABLE "TableroSistemaPenal_1"(
  "AGRUPADOR" varchar(55),
  "Año Ingreso" varchar(4) NOT NULL,
  "CLASE" varchar(13) NOT NULL,
  "COD DELITO" double NOT NULL,
  "COD. REGIÓN" smallint NOT NULL,
  "COMUNA" varchar(19) NOT NULL,
  "CORTE" varchar(17) NOT NULL,
  "CREA RUC" varchar(9) NOT NULL,
  "Calculation_0520821013255406" varchar(55),
  "Calculation_0640821194248948" varchar(55),
  "Calculation_7430825124450529" decimal(4, 0) NOT NULL,
  "Cod. Tribunal" smallint NOT NULL,
  "DELITO" varchar(64),
  "Forma Inicio" varchar(25) NOT NULL,
  "INGRESOS" decimal(4, 0) NOT NULL,
  "Ingresos _ Garantia (copy)" decimal(3, 0) NOT NULL,
  "Jurisdicción" varchar(9) NOT NULL,
  "LATITUD" varchar(19) NOT NULL,
  "LONGITUD" varchar(19) NOT NULL,
  "Mes Ingreso" varchar(7) NOT NULL,
  "Number of Records" smallint NOT NULL,
  "PAÍS" varchar(5) NOT NULL,
  "REGIÓN" varchar(53) NOT NULL,
  "RPA" varchar(3),
  "TIPO" varchar(9) NOT NULL,
  "TIP_TRIB" smallint NOT NULL,
  "TRIBUNAL" varchar(30) NOT NULL
);

CREATE TABLE "TableroSistemaPenal_3"(
  "AGRUPADOR" varchar(55) NOT NULL,
  "AÑO CAUSA" smallint NOT NULL,
  "CAUTELAR" decimal(1, 0),
  "COD_CTE" smallint NOT NULL,
  "COD_MATERIA" smallint NOT NULL,
  "COD_TRIB" smallint NOT NULL,
  "CORTE" varchar(17) NOT NULL,
  "DELITO" varchar(64),
  "FECHA AUDIENCIA" timestamp NOT NULL,
  "ID_EVENTO" integer NOT NULL,
  "ILEGALIDAD_DETENCION" decimal(1, 0),
  "Number of Records" smallint NOT NULL,
  "PAÍS" varchar(5) NOT NULL,
  "PRISION_PREVENTIVA" decimal(4, 0),
  "PRISION_PREVENTIVA_DECRETADA" decimal(4, 0),
  "PRISION_PREVENTIVA_RECHAZADA" decimal(4, 0),
  "RIT" smallint NOT NULL,
  "Rechaza Prisión Preventiva (copia)" varchar(27),
  "SENTENCIA" decimal(1, 0),
  "TIPO" smallint NOT NULL,
  "TRIBUNAL" varchar(30) NOT NULL
);

CREATE TABLE "TableroSistemaPenal_8"(
  "ACUSACIÓN FISCAL" varchar(2) NOT NULL,
  "AUDIENCIAS" varchar(18) NOT NULL,
  "COD DELITO" smallint NOT NULL,
  "COD_TRIBUNAL" smallint NOT NULL,
  "CORTE" varchar(17) NOT NULL,
  "CRR_IDCAUSA" integer NOT NULL,
  "CRR_IDPARTICIPANTE" integer NOT NULL,
  "Calculation_6750825005603169" varchar(55),
  "DELITO" varchar(64),
  "ESTADO_RELACION" varchar(13) NOT NULL,
  "FECHA" timestamp NOT NULL,
  "MEDIDAS CAUTELARES" varchar(2) NOT NULL,
  "Number of Records" smallint NOT NULL,
  "OTROS TÉRMINOS" varchar(51),
  "PAÍS" varchar(5) NOT NULL,
  "RIT" varchar(12) NOT NULL,
  "RPA" smallint NOT NULL,
  "RUC" varchar(12) NOT NULL,
  "SALIDAS TEMPRANAS" varchar(42),
  "SEXO" varchar(6) NOT NULL,
  "TRIBUNAL" varchar(30) NOT NULL,
  "Tipo Sentencia" varchar(22)
);

CREATE TABLE "TableroSistemaPenal_5"(
  "AGRUPADOR" varchar(55) NOT NULL,
  "AÑO CAUSA" smallint NOT NULL,
  "CAUTELAR" decimal(1, 0),
  "COD_CTE" smallint NOT NULL,
  "COD_MATERIA" smallint NOT NULL,
  "COD_TRIB" smallint NOT NULL,
  "CORTE" varchar(17) NOT NULL,
  "DELITO" varchar(64),
  "FECHA AUDIENCIA" timestamp NOT NULL,
  "ID_EVENTO" integer NOT NULL,
  "ILEGALIDAD_DETENCION" decimal(1, 0),
  "Number of Records" smallint NOT NULL,
  "PAÍS" varchar(5) NOT NULL,
  "PRISION_PREVENTIVA" decimal(4, 0),
  "PRISION_PREVENTIVA_DECRETADA" decimal(4, 0),
  "PRISION_PREVENTIVA_RECHAZADA" decimal(4, 0),
  "RIT" smallint NOT NULL,
  "Rechaza Prisión Preventiva (copia)" varchar(27),
  "SENTENCIA" decimal(1, 0),
  "TIPO" smallint NOT NULL,
  "TRIBUNAL" varchar(30) NOT NULL
);

CREATE TABLE "TableroSistemaPenal_2"(
  "AGRUPADOR" varchar(55) NOT NULL,
  "AÑO" smallint NOT NULL,
  "Año Ingreso" varchar(4) NOT NULL,
  "COD_FORMAINICIO" smallint NOT NULL,
  "CRR_IDCAUSA" integer NOT NULL,
  "Calculation_6580723191504157" varchar(5) NOT NULL,
  "Cod Materia" smallint NOT NULL,
  "Cod. Corte" smallint NOT NULL,
  "Cod. Tribunal" smallint NOT NULL,
  "Corte" varchar(17) NOT NULL,
  "Fecha Ingreso Delito" varchar(10) NOT NULL,
  "Fecha Ingreso" varchar(10) NOT NULL,
  "Forma Inicio" varchar(25) NOT NULL,
  "Materia" varchar(64) NOT NULL,
  "Mes Ingreso" varchar(7) NOT NULL,
  "Number of Records" smallint NOT NULL,
  "RIT" smallint NOT NULL,
  "Región" varchar(53) NOT NULL,
  "TIP_TRIB" varchar(9) NOT NULL,
  "Tipo" varchar(9) NOT NULL,
  "Tribunal" varchar(30) NOT NULL,
  "Materia (group)" varchar(64) NOT NULL
);

CREATE TABLE "TableroSistemaPenal_7"(
  "AGRUPADOR" varchar(55),
  "Año Ingreso" varchar(4) NOT NULL,
  "CLASE" varchar(13) NOT NULL,
  "COD DELITO" double NOT NULL,
  "COD. REGIÓN" smallint NOT NULL,
  "COMUNA" varchar(19) NOT NULL,
  "CORTE" varchar(17) NOT NULL,
  "CREA RUC" varchar(9) NOT NULL,
  "Calculation_0520821013255406" varchar(55),
  "Calculation_0640821194248948" varchar(55),
  "Calculation_7430825124450529" decimal(4, 0) NOT NULL,
  "Cod. Tribunal" smallint NOT NULL,
  "DELITO" varchar(64),
  "Forma Inicio" varchar(25) NOT NULL,
  "INGRESOS" decimal(4, 0) NOT NULL,
  "Ingresos _ Garantia (copy)" decimal(3, 0) NOT NULL,
  "Jurisdicción" varchar(9) NOT NULL,
  "LATITUD" varchar(19) NOT NULL,
  "LONGITUD" varchar(19) NOT NULL,
  "Mes Ingreso" varchar(7) NOT NULL,
  "Number of Records" smallint NOT NULL,
  "PAÍS" varchar(5) NOT NULL,
  "REGIÓN" varchar(53) NOT NULL,
  "RPA" varchar(3),
  "TIPO" varchar(9) NOT NULL,
  "TIP_TRIB" smallint NOT NULL,
  "TRIBUNAL" varchar(30) NOT NULL
);


COPY TableroSistemaPenal_4 FROM 'benchmark/publicbi/TableroSistemaPenal_4.csv.gz' ( DELIMITER '|', NULL 'null', QUOTE '', ESCAPE '\\n' );
COPY TableroSistemaPenal_6 FROM 'benchmark/publicbi/TableroSistemaPenal_6.csv.gz' ( DELIMITER '|', NULL 'null', QUOTE '', ESCAPE '\\n' );
COPY TableroSistemaPenal_1 FROM 'benchmark/publicbi/TableroSistemaPenal_1.csv.gz' ( DELIMITER '|', NULL 'null', QUOTE '', ESCAPE '\\n' );
COPY TableroSistemaPenal_3 FROM 'benchmark/publicbi/TableroSistemaPenal_3.csv.gz' ( DELIMITER '|', NULL 'null', QUOTE '', ESCAPE '\\n' );
COPY TableroSistemaPenal_8 FROM 'benchmark/publicbi/TableroSistemaPenal_8.csv.gz' ( DELIMITER '|', NULL 'null', QUOTE '', ESCAPE '\\n' );
COPY TableroSistemaPenal_5 FROM 'benchmark/publicbi/TableroSistemaPenal_5.csv.gz' ( DELIMITER '|', NULL 'null', QUOTE '', ESCAPE '\\n' );
COPY TableroSistemaPenal_2 FROM 'benchmark/publicbi/TableroSistemaPenal_2.csv.gz' ( DELIMITER '|', NULL 'null', QUOTE '', ESCAPE '\\n' );
COPY TableroSistemaPenal_7 FROM 'benchmark/publicbi/TableroSistemaPenal_7.csv.gz' ( DELIMITER '|', NULL 'null', QUOTE '', ESCAPE '\\n' );