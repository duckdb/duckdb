CREATE TABLE "Romance_2"(
  "F1" integer,
  "Number of Records" smallint NOT NULL,
  "caption" varchar(8160),
  "created_time" integer,
  "id" varchar(55),
  "lat" decimal(18, 15),
  "link" varchar(40),
  "lng" double,
  "location" varchar(213),
  "tags" varchar(950),
  "term" varchar(12),
  "user" bigint
);

CREATE TABLE "Romance_1"(
  "F1" integer,
  "Number of Records" smallint NOT NULL,
  "caption" varchar(8160),
  "created_time" integer,
  "id" varchar(55),
  "lat" decimal(18, 15),
  "link" varchar(40),
  "lng" double,
  "location" varchar(213),
  "tags" varchar(950),
  "term" varchar(12),
  "user" bigint
);


COPY Romance_2 FROM 'benchmark/publicbi/Romance_2.csv.gz' ( DELIMITER '|', NULL 'null', QUOTE '', ESCAPE '\\n' );
COPY Romance_1 FROM 'benchmark/publicbi/Romance_1.csv.gz' ( DELIMITER '|', NULL 'null', QUOTE '', ESCAPE '\\n' );