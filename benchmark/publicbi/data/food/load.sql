CREATE TABLE "Food_1"(
  "Number of Records" smallint NOT NULL,
  "activity_sec" integer NOT NULL,
  "application" varchar(28),
  "device" varchar(40) NOT NULL,
  "subscribers" smallint NOT NULL,
  "volume_total_bytes" double NOT NULL
);


COPY Food_1 FROM 'benchmark/publicbi/Food_1.csv.gz' ( DELIMITER '|', NULL 'null', QUOTE '', ESCAPE '\\n' );