CREATE TABLE "TrainsUK1_4"(
  "Average Lateness" double NOT NULL,
  "Calculation_2040623161253421" decimal(4, 2) NOT NULL,
  "Calculation_2890421151640665" varchar(18) NOT NULL,
  "Calculation_3330422103625946" decimal(17, 16) NOT NULL,
  "Engineering Allowance (mins)" smallint,
  "Headcode" varchar(4) NOT NULL,
  "Median Lateness" decimal(4, 2) NOT NULL,
  "Operator Name" varchar(24) NOT NULL,
  "Operator" varchar(2) NOT NULL,
  "Pathing Allowance (mins)" decimal(3, 1) NOT NULL,
  "Performance Allowance (mins)" decimal(2, 1) NOT NULL,
  "Planned Dest Location Full Name" varchar(30) NOT NULL,
  "Planned Origin Location Full Name" varchar(30) NOT NULL,
  "Punctuality Threshold" smallint NOT NULL,
  "RT%" decimal(18, 17) NOT NULL,
  "Ranking" smallint,
  "Section Start Location Full Name" varchar(31) NOT NULL,
  "Section Start Location Name" varchar(31) NOT NULL,
  "Time-to-10%" decimal(17, 16) NOT NULL,
  "Time-to-2%" decimal(18, 17) NOT NULL,
  "Time-to-5%" decimal(17, 16) NOT NULL,
  "Timetable" varchar(3) NOT NULL,
  "Train Count" smallint NOT NULL,
  "v_Headcode Description" varchar(77) NOT NULL,
  "v_Section_WTT_Time" time NOT NULL,
  "v_WTT and Section Name and Timing Event" varchar(43) NOT NULL,
  "Calculation_2480421151322357" varchar(1) NOT NULL,
  "Calculation_0430624152715434" varchar(69) NOT NULL
);

CREATE TABLE "TrainsUK1_2"(
  "Average Lateness" double NOT NULL,
  "Calculation_2040623161253421" decimal(4, 2) NOT NULL,
  "Calculation_2890421151640665" varchar(18) NOT NULL,
  "Calculation_3330422103625946" decimal(17, 16) NOT NULL,
  "Engineering Allowance (mins)" smallint,
  "Headcode" varchar(4) NOT NULL,
  "Median Lateness" decimal(4, 2) NOT NULL,
  "Operator Name" varchar(24) NOT NULL,
  "Operator" varchar(2) NOT NULL,
  "Pathing Allowance (mins)" decimal(3, 1) NOT NULL,
  "Performance Allowance (mins)" decimal(2, 1) NOT NULL,
  "Planned Dest Location Full Name" varchar(30) NOT NULL,
  "Planned Origin Location Full Name" varchar(30) NOT NULL,
  "Punctuality Threshold" smallint NOT NULL,
  "RT%" decimal(18, 17) NOT NULL,
  "Ranking" smallint,
  "Section Start Location Full Name" varchar(31) NOT NULL,
  "Section Start Location Name" varchar(31) NOT NULL,
  "Time-to-10%" decimal(17, 16) NOT NULL,
  "Time-to-2%" decimal(18, 17) NOT NULL,
  "Time-to-5%" decimal(17, 16) NOT NULL,
  "Timetable" varchar(3) NOT NULL,
  "Train Count" smallint NOT NULL,
  "v_Headcode Description" varchar(77) NOT NULL,
  "v_Section_WTT_Time" time NOT NULL,
  "v_WTT and Section Name and Timing Event" varchar(43) NOT NULL,
  "Calculation_2480421151322357" varchar(1) NOT NULL,
  "Calculation_0430624152715434" varchar(69) NOT NULL
);

CREATE TABLE "TrainsUK1_1"(
  "Date" date NOT NULL,
  "Number of Records" smallint NOT NULL,
  "Test" smallint NOT NULL
);

CREATE TABLE "TrainsUK1_3"(
  "Average Lateness" double NOT NULL,
  "Calculation_2040623161253421" decimal(4, 2) NOT NULL,
  "Calculation_2890421151640665" varchar(18) NOT NULL,
  "Calculation_3330422103625946" decimal(17, 16) NOT NULL,
  "Engineering Allowance (mins)" smallint,
  "Headcode" varchar(4) NOT NULL,
  "Median Lateness" decimal(4, 2) NOT NULL,
  "Operator Name" varchar(24) NOT NULL,
  "Operator" varchar(2) NOT NULL,
  "Pathing Allowance (mins)" decimal(3, 1) NOT NULL,
  "Performance Allowance (mins)" decimal(2, 1) NOT NULL,
  "Planned Dest Location Full Name" varchar(30) NOT NULL,
  "Planned Origin Location Full Name" varchar(30) NOT NULL,
  "Punctuality Threshold" smallint NOT NULL,
  "RT%" decimal(18, 17) NOT NULL,
  "Ranking" smallint,
  "Section Start Location Full Name" varchar(31) NOT NULL,
  "Section Start Location Name" varchar(31) NOT NULL,
  "Time-to-10%" decimal(17, 16) NOT NULL,
  "Time-to-2%" decimal(18, 17) NOT NULL,
  "Time-to-5%" decimal(17, 16) NOT NULL,
  "Timetable" varchar(3) NOT NULL,
  "Train Count" smallint NOT NULL,
  "v_Headcode Description" varchar(77) NOT NULL,
  "v_Section_WTT_Time" time NOT NULL,
  "v_WTT and Section Name and Timing Event" varchar(43) NOT NULL,
  "Calculation_2480421151322357" varchar(1) NOT NULL,
  "Calculation_0430624152715434" varchar(69) NOT NULL
);


COPY TrainsUK1_4 FROM 'benchmark/publicbi/TrainsUK1_4.csv.gz' ( DELIMITER '|', NULL 'null', QUOTE '', ESCAPE '\\n' );
COPY TrainsUK1_2 FROM 'benchmark/publicbi/TrainsUK1_2.csv.gz' ( DELIMITER '|', NULL 'null', QUOTE '', ESCAPE '\\n' );
COPY TrainsUK1_1 FROM 'benchmark/publicbi/TrainsUK1_1.csv.gz' ( DELIMITER '|', NULL 'null', QUOTE '', ESCAPE '\\n' );
COPY TrainsUK1_3 FROM 'benchmark/publicbi/TrainsUK1_3.csv.gz' ( DELIMITER '|', NULL 'null', QUOTE '', ESCAPE '\\n' );