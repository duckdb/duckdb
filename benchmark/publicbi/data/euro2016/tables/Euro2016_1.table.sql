CREATE TABLE "Euro2016_1"(
  "Number of Records" smallint NOT NULL,
  "id" integer NOT NULL,
  "lang" varchar(3) NOT NULL,
  "latitude" varchar(12),
  "longitude" varchar(13),
  "polarity" varchar(8),
  "polarity_confidence" decimal(16, 15),
  "subjectivity" varchar(10),
  "subjectivity_confidence" decimal(16, 15),
  "tweet" varchar(402) NOT NULL,
  "tweeted_at" timestamp
);
