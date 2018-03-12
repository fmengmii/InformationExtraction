CREATE TABLE SCHEMA."profile" (
  "profile_id" int NOT NULL IDENTITY(1,1),
  "group" varchar(500) DEFAULT NULL,
  "profile" text,
  "name" varchar(500) DEFAULT NULL,
  "profile_type" int DEFAULT NULL,
  "annotation_type" varchar(500) DEFAULT NULL,
  "score" double DEFAULT NULL,
  "true_pos" double DEFAULT NULL,
  "false_pos" double DEFAULT NULL,
  "rows" int DEFAULT NULL,
  PRIMARY KEY ("profile_id")
)


CREATE TABLE SCHEMA."filter" (
  "profile_id" bigint(20) NOT NULL,
  "target_id" bigint(20) DEFAULT NULL,
  PRIMARY KEY ("profile_id")
)



CREATE TABLE SCHEMA."final" (
  "profile_id" bigint(20) DEFAULT NULL,
  "target_id" bigint(20) DEFAULT NULL,
  "total" int DEFAULT NULL,
  "prec" double DEFAULT NULL,
  "valence" tinyint(4) DEFAULT NULL
)


CREATE TABLE SCHEMA."auto_status" (
  "annotation_type" varchar(500) NOT NULL,
  "document_id" bigint(20) DEFAULT NULL,
  "profile_id" int DEFAULT NULL,
  PRIMARY KEY ("annotation_type")
)


CREATE TABLE SCHEMA."gen_filter_status" (
  "document_id" bigint(20) NOT NULL,
  PRIMARY KEY ("document_id")
)


CREATE TABLE SCHEMA."gen_msa_status" (
  "annotation_type" varchar(500) NOT NULL,
  "profile_count" int DEFAULT NULL,
  PRIMARY KEY ("annotation_type")
)
