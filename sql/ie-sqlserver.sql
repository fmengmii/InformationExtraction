CREATE TABLE SCHEMA.`profile` (
  `profile_id` int(11) NOT NULL IDENTITY(1,1),
  `group` varchar(500) DEFAULT NULL,
  `profile` text,
  `name` varchar(500) DEFAULT NULL,
  `profile_type` int(11) DEFAULT NULL,
  `annotation_type` varchar(500) DEFAULT NULL,
  `score` double DEFAULT NULL,
  `true_pos` double DEFAULT NULL,
  `false_pos` double DEFAULT NULL,
  `rows` int(11) DEFAULT NULL,
  PRIMARY KEY (`profile_id`)
)


CREATE TABLE SCHEMA.`filter` (
  `profile_id` bigint(20) NOT NULL,
  `target_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`profile_id`)
)



CREATE TABLE SCHEMA.`final` (
  `profile_id` bigint(20) DEFAULT NULL,
  `target_id` bigint(20) DEFAULT NULL,
  `total` int(11) DEFAULT NULL,
  `prec` double DEFAULT NULL,
  `valence` tinyint(4) DEFAULT NULL
)