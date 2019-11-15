CREATE TABLE `profile` (
  `profile_id` int(11) NOT NULL AUTO_INCREMENT,
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
) ENGINE=InnoDB AUTO_INCREMENT=29 DEFAULT CHARSET=latin1;


CREATE TABLE `filter` (
  `profile_id` bigint(20) NOT NULL,
  `target_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`profile_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



CREATE TABLE `final` (
  `profile_id` bigint(20) DEFAULT NULL,
  `target_id` bigint(20) DEFAULT NULL,
  `total` int(11) DEFAULT NULL,
  `prec` double DEFAULT NULL,
  `valence` tinyint(4) DEFAULT NULL,
  `true_pos` int NULL,
  `false_pos` int NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



CREATE TABLE `index` (
  `profile_id` bigint(20) DEFAULT NULL,
  `target_id` bigint(20) DEFAULT NULL,
  `document_id` bigint(20) DEFAULT NULL,
  `start` bigint(20) DEFAULT NULL,
  `end` bigint(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



CREATE TABLE `auto_status` (
  `annotation_type` varchar(500) NOT NULL,
  `document_id` bigint(20) DEFAULT NULL,
  `profile_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`annotation_type`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `gen_filter_status` (
  `document_id` bigint(20) NOT NULL,
  PRIMARY KEY (`document_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `gen_msa_status` (
  `annotation_type` varchar(500) NOT NULL,
  `profile_count` int(11) DEFAULT NULL,
  PRIMARY KEY (`annotation_type`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;




