insert into prob_per_entity

select a.value, b.c, a.c, (b.c/a.c) 
from
(select value, count(*) c
from annotation
where provenance = 'conll2003-entity'
and document_id < 1163 and value like '% %'
group by value) a left join

(select value, count(*) c
from annotation
where provenance = 'conll2003-entity' and annotation_type = 'I-PER' and value like '% %'
and document_id < 1163
group by value) b

on a.value = b.value;



insert into prob_org_entity

select a.value, b.c, a.c, (b.c/a.c) 
from
(select value, count(*) c
from annotation
where provenance = 'conll2003-entity'
and document_id < 1163
group by value) a left join

(select value, count(*) c
from annotation
where provenance = 'conll2003-entity' and annotation_type = 'I-ORG'
and document_id < 1163
group by value) b

on a.value = b.value;



insert into prob_loc_entity

select a.value, b.c, a.c, (b.c/a.c) 
from
(select value, count(*) c
from annotation
where provenance = 'conll2003-entity'
and document_id < 1163
group by value) a left join

(select value, count(*) c
from annotation
where provenance = 'conll2003-entity' and annotation_type = 'I-LOC'
and document_id < 1163
group by value) b

on a.value = b.value;



update prob_loc_entity set prob = 0.0 where prob is null;



CREATE TABLE `prob_loc_entity` (
  `value` varchar(500) DEFAULT NULL,
  `pos` int(11) DEFAULT NULL,
  `total` int(11) DEFAULT NULL,
  `prob` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;





insert into prob_per
select a.value, b.c, a.c, (b.c/a.c)
from
(select value, count(*) c from annotation
where annotation_type = 'Token' and features like '%nnp%' and document_id < 1163
group by value) a left join

(select d.value, count(*) c from annotation d, annotation e
where d.annotation_type = 'I-PER' and d.provenance = 'conll2003-token' and d.document_id < 1163 and
e.annotation_type = 'Token' and (e.features like '%upperInitial%' or e.features like '%allCaps%') and
d.document_id = e.document_id and d.start = e.start
group by value) b

on a.value = b.value;



insert into prob_org
select a.value, b.c, a.c, (b.c/a.c)
from
(select value, count(*) c from annotation
where annotation_type = 'Token' and document_id < 1163 and (features like '%upperInitial%' or features like '%allCaps%')
group by value) a left join

(select d.value, count(*) c from annotation d, annotation e
where d.annotation_type = 'I-ORG' and d.provenance = 'conll2003-token' and d.document_id < 1163 and
e.annotation_type = 'Token' and (e.features like '%upperInitial%' or e.features like '%allCaps%') and
d.document_id = e.document_id and d.start = e.start
group by value) b

on a.value = b.value;



insert into prob_loc
select a.value, b.c, a.c, (b.c/a.c)
from
(select value, count(*) c from annotation
where annotation_type = 'Token' and document_id < 1163 and (features like '%upperInitial%' or features like '%allCaps%')
group by value) a left join

(select d.value, count(*) c from annotation d, annotation e
where d.annotation_type = 'I-LOC' and d.provenance = 'conll2003-token' and d.document_id < 1163 and
e.annotation_type = 'Token' and (e.features like '%upperInitial%' or e.features like '%allCaps%') and
d.document_id = e.document_id and d.start = e.start
group by value) b

on a.value = b.value;


CREATE TABLE `prob_per` (
  `value` varchar(500) DEFAULT NULL,
  `pos` int(11) DEFAULT NULL,
  `total` int(11) DEFAULT NULL,
  `prob` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


update prob_loc set pos = 0.0 where pos is null;
