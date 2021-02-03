
-- status definitions
-- -2 = annotated, No GATE, 0 = GATE, 1 = GATE+annotated, 2 = trained

-- insert rows into document_status table based on whether GATE annotations were generated

insert into SCHEMA.document_status

select distinct a.document_namespace, a.document_table, a.document_id, 0, ''
from SCHEMA.annotation a, SCHEMA.project_frame_instance b, SCHEMA.frame_instance_document c, SCHEMA.project d
where a.annotation_type = 'Token' and a.document_namespace = c.document_namespace and a.document_table = c.document_table and
a.document_id = c.document_id and c.frame_instance_id = b.frame_instance_id and c.project_id = d.project_id and d.name = ?


insert into SCHEMA.frame_instance_status

select a.frame_instance_id, 1, b.user_id
from SCHEMA.project_frame_instance a, SCHEMA."user" b, SCHEMA.project c
where c.name = ? and exists
(

)



-- update -2 statuses to 1

update SCHEMA.document_status a set a.status = 1
where a.status = -2 and exists
(
select * from SCHEMA.annotation b
where a.document_namespace = b.document_namespace and a.document_table = b.document_table and a.document_id = b.document_id
and b.annotation_type = 'Token'
)