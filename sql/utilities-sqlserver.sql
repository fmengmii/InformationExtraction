
-- insert rows into document_status table based on whether GATE annotations were generated

insert into SCHEMA.document_status

select distinct a.document_namespace, a.document_table, a.document_id, 0 ,''
from SCHEMA.annotation a, SCHEMA.project_frame_instance b, SCHEMA.frame_instance_document c, SCHEMA.project d
where a.annotation_type = 'Token' and a.document_namespace = c.document_namespace and a.document_table = c.document_table and
a.document_id = c.document_id and c.frame_instance_id = b.frame_instance_id and c.project_id = d.project_id and d.name = ?