create table t1 (g GEOMETRY, id INT);
insert into t1 values ('POINT(1 2)', 1), (NULL, 2);
create index t1_g_idx on t1 (id);
UPDATE t1 SET id = 3 where g = 'POINT(1 2)';
