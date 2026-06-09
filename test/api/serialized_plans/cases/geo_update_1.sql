create table t1 (g GEOMETRY, id INT);
insert into t1 values ('POINT(1 2)', 1), (NULL, 2);
UPDATE t1 SET g = 'POINT(2 3)' where id = 2;