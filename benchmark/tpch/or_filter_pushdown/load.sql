call dbgen(sf=100);
create or replace table lineitem_sf1 as from lineitem limit 6001215;
create or replace table lineitem_sf1_random as (select * from lineitem_sf1 order by hash(rowid + 42));
create or replace table lineitem_sf10 as from lineitem limit 59986052;
create or replace table lineitem_sf10_random as (select * from lineitem_sf10 order by hash(rowid + 42));
create or replace table lineitem_sf100_random as (select * from lineitem order by hash(rowid + 42));
alter table lineitem rename to lineitem_sf100;