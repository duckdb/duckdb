call dbgen(sf=100);
create table lineitem_sf1 as from lineiteim limit 6001215;
create table lineitem_sf1_random as (select * (from lineiteim limit 6001215) order by hash(rowid + 42));
create table lineitem_sf10 as from lineiteim limit 59986052;
create table lineitem_sf10_random as (select * from (from lineiteim limit 59986052) order by hash(rowid + 42));
create table lineitem_sf100_random as (select * from lineitem order by hash(rowid + 42));
alter table lineitem rename to lineitem_sf100;