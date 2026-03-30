call dbgen(sf=100);
create or replace table lineitem_sf1 as from lineitem limit 6001215;
create or replace table lineitem_sf1_random as (select * from lineitem_sf1 order by hash(rowid + 42));
create or replace table lineitem_sf10 as from lineitem limit 59986052;
create or replace table lineitem_sf10_random as (select * from lineitem_sf10 order by hash(rowid + 42));
pragma memory_limit='40GB';
create or replace table lineitem_sf100_random as (select * from lineitem order by hash(rowid + 42));
pragma memory_limit='';
alter table lineitem rename to lineitem_sf100;

create or replace table orders_sf1 as from orders limit 1500000;
create or replace table orders_sf1_random as (select * from orders_sf1 order by hash(rowid + 42));
create or replace table orders_sf10 as from orders limit 15000000;
create or replace table orders_sf10_random as (select * from orders_sf10 order by hash(rowid + 42));
pragma memory_limit='40GB';
create or replace table orders_sf100_random as (select * from orders order by hash(rowid + 42));
pragma memory_limit='';
alter table orders rename to orders_sf100;