call dbgen(sf=1);

create view customer_native as select * from customer;
create view lineitem_native as select * from lineitem;
create view nation_native as select * from nation;
create view orders_native as select * from orders;
create view part_native as select * from part;
create view partsupp_native as select * from partsupp;
create view region_native as select * from region;
create view supplier_native as select * from supplier;