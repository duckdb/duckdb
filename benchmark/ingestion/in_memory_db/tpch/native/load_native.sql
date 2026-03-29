call dbgen(sf = 1);

CREATE VIEW customer_native AS
SELECT *
FROM customer;
CREATE VIEW lineitem_native AS
SELECT *
FROM lineitem;
CREATE VIEW nation_native AS
SELECT *
FROM nation;
CREATE VIEW orders_native AS
SELECT *
FROM orders;
CREATE VIEW part_native AS
SELECT *
FROM part;
CREATE VIEW partsupp_native AS
SELECT *
FROM partsupp;
CREATE VIEW region_native AS
SELECT *
FROM region;
CREATE VIEW supplier_native AS
SELECT *
FROM supplier;
