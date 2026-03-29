call dbgen(sf = 100);
CREATE OR REPLACE TABLE lineitem_sf1 AS
FROM lineitem
LIMIT 6001215;
CREATE OR REPLACE TABLE lineitem_sf1_random AS (
        SELECT *
        FROM lineitem_sf1
        ORDER BY hash(rowid + 42)
    );
CREATE OR REPLACE TABLE lineitem_sf10 AS
FROM lineitem
LIMIT 59986052;
CREATE OR REPLACE TABLE lineitem_sf10_random AS (
        SELECT *
        FROM lineitem_sf10
        ORDER BY hash(rowid + 42)
    );
PRAGMA memory_limit = '40GB';
CREATE OR REPLACE TABLE lineitem_sf100_random AS (
        SELECT *
        FROM lineitem
        ORDER BY hash(rowid + 42)
    );
PRAGMA memory_limit = '';
ALTER TABLE lineitem RENAME to lineitem_sf100;

CREATE OR REPLACE TABLE orders_sf1 AS
FROM orders
LIMIT 1500000;
CREATE OR REPLACE TABLE orders_sf1_random AS (
        SELECT *
        FROM orders_sf1
        ORDER BY hash(rowid + 42)
    );
CREATE OR REPLACE TABLE orders_sf10 AS
FROM orders
LIMIT 15000000;
CREATE OR REPLACE TABLE orders_sf10_random AS (
        SELECT *
        FROM orders_sf10
        ORDER BY hash(rowid + 42)
    );
PRAGMA memory_limit = '40GB';
CREATE OR REPLACE TABLE orders_sf100_random AS (
        SELECT *
        FROM orders
        ORDER BY hash(rowid + 42)
    );
PRAGMA memory_limit = '';
ALTER TABLE orders RENAME to orders_sf100;
