DROP TABLE IF EXISTS fact_imprint_demo;

CREATE TABLE fact_imprint_demo (
    id BIGINT,
    v  BIGINT
);

INSERT INTO fact_imprint_demo
SELECT
    i::BIGINT AS id,
    CASE
        WHEN i % 1000 = 0 THEN 0                    
        WHEN i % 1000 = 1 THEN 1000000000          
        ELSE (i % 64) * 1000                       
    END AS v
FROM range(0, 1000000) t(i);

--- every 1000 rows, there is a 0 and 1000000000, the rest are in the range [0, 63000]

