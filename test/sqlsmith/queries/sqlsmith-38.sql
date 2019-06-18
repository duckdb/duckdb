-- SELECT
--     subq_1.c2 AS c0,
--     subq_1.c0 AS c1
-- FROM (

SELECT
    nullif (nullif ((
                SELECT
                    ps_comment
                FROM
                    main.partsupp
                LIMIT 1 offset 6),
            subq_0.s_name),
        subq_0.s_name) AS c0
FROM
    main.supplier AS subq_0 -- LIMIT 33
    -- WHERE
    --     1
    -- LIMIT 97) AS subq_1
    -- WHERE
    --     1
