SELECT
    ref_0.ps_availqty AS c0,
    ref_0.ps_partkey AS c1,
    CASE WHEN (7 IS NULL)
        AND ((0)
            AND ((EXISTS (
                        SELECT
                            ref_0.ps_availqty AS c0,
                            ref_1.s_address AS c1
                        FROM
                            main.supplier AS ref_1
                        WHERE (EXISTS (
                                SELECT
                                    ref_1.s_acctbal AS c0, ref_0.ps_partkey AS c1, ref_0.ps_comment AS c2, ref_1.s_phone AS c3, ref_1.s_address AS c4, ref_0.ps_availqty AS c5
                                FROM
                                    main.partsupp AS ref_2
                                WHERE
                                    EXISTS (
                                        SELECT
                                            DISTINCT ref_1.s_address AS c0
                                        FROM
                                            main.region AS ref_3
                                        WHERE ((ref_3.r_regionkey IS NOT NULL)
                                            AND (EXISTS (
                                                    SELECT
                                                        ref_1.s_phone AS c0
                                                    FROM
                                                        main.customer AS ref_4
                                                    WHERE ((1)
                                                        OR (0))
                                                    AND (0)
                                                LIMIT 148)))
                                    OR (1)
                                LIMIT 48)))
                    AND (0)
                LIMIT 104))
        OR ((
                SELECT
                    ps_availqty
                FROM
                    main.partsupp
                LIMIT 1 offset 12)
            IS NULL))) THEN
ref_0.ps_partkey
ELSE
    ref_0.ps_partkey
END AS c2,
ref_0.ps_supplycost AS c3,
ref_0.ps_suppkey AS c4,
ref_0.ps_supplycost AS c5,
ref_0.ps_supplycost AS c6
FROM
    main.partsupp AS ref_0
WHERE
    ref_0.ps_supplycost IS NULL
