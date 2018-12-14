SELECT
    subq_0.c17 AS c0,
    34 AS c1,
    subq_0.c6 AS c2,
    subq_0.c3 AS c3,
    subq_0.c9 AS c4,
    subq_0.c6 AS c5,
    subq_0.c4 AS c6,
    subq_0.c16 AS c7,
    subq_0.c4 AS c8,
    subq_0.c8 AS c9
FROM (
    SELECT
        ref_2.p_size AS c0,
        (
            SELECT
                c_nationkey
            FROM
                main.customer
            LIMIT 1 offset 4) AS c1,
        ref_1.n_regionkey AS c2,
        56 AS c3,
        ref_2.p_size AS c4,
        ref_2.p_type AS c5,
        ref_2.p_retailprice AS c6,
        ref_1.n_nationkey AS c7,
        ref_2.p_container AS c8,
        ref_1.n_name AS c9,
        59 AS c10,
        ref_2.p_container AS c11,
        ref_0.p_comment AS c12,
        ref_1.n_nationkey AS c13,
        ref_1.n_regionkey AS c14,
        ref_1.n_name AS c15,
        ref_2.p_mfgr AS c16,
        ref_1.n_regionkey AS c17
    FROM
        main.part AS ref_0
        INNER JOIN main.nation AS ref_1
        RIGHT JOIN main.part AS ref_2 ON (ref_1.n_name IS NOT NULL) ON (ref_0.p_name = ref_2.p_name)
    WHERE ((1)
        AND ((ref_2.p_brand IS NOT NULL)
            OR ((ref_1.n_name IS NOT NULL)
                AND ((EXISTS (
                            SELECT
                                ref_2.p_retailprice AS c0, ref_2.p_partkey AS c1, ref_0.p_retailprice AS c2, ref_0.p_partkey AS c3, ref_3.r_name AS c4, ref_2.p_name AS c5, (
                                    SELECT
                                        ps_suppkey
                                    FROM
                                        main.partsupp
                                    LIMIT 1 offset 5) AS c6,
                                ref_2.p_partkey AS c7
                            FROM
                                main.region AS ref_3
                            WHERE
                                0
                            LIMIT 107))
                    AND (EXISTS (
                            SELECT
                                ref_1.n_name AS c0,
                                ref_2.p_retailprice AS c1
                            FROM
                                main.orders AS ref_4
                            WHERE
                                ref_0.p_size IS NOT NULL
                            LIMIT 33))))))
    AND (EXISTS (
            SELECT
                ref_2.p_size AS c0,
                ref_2.p_container AS c1,
                ref_0.p_brand AS c2,
                ref_5.ps_comment AS c3,
                ref_0.p_partkey AS c4,
                ref_1.n_regionkey AS c5,
                ref_1.n_comment AS c6,
                ref_0.p_comment AS c7
            FROM
                main.partsupp AS ref_5
            WHERE
                1
            LIMIT 70))
LIMIT 34) AS subq_0
WHERE
    subq_0.c10 IS NOT NULL
