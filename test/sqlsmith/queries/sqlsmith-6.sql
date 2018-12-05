SELECT
    (
        SELECT
            l_linestatus
        FROM
            main.lineitem
        LIMIT 1 offset 4) AS c0,
    subq_0.c1 AS c1,
    subq_0.c0 AS c2,
    subq_0.c1 AS c3,
    subq_0.c0 AS c4,
    CASE WHEN subq_0.c1 IS NULL THEN
        subq_0.c1
    ELSE
        subq_0.c1
    END AS c5
FROM (
    SELECT
        ref_1.p_comment AS c0,
        ref_1.p_brand AS c1
    FROM
        main.region AS ref_0
        INNER JOIN main.part AS ref_1 ON (ref_0.r_comment = ref_1.p_name)
    WHERE (0)
    OR ((EXISTS (
                SELECT
                    82 AS c0, ref_1.p_retailprice AS c1, ref_1.p_partkey AS c2, ref_2.ps_comment AS c3, ref_1.p_comment AS c4, ref_2.ps_suppkey AS c5, (
                        SELECT
                            s_suppkey
                        FROM
                            main.supplier
                        LIMIT 1 offset 1) AS c6,
                    ref_0.r_regionkey AS c7,
                    ref_0.r_regionkey AS c8,
                    ref_2.ps_comment AS c9,
                    ref_0.r_name AS c10
                FROM
                    main.partsupp AS ref_2
                WHERE ((((0)
                            AND ((0)
                                AND (((1)
                                        OR (0))
                                    AND ((1)
                                        OR (EXISTS (
                                                SELECT
                                                    ref_2.ps_partkey AS c0, ref_2.ps_availqty AS c1
                                                FROM
                                                    main.nation AS ref_3
                                                WHERE
                                                    ref_0.r_comment IS NOT NULL
                                                LIMIT 103))))))
                        OR (0))
                    OR (EXISTS (
                            SELECT
                                39 AS c0,
                                ref_0.r_regionkey AS c1,
                                ref_1.p_partkey AS c2,
                                ref_0.r_regionkey AS c3,
                                ref_0.r_name AS c4,
                                ref_0.r_regionkey AS c5,
                                ref_0.r_comment AS c6,
                                (
                                    SELECT
                                        n_name
                                    FROM
                                        main.nation
                                    LIMIT 1 offset 26) AS c7,
                                ref_0.r_regionkey AS c8,
                                ref_0.r_name AS c9,
                                ref_2.ps_supplycost AS c10,
                                ref_1.p_comment AS c11,
                                ref_2.ps_availqty AS c12,
                                ref_1.p_brand AS c13,
                                (
                                    SELECT
                                        ps_partkey
                                    FROM
                                        main.partsupp
                                    LIMIT 1 offset 6) AS c14,
                                ref_1.p_brand AS c15,
                                ref_2.ps_comment AS c16,
                                ref_1.p_partkey AS c17,
                                ref_1.p_retailprice AS c18,
                                ref_1.p_container AS c19,
                                ref_2.ps_comment AS c20,
                                ref_4.c_name AS c21,
                                ref_0.r_name AS c22,
                                ref_2.ps_partkey AS c23,
                                ref_4.c_nationkey AS c24,
                                ref_4.c_comment AS c25,
                                ref_4.c_nationkey AS c26,
                                ref_0.r_regionkey AS c27,
                                ref_1.p_retailprice AS c28,
                                ref_1.p_partkey AS c29,
                                34 AS c30,
                                ref_1.p_type AS c31,
                                ref_0.r_name AS c32,
                                ref_2.ps_supplycost AS c33,
                                ref_2.ps_partkey AS c34,
                                ref_0.r_name AS c35,
                                ref_4.c_phone AS c36,
                                70 AS c37
                            FROM
                                main.customer AS ref_4
                            WHERE
                                EXISTS (
                                    SELECT
                                        ref_5.c_nationkey AS c0, ref_2.ps_suppkey AS c1, ref_2.ps_comment AS c2, ref_4.c_acctbal AS c3, ref_0.r_name AS c4, ref_0.r_comment AS c5, ref_0.r_name AS c6, ref_1.p_type AS c7, ref_2.ps_supplycost AS c8, ref_4.c_mktsegment AS c9, ref_1.p_comment AS c10
                                    FROM
                                        main.customer AS ref_5
                                    WHERE (1)
                                    AND (ref_2.ps_suppkey IS NULL)
                                LIMIT 90))))
                AND (0)))
        OR (ref_1.p_retailprice IS NULL))) AS subq_0
WHERE
    11 IS NOT NULL
LIMIT 117
