SELECT
    subq_0.c8 AS c0,
    subq_0.c8 AS c1,
    subq_0.c18 AS c2,
    subq_0.c7 AS c3,
    subq_0.c0 AS c4,
    subq_0.c16 AS c5,
    subq_0.c14 AS c6,
    CASE WHEN subq_0.c20 IS NULL THEN
        subq_0.c0
    ELSE
        subq_0.c0
    END AS c7
FROM (
    SELECT
        ref_0.l_suppkey AS c0,
        ref_2.p_retailprice AS c1,
        ref_0.l_orderkey AS c2,
        ref_1.p_partkey AS c3,
        ref_0.l_orderkey AS c4,
        ref_0.l_shipmode AS c5,
        ref_0.l_partkey AS c6,
        ref_0.l_quantity AS c7,
        ref_0.l_linestatus AS c8,
        ref_0.l_quantity AS c9,
        CASE WHEN (((ref_1.p_brand IS NOT NULL)
                    AND (EXISTS (
                            SELECT
                                ref_1.p_comment AS c0,
                                ref_1.p_container AS c1,
                                ref_2.p_partkey AS c2
                            FROM
                                main.customer AS ref_3
                            WHERE ((EXISTS (
                                        SELECT
                                            ref_3.c_comment AS c0, ref_1.p_brand AS c1, ref_0.l_comment AS c2, ref_4.p_type AS c3, ref_4.p_brand AS c4, ref_4.p_container AS c5, ref_3.c_name AS c6
                                        FROM
                                            main.part AS ref_4
                                        WHERE
                                            ref_3.c_comment IS NOT NULL
                                        LIMIT 135))
                                OR (0))
                            OR ((1)
                                OR (ref_1.p_partkey IS NULL)))))
                AND (ref_2.p_name IS NULL))
            OR (ref_0.l_linenumber IS NOT NULL) THEN
            ref_0.l_discount
        ELSE
            ref_0.l_discount
        END AS c10,
        ref_1.p_comment AS c11,
        CASE WHEN 1 THEN
            ref_2.p_retailprice
        ELSE
            ref_2.p_retailprice
        END AS c12,
        ref_2.p_brand AS c13,
        ref_0.l_orderkey AS c14,
        ref_2.p_comment AS c15,
        ref_0.l_returnflag AS c16,
        ref_2.p_mfgr AS c17,
        ref_2.p_mfgr AS c18,
        ref_1.p_name AS c19,
        ref_0.l_linenumber AS c20,
        ref_2.p_retailprice AS c21,
        ref_2.p_name AS c22,
        ref_2.p_brand AS c23
    FROM
        main.lineitem AS ref_0
        INNER JOIN main.part AS ref_1
        INNER JOIN main.part AS ref_2 ON (ref_1.p_type IS NULL) ON (ref_0.l_comment = ref_2.p_name)
    WHERE
        ref_2.p_mfgr IS NOT NULL
    LIMIT 130) AS subq_0
WHERE
    CAST(coalesce(subq_0.c12, subq_0.c12) AS DECIMAL)
    IS NULL
