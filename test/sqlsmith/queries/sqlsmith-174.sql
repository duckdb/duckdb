SELECT
    ref_1.r_comment AS c0
FROM
    main.supplier AS ref_0
    LEFT JOIN main.region AS ref_1
    LEFT JOIN main.customer AS ref_2 ON ((0)
            OR (1)) ON (ref_0.s_suppkey = ref_1.r_regionkey)
    RIGHT JOIN main.part AS ref_3 ON (ref_1.r_regionkey = ref_3.p_partkey)
WHERE ((0)
    OR (1))
OR (((((ref_0.s_address IS NOT NULL)
                AND (1))
            OR ((1)
                AND (((ref_2.c_phone IS NOT NULL)
                        OR (0))
                    AND (ref_1.r_regionkey IS NOT NULL))))
        AND (((ref_2.c_acctbal IS NULL)
                OR (((ref_1.r_name IS NULL)
                        AND (EXISTS (
                                SELECT
                                    ref_4.c_custkey AS c0, ref_3.p_type AS c1, ref_4.c_comment AS c2
                                FROM
                                    main.customer AS ref_4
                                WHERE
                                    ref_2.c_comment IS NULL
                                LIMIT 101)))
                    AND ((0)
                        AND ((EXISTS (
                                    SELECT
                                        ref_3.p_brand AS c0,
                                        ref_5.ps_availqty AS c1,
                                        ref_5.ps_availqty AS c2
                                    FROM
                                        main.partsupp AS ref_5
                                    WHERE (ref_2.c_nationkey IS NOT NULL)
                                    OR (((EXISTS (
                                                    SELECT
                                                        ref_1.r_name AS c0, ref_1.r_regionkey AS c1
                                                    FROM
                                                        main.nation AS ref_6
                                                    WHERE
                                                        ref_3.p_size IS NOT NULL
                                                    LIMIT 26))
                                            AND ((1)
                                                AND ((((0)
                                                            OR (0))
                                                        OR (0))
                                                    AND (((((EXISTS (
                                                                            SELECT
                                                                                ref_5.ps_supplycost AS c0
                                                                            FROM
                                                                                main.partsupp AS ref_7
                                                                            WHERE
                                                                                1
                                                                            LIMIT 105))
                                                                    OR (91 IS NULL))
                                                                OR (((
                                                                            SELECT
                                                                                r_regionkey
                                                                            FROM
                                                                                main.region
                                                                            LIMIT 1 offset 31)
                                                                        IS NULL)
                                                                    AND (1)))
                                                            OR (1))
                                                        AND (ref_1.r_regionkey IS NULL)))))
                                        AND ((0)
                                            OR (1)))))
                            OR (ref_3.p_container IS NULL)))))
            AND (ref_0.s_name IS NULL)))
    AND (ref_0.s_suppkey IS NULL))
