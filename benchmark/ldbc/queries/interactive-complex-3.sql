SELECT p_personid, p_firstname, p_lastname, ct1, ct2, total
FROM (
        SELECT k_person2id
        FROM knows
        WHERE k_person1id = 6597069767251
        UNION
        SELECT k2.k_person2id
        FROM knows k1, knows k2
        WHERE k1.k_person1id = 6597069767251 AND k1.k_person2id = k2.k_person1id AND k2.k_person2id <> 15393162789164
    ) f,
    person,
    place p1,
    place p2,
    (
        SELECT chn.m_c_creatorid, ct1, ct2, ct1 + ct2 AS total
        FROM (
                SELECT m_creatorid AS m_c_creatorid, count(*) AS ct1
                FROM message, place
                WHERE m_locationid = pl_placeid
                  AND pl_name = 'United_States'
                  AND m_creationdate >= '2010-07-21T22:00:00'
                  AND m_creationdate < '2012-07-26T22:00:00' --('2011-07-21T22:00:00' + INTERVAL '1 days' * 5)

                GROUP BY m_c_creatorid
            ) chn,
            (
                SELECT m_creatorid AS m_c_creatorid, count(*) AS ct2
                FROM message, place
                WHERE m_locationid = pl_placeid
                  AND pl_name = 'Canada'
                  AND m_creationdate >= '2010-07-21T22:00:00'
                  AND m_creationdate < '2012-01-26T22:00:00' --('2011-07-21T22:00:00' + INTERVAL '1 days' * 5)

                GROUP BY m_creatorid --m_c_creatorid

) ind
        WHERE chn.m_c_creatorid = ind.m_c_creatorid
    ) cpc
WHERE f.k_person2id = p_personid
  AND p_placeid = p1.pl_placeid
  AND p1.pl_containerplaceid = p2.pl_placeid
  AND p2.pl_name <> 'United_States'
  AND p2.pl_name <> 'Canada'
  AND f.k_person2id = cpc.m_c_creatorid
ORDER BY 6 DESC, 1
LIMIT 20
