SELECT
    p_personid,
    p_firstname,
    p_lastname,
    o_name,
    pc_workfrom
FROM
    person,
    person_company,
    organisation,
    place,
    (
        SELECT k_person2id
        FROM knows
        WHERE k_person1id = 21990232556256
        UNION
        SELECT k2.k_person2id
        FROM knows k1, knows k2
        WHERE k1.k_person1id = 21990232556256
          AND k1.k_person2id = k2.k_person1id
          AND k2.k_person2id <> 21990232556256
    ) f
WHERE p_personid = f.k_person2id
  AND p_personid = pc_personid
  AND pc_organisationid = o_organisationid
  AND pc_workfrom < 2012
  AND -- :workFromYear

o_placeid = pl_placeid
    AND pl_name = 'United_States' -- :countryName

ORDER BY pc_workfrom, p_personid, o_name DESC
LIMIT 10
