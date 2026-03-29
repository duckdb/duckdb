SELECT
    p_personid,
    p_firstname,
    p_lastname,
    m_messageid,
    COALESCE(m_ps_imagefile, '') || COALESCE(m_content, '') AS content,
    m_creationdate
FROM (
        SELECT k_person2id
        FROM knows
        WHERE k_person1id = 21990232556256
        UNION
        SELECT k2.k_person2id
        FROM knows k1, knows k2
        WHERE k1.k_person1id = 21990232556256
          AND k1.k_person2id = k2.k_person1id
          AND k2.k_person2id <> 21990232556256
    ) f,
    person,
    message
WHERE p_personid = m_creatorid
  AND p_personid = f.k_person2id
  AND m_creationdate < '2012-07-26T22:00:00'
ORDER BY m_creationdate DESC, m_messageid ASC
LIMIT 20
