SELECT
    p2.m_messageid,
    p2.m_content,
    p2.m_creationdate,
    p_personid,
    p_firstname,
    p_lastname,
    (CASE WHEN EXISTS (
                SELECT 1
                FROM knows
                WHERE p1.m_creatorid = k_person1id AND p2.m_creatorid = k_person2id
            ) THEN TRUE ELSE FALSE END) AS knows
FROM message p1, message p2, person
WHERE p1.m_messageid = 687194767741
  AND p2.m_c_replyof = p1.m_messageid
  AND p2.m_creatorid = p_personid
ORDER BY p2.m_creationdate DESC, p2.m_creatorid ASC;
