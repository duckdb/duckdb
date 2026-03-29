SELECT p1.m_creatorid, p_firstname, p_lastname, p1.m_creationdate, p1.m_messageid, p1.m_content
FROM message p1, message p2, person
WHERE p1.m_c_replyof = p2.m_messageid AND p2.m_creatorid = 21990232556256 AND p_personid = p1.m_creatorid
ORDER BY p1.m_creationdate DESC, 5
LIMIT 20
