SELECT
    p_personid,
    p_firstname,
    p_lastname,
    m_messageid,
    COALESCE(m_ps_imagefile, m_content, '') AS content,
    m_creationdate
FROM person, message, knows
WHERE p_personid = m_creatorid
  AND m_creationdate < '2011-07-21T22:00:00'
  AND k_person1id = 21990232556256
  AND k_person2id = p_personid
ORDER BY m_creationdate DESC, m_messageid ASC
LIMIT 20
