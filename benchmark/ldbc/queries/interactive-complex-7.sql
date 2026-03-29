SELECT
    p_personid,
    p_firstname,
    p_lastname,
    l.l_creationdate,
    m_messageid,
    COALESCE(m_ps_imagefile, '') || COALESCE(m_content, ''),
    0 AS lag,
    -- TODO

--EXTRACT(EPOCH FROM (l.l_creationdate - m_creationdate)) / 60 as lag,

(CASE WHEN EXISTS (
                SELECT 1
                FROM knows
                WHERE k_person1id = 21990232556256 AND k_person2id = p_personid
            ) THEN 0 ELSE 1 END) AS isnew
FROM (
        SELECT l_personid, max(l_creationdate) AS l_creationdate
        FROM likes, message
        WHERE m_messageid = l_messageid AND m_creatorid = 21990232556256
        GROUP BY l_personid
        ORDER BY 2 DESC
        LIMIT 20
    ) tmp,
    message,
    person,
    likes AS l
WHERE p_personid = tmp.l_personid
  AND tmp.l_personid = l.l_personid
  AND tmp.l_creationdate = l.l_creationdate
  AND l.l_messageid = m_messageid
ORDER BY 4 DESC, 1
