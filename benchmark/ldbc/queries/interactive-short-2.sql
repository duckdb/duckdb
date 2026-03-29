WITH RECURSIVE cposts(
    m_messageid,
    m_content,
    m_ps_imagefile,
    m_creationdate,
    m_c_replyof,
    m_creatorid) AS (
        SELECT m_messageid, m_content, m_ps_imagefile, m_creationdate, m_c_replyof, m_creatorid
        FROM message
        WHERE m_creatorid = 21990232556256
        ORDER BY m_creationdate DESC
        LIMIT 10
    ),
    parent(postid, replyof, orig_postid, creator) AS (
        SELECT m_messageid, m_c_replyof, m_messageid, m_creatorid
        FROM cposts
        UNION ALL
        SELECT m_messageid, m_c_replyof, orig_postid, m_creatorid
        FROM message, parent
        WHERE m_messageid = replyof
    )
SELECT
    p1.m_messageid,
    COALESCE(m_ps_imagefile, '') || COALESCE(m_content, '') AS content,
    p1.m_creationdate,
    p2.m_messageid,
    p2.p_personid,
    p2.p_firstname,
    p2.p_lastname
FROM (
        SELECT m_messageid, m_content, m_ps_imagefile, m_creationdate, m_c_replyof
        FROM cposts
    ) p1
LEFT JOIN (
        SELECT orig_postid, postid AS m_messageid, p_personid, p_firstname, p_lastname
        FROM parent, person
        WHERE replyof IS NULL AND creator = p_personid
    ) p2 ON p2.orig_postid = p1.m_messageid
ORDER BY m_creationdate DESC, p2.m_messageid DESC;
