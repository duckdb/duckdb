SELECT
    COALESCE(m_ps_imagefile, '') || COALESCE(m_content, '') AS content,
    m_creationdate
FROM message
WHERE m_messageid = 687194767741;
