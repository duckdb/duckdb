SELECT p_personid, p_firstname, p_lastname
FROM message, person
WHERE m_messageid = 687194767741
  AND m_creatorid = p_personid;
