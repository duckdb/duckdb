select p_personid, p_firstname, p_lastname
from message, person
where m_messageid = 687194767741 and m_creatorid = p_personid;
