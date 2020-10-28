select COALESCE(m_ps_imagefile,'')||COALESCE(m_content,'') AS content, m_creationdate
from message
where m_messageid = 687194767741;
