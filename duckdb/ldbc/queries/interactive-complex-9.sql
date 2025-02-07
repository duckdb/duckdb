select p_personid, p_firstname, p_lastname,
       m_messageid, COALESCE(m_ps_imagefile,'')||COALESCE(m_content,'') AS content, m_creationdate
from
  ( select k_person2id
    from knows
    where
    k_person1id = 21990232556256
    union
    select k2.k_person2id
    from knows k1, knows k2
    where
    k1.k_person1id = 21990232556256 and k1.k_person2id = k2.k_person1id and k2.k_person2id <> 21990232556256
  ) f, person, message
where
  p_personid = m_creatorid and p_personid = f.k_person2id and
  m_creationdate < '2012-07-26T22:00:00'
order by m_creationdate desc, m_messageid asc
limit 20
