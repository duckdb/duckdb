select t_name, count(*)
from tag, message_tag, message,
 ( select k_person2id
   from knows
   where
   k_person1id = 21990232556256
   union
   select k2.k_person2id
   from knows k1, knows k2
   where
   k1.k_person1id = 21990232556256 and k1.k_person2id = k2.k_person1id and k2.k_person2id <> 21990232556256
 ) f
where
m_creatorid = f.k_person2id and
m_c_replyof IS NULL and -- post, not comment
m_messageid = mt_messageid and
mt_tagid = t_tagid and
t_name <> 'Hamid_Karzai' and
exists (select * from tag, message_tag where mt_messageid = m_messageid and mt_tagid = t_tagid and t_name = 'Hamid_Karzai')
group by t_name
order by 2 desc, t_name
limit 10
