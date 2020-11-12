select f_title, count(m_messageid)
from (
select f_title, f_forumid, f.k_person2id
from forum, forum_person,
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
where f_forumid = fp_forumid and fp_personid = f.k_person2id and
      fp_creationdate >= '2011-07-21T22:00:00'
) tmp left join message
on tmp.f_forumid = m_ps_forumid and m_creatorid = tmp.k_person2id
group by f_forumid, f_title
order by 2 desc, f_forumid
limit 20
