/* Q5. Most active Posters of a given Topic
\set tag '\'Abbas_I_of_Persia\''
 */
WITH detail AS (
SELECT cr.p_personid AS person_id
     , count(DISTINCT r.m_messageid)  AS replyCount
     , count(DISTINCT l.l_messageid||' '||l.l_personid) AS likeCount
     , count(DISTINCT m.m_messageid)  AS messageCount
     , null as score
  FROM tag t
     , message_tag pt
     , message m LEFT JOIN message  r ON (m.m_messageid = r.m_c_replyof)
              LEFT JOIN likes l ON (m.m_messageid = l.l_messageid)  -- l: likes to m
     , person cr -- creator
 WHERE 1=1
    -- join
   AND t.t_tagid = pt.mt_tagid
   AND pt.mt_messageid = m.m_messageid
   AND m.m_creatorid = cr.p_personid
    -- filter
   AND t.t_name = 'Abbas_I_of_Persia'
 GROUP BY cr.p_personid
)
SELECT person_id AS "person.id"
     , replyCount
     , likeCount
     , messageCount
     , 1*messageCount + 2*replyCount + 10*likeCount AS score
  FROM detail
 ORDER BY score DESC, person_id
 LIMIT 100
;
