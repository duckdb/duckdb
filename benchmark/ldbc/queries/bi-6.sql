/* Q6. Most authoritative users on a given topic
\set tag '\'Arnold_Schwarzenegger\''
 */
WITH poster_w_liker AS (
  SELECT DISTINCT
         m1.m_creatorid posterPersonid
       , l2.l_personid as likerPersonid
    FROM tag t
       , message_tag pt
       -- as an optimization, we use that the set of message1 is the same as message2
       , message m1 LEFT JOIN likes l2 ON (m1.m_messageid = l2.l_messageid)
       --, person p2 -- we don't need the person itself as its ID is in the like l2
   WHERE 1=1
      -- join
     AND t.t_tagid = pt.mt_tagid
     AND pt.mt_messageid = m1.m_messageid
      -- filter
     AND t.t_name = 'Arnold_Schwarzenegger'
)
, popularity_score AS (
  SELECT m3.m_creatorid as personid, count(*) as popularityScore
    FROM message m3
       , likes l3
   WHERE 1=1
      -- join
     AND m3.m_messageid = l3.l_messageid
   GROUP BY personId
)
SELECT pl.posterPersonid as "person1.id"
     , sum(coalesce(ps.popularityScore, 0)) as authorityScore
  FROM poster_w_liker pl LEFT JOIN popularity_score ps ON (pl.likerPersonid = ps.personid)
 GROUP BY pl.posterPersonid
 ORDER BY authorityScore DESC, pl.posterPersonid ASC
 LIMIT 100
;
