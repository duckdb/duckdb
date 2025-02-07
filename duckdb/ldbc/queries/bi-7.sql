/* Q7. Related Topics
\set tag '\'Enrique_Iglesias\''
 */
SELECT t2.t_name AS "relatedTag.name"
     , count(*) AS count
  FROM tag t INNER JOIN message_tag pt ON (t.t_tagid = pt.mt_tagid)
             -- as an optimization, we don't need message here as it's ID is in message_tag pt
             -- so proceed to the comment directly
             INNER JOIN message c      ON (pt.mt_messageid = c.m_c_replyof)
             -- comment's tag
             INNER JOIN message_tag ct ON (c.m_messageid = ct.mt_messageid)
             INNER JOIN tag t2      ON (ct.mt_tagid = t2.t_tagid)
             -- comment doesn't have the given tag: antijoin in the where clause
             LEFT  JOIN message_tag nt ON (c.m_messageid = nt.mt_messageid AND nt.mt_tagid = pt.mt_tagid)
 WHERE 1=1
    -- join
   AND nt.mt_messageid IS NULL -- antijoin: comment (c) does not have the given tag
    -- filter
   AND t.t_name = 'Enrique_Iglesias'
 GROUP BY t2.t_name
 ORDER BY count DESC, t2.t_name
 LIMIT 100
;
