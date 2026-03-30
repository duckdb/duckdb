/* Q3. Popular topics in a country
\set tagClass '\'MusicalArtist\''
\set country  '\'Burma\''
 */
SELECT f.f_forumid      AS "forum.id"
     , f.f_title        AS "forum.title"
     , f.f_creationdate AS "forum.creationDate"
     , f.f_moderatorid  AS "person.id"
     , count(DISTINCT p.m_messageid) AS postCount
  FROM tagClass tc
     , tag t
     , message_tag pt
     , message p
     , forum f
     , person m   -- moderator
     , place  ci  -- city
     , place  co  -- country
 WHERE 1=1
    -- join
   AND tc.tc_tagclassid = t.t_tagclassid
   AND t.t_tagid = pt.mt_tagid
   AND pt.mt_messageid = p.m_messageid
   AND p.m_ps_forumid = f.f_forumid
   AND f.f_moderatorid = m.p_personid
   AND m.p_placeid = ci.pl_placeid
   AND ci.pl_containerplaceid = co.pl_placeid
    -- filter
   AND tc.tc_name = 'MusicalArtist'
   AND co.pl_name = 'Burma'
 GROUP BY f.f_forumid, f.f_title, f.f_creationdate, f.f_moderatorid
 ORDER BY postCount DESC, f.f_forumid
 LIMIT 20
;