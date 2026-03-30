/* Q14. International dialog
\set country1 '\'Indonesia\''
\set country2 '\'Brazil\''
 */
WITH person1_list AS (
    SELECT p.p_personid AS personid
         , ci.pl_placeid AS cityid
      FROM place co -- country
         , place ci -- city
         , person p
     WHERE 1=1
        -- join
       AND co.pl_placeid = ci.pl_containerplaceid
       AND ci.pl_placeid = p.p_placeid
        -- filter
       AND co.pl_name = 'Indonesia'
)
,  person2_list AS (
    SELECT p.p_personid AS personid
      FROM place co -- country
         , place ci -- city
         , person p
     WHERE 1=1
        -- join
       AND co.pl_placeid = ci.pl_containerplaceid
       AND ci.pl_placeid = p.p_placeid
        -- filter
       AND co.pl_name = 'Brazil'
)
,  case1 AS (
    SELECT DISTINCT
           p1.personid AS person1id
         , p2.personid AS person2id
         , 4 AS score
      FROM person1_list p1
         , person2_list p2
         , message m -- message by p2
         , message r -- reply by p1
     WHERE 1=1
        -- join
       AND m.m_messageid = r.m_c_replyof
       AND p1.personid = r.m_creatorid
       AND p2.personid = m.m_creatorid
)
,  case2 AS (
    SELECT DISTINCT
           p1.personid AS person1id
         , p2.personid AS person2id
         , 1 AS score
      FROM person1_list p1
         , person2_list p2
         , message m -- message by p1
         , message r -- reply by p2
     WHERE 1=1
        -- join
       AND m.m_messageid = r.m_c_replyof
       AND p2.personid = r.m_creatorid
       AND p1.personid = m.m_creatorid
)
,  case3 AS (
    SELECT -- no need for distinct
           p1.personid AS person1id
         , p2.personid AS person2id
         , 15 AS score
      FROM person1_list p1
         , person2_list p2
         , knows k
     WHERE 1=1
        -- join
       AND p1.personid = k.k_person1id
       AND p2.personid = k.k_person2id
)
,  case4 AS (
    SELECT DISTINCT
           p1.personid AS person1id
         , p2.personid AS person2id
         , 10 AS score
      FROM person1_list p1
         , person2_list p2
         , message m -- message by p2
         , likes l
     WHERE 1=1
        -- join
       AND p2.personid = m.m_creatorid
       AND m.m_messageid = l.l_messageid
       AND l.l_personid = p1.personid
)
,  case5 AS (
    SELECT DISTINCT
           p1.personid AS person1id
         , p2.personid AS person2id
         , 1 AS score
      FROM person1_list p1
         , person2_list p2
         , message m -- message by p1
         , likes l
     WHERE 1=1
        -- join
       AND p1.personid = m.m_creatorid
       AND m.m_messageid = l.l_messageid
       AND l.l_personid = p2.personid
)
,  pair_scores AS (
    SELECT person1id, person2id, sum(score) AS score
      FROM (SELECT * FROM case1
            UNION ALL SELECT * FROM case2
            UNION ALL SELECT * FROM case3
            UNION ALL SELECT * FROM case4
            UNION ALL SELECT * FROM case5
           ) t
     GROUP BY person1id, person2id
)
,  score_ranks AS (
    SELECT s.person1id
         , s.person2id
         , ci.pl_name AS cityName
         , s.score
         , row_number() OVER (PARTITION BY ci.pl_placeid ORDER BY s.score DESC NULLS LAST, s.person1id, s.person2id) AS rn
      FROM place co -- country
           INNER JOIN place ci ON (co.pl_placeid = ci.pl_containerplaceid) -- city
           LEFT  JOIN person1_list p1l ON (ci.pl_placeid = p1l.cityid)
           LEFT  JOIN pair_scores s ON (p1l.personid = s.person1id)
     WHERE 1=1
        -- filter
       AND co.pl_name = 'Indonesia'
)
SELECT s.person1id AS "person1.id"
     , s.person2id AS "person2.id"
     , s.cityName AS "city1.name"
     , s.score
  FROM score_ranks s
 WHERE 1=1
    -- filter
   AND s.rn = 1
 ORDER BY s.score DESC, s.person1id, s.person2id
 LIMIT 100
;
