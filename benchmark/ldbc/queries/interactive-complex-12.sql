WITH RECURSIVE extended_tags(s_subtagclassid, s_supertagclassid) AS (
        SELECT tc_tagclassid, tc_tagclassid
        FROM tagclass
        UNION
        SELECT tc.tc_tagclassid, t.s_supertagclassid
        FROM tagclass tc, extended_tags t
        WHERE tc.tc_subclassoftagclassid = t.s_subtagclassid
    )
SELECT
    p_personid,
    p_firstname,
    p_lastname,
    array_agg(DISTINCT t_name),
    count(*)
FROM
    person,
    message p1,
    knows,
    message p2,
    message_tag,
    (
        SELECT DISTINCT t_tagid, t_name
        FROM tag
        WHERE (t_tagclassid IN (
                        SELECT DISTINCT s_subtagclassid
                        FROM extended_tags k, tagclass
                        WHERE
                            tc_tagclassid = k.s_supertagclassid
                            AND tc_name = 'OfficeHolder'
                    ) -- :tagClassName

)
    ) selected_tags
WHERE k_person1id = 21990232556256
  AND k_person2id = p_personid
  AND p_personid = p1.m_creatorid
  AND p1.m_c_replyof = p2.m_messageid
  AND p2.m_c_replyof IS NULL
  AND p2.m_messageid = mt_messageid
  AND mt_tagid = t_tagid
GROUP BY p_personid, p_firstname, p_lastname
ORDER BY 5 DESC, 1
LIMIT 20
