SELECT t_name, count(*)
FROM tag, message, message_tag recent, knows
WHERE m_messageid = mt_messageid
  AND mt_tagid = t_tagid
  AND m_creatorid = k_person2id
  AND m_c_replyof IS NULL
  AND -- post, not comment

k_person1id = 21990232556256
    AND m_creationdate >= '2011-07-21T22:00:00'
    AND m_creationdate < '2012-07-26T22:00:00'
    AND --('2011-07-21T22:00:00' + INTERVAL '1 days' * 5)

NOT EXISTS (
        SELECT *
        FROM (
                SELECT DISTINCT mt_tagid
                FROM message, message_tag, knows
                WHERE k_person1id = 21990232556256
                  AND k_person2id = m_creatorid
                  AND m_c_replyof IS NULL
                  AND -- post, not comment

mt_messageid = m_messageid
                    AND m_creationdate < '2011-07-21T22:00:00'
            ) tags
        WHERE tags.mt_tagid = recent.mt_tagid
    )
GROUP BY t_name
ORDER BY 2 DESC, t_name
LIMIT 10
