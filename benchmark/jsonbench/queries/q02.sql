SELECT j.commit.collection AS event,count() AS count,count(DISTINCT j.did) AS users FROM bluesky WHERE (j.kind = 'commit') AND (j.commit.operation = 'create') GROUP BY event ORDER BY count DESC;
