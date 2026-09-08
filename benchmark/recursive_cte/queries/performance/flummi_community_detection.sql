WITH RECURSIVE
  "🔄"("#️⃣", "🏷️", "📊.1", "node", "community", "state") AS (
    (SELECT CAST((0) AS INTEGER) AS "#️⃣",
            CAST(('start.1') AS VARCHAR) AS "🏷️",
            CAST((NULL) AS INTEGER[]) AS "📊.1",
            CAST((NULL) AS INTEGER) AS "node",
            CAST((NULL) AS INTEGER) AS "community",
            CAST((NULL) AS INTEGER) AS "state")
      UNION ALL
    (WITH
       "start.1"("#️⃣", "⚙️") AS (
         SELECT "🔄"."#️⃣" AS "#️⃣",
                NULL AS "⚙️"
         FROM   "🔄"
         WHERE  "🔄"."🏷️" IS NOT DISTINCT FROM 'start.1'
       ),
       "fork.1"("#️⃣", "⚙️", "node") AS (
         SELECT "start.1"."#️⃣" AS "#️⃣",
                "start.1"."⚙️" AS "⚙️",
                CAST(("ℚ"."node") AS INTEGER) AS "node"
         FROM   "start.1",
                (FROM nodes) AS "ℚ"("node")
       ),
       "assignment.1"("#️⃣", "⚙️", "node", "community") AS (
         SELECT "fork.1"."#️⃣" AS "#️⃣",
                "fork.1"."⚙️" AS "⚙️",
                "fork.1"."node" AS "node",
                CAST((("fork.1"."node")) AS INTEGER) AS "community"
         FROM   "fork.1"
       ),
       "assignment.2"("#️⃣", "⚙️", "node", "community", "state") AS (
         SELECT "assignment.1"."#️⃣" AS "#️⃣",
                "assignment.1"."⚙️" AS "⚙️",
                "assignment.1"."node" AS "node",
                "assignment.1"."community" AS "community",
                CAST((bit_xor(("assignment.1"."community")) OVER ()) AS INTEGER) AS "state"
         FROM   "assignment.1"
       ),
       "start.2"("#️⃣", "⚙️", "node", "community", "state") AS (
         SELECT "🔄"."#️⃣" AS "#️⃣",
                NULL AS "⚙️",
                "🔄"."node" AS "node",
                "🔄"."community" AS "community",
                "🔄"."state" AS "state"
         FROM   "🔄"
         WHERE  "🔄"."🏷️" IS NOT DISTINCT FROM 'start.2'
       ),
       "merge.1"("#️⃣", "⚙️", "node", "community", "state") AS (
         (SELECT "assignment.2"."#️⃣" AS "#️⃣",
                 "assignment.2"."⚙️" AS "⚙️",
                 "assignment.2"."node" AS "node",
                 "assignment.2"."community" AS "community",
                 "assignment.2"."state" AS "state"
          FROM   "assignment.2")
           UNION ALL
         (SELECT "start.2"."#️⃣" AS "#️⃣",
                 "start.2"."⚙️" AS "⚙️",
                 "start.2"."node" AS "node",
                 "start.2"."community" AS "community",
                 "start.2"."state" AS "state"
          FROM   "start.2")
       ),
       "fork.2"("#️⃣", "⚙️", "node", "community", "state", "flip") AS (
         SELECT "merge.1"."#️⃣" AS "#️⃣",
                "merge.1"."⚙️" AS "⚙️",
                "merge.1"."node" AS "node",
                "merge.1"."community" AS "community",
                "merge.1"."state" AS "state",
                CAST(("ℚ"."flip") AS BOOLEAN) AS "flip"
         FROM   "merge.1",
                (VALUES (TRUE), (FALSE)) AS "ℚ"("flip")
       ),
       "assignment.3"("#️⃣", "⚙️", "🔍", "node", "community", "state") AS (
         SELECT "fork.2"."#️⃣" AS "#️⃣",
                "fork.2"."⚙️" AS "⚙️",
                CAST((("fork.2"."flip")) AS BOOLEAN) AS "🔍",
                "fork.2"."node" AS "node",
                "fork.2"."community" AS "community",
                "fork.2"."state" AS "state"
         FROM   "fork.2"
       ),
       "where.1"("#️⃣", "⚙️", "node", "community", "state") AS (
         SELECT "assignment.3"."#️⃣" AS "#️⃣",
                "assignment.3"."⚙️" AS "⚙️",
                "assignment.3"."node" AS "node",
                "assignment.3"."community" AS "community",
                "assignment.3"."state" AS "state"
         FROM   "assignment.3"
         WHERE  "assignment.3"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "fork.3"("#️⃣", "⚙️", "node", "community", "state") AS (
         SELECT "where.1"."#️⃣" AS "#️⃣",
                "where.1"."⚙️" AS "⚙️",
                CAST(("ℚ"."node") AS INTEGER) AS "node",
                "where.1"."community" AS "community",
                "where.1"."state" AS "state"
         FROM   "where.1",
                LATERAL (SELECT there FROM edges WHERE here = ("where.1"."node")
                             UNION ALL
                         SELECT here FROM edges WHERE there = ("where.1"."node")) AS "ℚ"("node")
       ),
       "gather.1"("#️⃣", "⚙️", "node", "community", "state") AS (
         SELECT "fork.3"."#️⃣" AS "#️⃣",
                "fork.3"."⚙️" AS "⚙️",
                "fork.3"."node" AS "node",
                CAST((mode(("fork.3"."community") ORDER BY ("fork.3"."community"))) AS INTEGER) AS "community",
                "fork.3"."state" AS "state"
         FROM   "fork.3"
         GROUP  BY "fork.3"."#️⃣",
                   "fork.3"."node",
                   "fork.3"."state",
                   "fork.3"."⚙️"
         HAVING COUNT(*) > 0
       ),
       "where.2"("#️⃣", "⚙️", "node", "community", "state") AS (
         SELECT "assignment.3"."#️⃣" AS "#️⃣",
                "assignment.3"."⚙️" AS "⚙️",
                "assignment.3"."node" AS "node",
                "assignment.3"."community" AS "community",
                "assignment.3"."state" AS "state"
         FROM   "assignment.3"
         WHERE  "assignment.3"."🔍" IS DISTINCT FROM TRUE
       ),
       "merge.2"("#️⃣", "⚙️", "node", "community", "state") AS (
         (SELECT "gather.1"."#️⃣" AS "#️⃣",
                 "gather.1"."⚙️" AS "⚙️",
                 "gather.1"."node" AS "node",
                 "gather.1"."community" AS "community",
                 "gather.1"."state" AS "state"
          FROM   "gather.1")
           UNION ALL
         (SELECT "where.2"."#️⃣" AS "#️⃣",
                 "where.2"."⚙️" AS "⚙️",
                 "where.2"."node" AS "node",
                 "where.2"."community" AS "community",
                 "where.2"."state" AS "state"
          FROM   "where.2")
       ),
       "gather.2"("#️⃣", "⚙️", "node", "community", "state") AS (
         SELECT "merge.2"."#️⃣" AS "#️⃣",
                "merge.2"."⚙️" AS "⚙️",
                "merge.2"."node" AS "node",
                CAST((min(("merge.2"."community"))) AS INTEGER) AS "community",
                "merge.2"."state" AS "state"
         FROM   "merge.2"
         GROUP  BY "merge.2"."#️⃣",
                   "merge.2"."node",
                   "merge.2"."state",
                   "merge.2"."⚙️"
         HAVING COUNT(*) > 0
       ),
       "assignment.4"("#️⃣", "⚙️", "node", "community", "state", "old_state") AS (
         SELECT "gather.2"."#️⃣" AS "#️⃣",
                "gather.2"."⚙️" AS "⚙️",
                "gather.2"."node" AS "node",
                "gather.2"."community" AS "community",
                CAST((bit_xor(("gather.2"."community")) OVER ()) AS INTEGER) AS "state",
                CAST((("gather.2"."state")) AS INTEGER) AS "old_state"
         FROM   "gather.2"
       ),
       "assignment.5"("#️⃣", "⚙️", "🔍", "node", "community", "state") AS (
         SELECT "assignment.4"."#️⃣" AS "#️⃣",
                "assignment.4"."⚙️" AS "⚙️",
                CAST((("assignment.4"."old_state") = ("assignment.4"."state")) AS BOOLEAN) AS "🔍",
                "assignment.4"."node" AS "node",
                "assignment.4"."community" AS "community",
                "assignment.4"."state" AS "state"
         FROM   "assignment.4"
       ),
       "where.3"("#️⃣", "⚙️", "node", "community") AS (
         SELECT "assignment.5"."#️⃣" AS "#️⃣",
                "assignment.5"."⚙️" AS "⚙️",
                "assignment.5"."node" AS "node",
                "assignment.5"."community" AS "community"
         FROM   "assignment.5"
         WHERE  "assignment.5"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "gather.3"("#️⃣", "⚙️", "nodes") AS (
         SELECT "where.3"."#️⃣" AS "#️⃣",
                "where.3"."⚙️" AS "⚙️",
                CAST((list(("where.3"."node") ORDER BY ("where.3"."node"))) AS INTEGER[]) AS "nodes"
         FROM   "where.3"
         GROUP  BY "where.3"."#️⃣",
                   "where.3"."community",
                   "where.3"."⚙️"
         HAVING COUNT(*) > 0
       ),
       "emit.1"("#️⃣", "⚙️", "📊.1") AS (
         SELECT "gather.3"."#️⃣" AS "#️⃣",
                "gather.3"."⚙️" AS "⚙️",
                "gather.3"."nodes" AS "📊.1"
         FROM   "gather.3"
       ),
       "stop.1"("⚙️") AS (
         SELECT "emit.1"."⚙️"
         FROM   "emit.1"
         WHERE  FALSE
       ),
       "where.4"("#️⃣", "⚙️", "node", "community", "state") AS (
         SELECT "assignment.5"."#️⃣" AS "#️⃣",
                "assignment.5"."⚙️" AS "⚙️",
                "assignment.5"."node" AS "node",
                "assignment.5"."community" AS "community",
                "assignment.5"."state" AS "state"
         FROM   "assignment.5"
         WHERE  "assignment.5"."🔍" IS DISTINCT FROM TRUE
       ),
       "jump.1"("#️⃣", "🏷️", "node", "community", "state") AS (
         SELECT "where.4"."#️⃣" AS "#️⃣",
                'start.2' AS "🏷️",
                "where.4"."node" AS "node",
                "where.4"."community" AS "community",
                "where.4"."state" AS "state"
         FROM   "where.4"
       )
     (SELECT CAST(("emit.1"."#️⃣") AS INTEGER) AS "#️⃣",
             CAST((NULL) AS VARCHAR) AS "🏷️",
             CAST(("emit.1"."📊.1") AS INTEGER[]) AS "📊.1",
             CAST((NULL) AS INTEGER) AS "node",
             CAST((NULL) AS INTEGER) AS "community",
             CAST((NULL) AS INTEGER) AS "state"
      FROM   "emit.1")
       UNION ALL
     (SELECT CAST(("jump.1"."#️⃣" + 1) AS INTEGER) AS "#️⃣",
             CAST(("jump.1"."🏷️") AS VARCHAR) AS "🏷️",
             CAST((NULL) AS INTEGER[]) AS "📊.1",
             CAST(("jump.1"."node") AS INTEGER) AS "node",
             CAST(("jump.1"."community") AS INTEGER) AS "community",
             CAST(("jump.1"."state") AS INTEGER) AS "state"
      FROM   "jump.1"))
  )
SELECT count(*) AS communities,
       sum(len("🔄"."📊.1"))::BIGINT AS nodes,
       sum(list_sum("🔄"."📊.1"))::BIGINT AS node_sum
FROM   "🔄"
WHERE  "🔄"."🏷️" IS NULL;
