WITH RECURSIVE
  "🔄"("#️⃣", "🏷️", "📊.1", "📊.2", "node", "depth") AS (
    (SELECT CAST((0) AS INTEGER) AS "#️⃣",
            CAST(('start.1') AS VARCHAR) AS "🏷️",
            CAST((NULL) AS INTEGER) AS "📊.1",
            CAST((NULL) AS INTEGER) AS "📊.2",
            CAST((NULL) AS INTEGER) AS "node",
            CAST((NULL) AS INTEGER) AS "depth")
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
       "assignment.1"("#️⃣", "⚙️", "🔍", "node") AS (
         SELECT "fork.1"."#️⃣" AS "#️⃣",
                "fork.1"."⚙️" AS "⚙️",
                CAST((("fork.1"."node") = 1) AS BOOLEAN) AS "🔍",
                "fork.1"."node" AS "node"
         FROM   "fork.1"
       ),
       "start.2"("#️⃣", "⚙️", "node", "depth") AS (
         SELECT "🔄"."#️⃣" AS "#️⃣",
                NULL AS "⚙️",
                "🔄"."node" AS "node",
                "🔄"."depth" AS "depth"
         FROM   "🔄"
         WHERE  "🔄"."🏷️" IS NOT DISTINCT FROM 'start.2'
       ),
       "where.1"("#️⃣", "⚙️", "node") AS (
         SELECT "assignment.1"."#️⃣" AS "#️⃣",
                "assignment.1"."⚙️" AS "⚙️",
                "assignment.1"."node" AS "node"
         FROM   "assignment.1"
         WHERE  "assignment.1"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "assignment.2"("#️⃣", "⚙️", "node", "depth") AS (
         SELECT "where.1"."#️⃣" AS "#️⃣",
                "where.1"."⚙️" AS "⚙️",
                "where.1"."node" AS "node",
                CAST((0) AS INTEGER) AS "depth"
         FROM   "where.1"
       ),
       "where.2"("#️⃣", "⚙️", "node") AS (
         SELECT "assignment.1"."#️⃣" AS "#️⃣",
                "assignment.1"."⚙️" AS "⚙️",
                "assignment.1"."node" AS "node"
         FROM   "assignment.1"
         WHERE  "assignment.1"."🔍" IS DISTINCT FROM TRUE
       ),
       "assignment.3"("#️⃣", "⚙️", "node", "depth") AS (
         SELECT "where.2"."#️⃣" AS "#️⃣",
                "where.2"."⚙️" AS "⚙️",
                "where.2"."node" AS "node",
                CAST((NULL) AS INTEGER) AS "depth"
         FROM   "where.2"
       ),
       "merge.1"("#️⃣", "⚙️", "node", "depth") AS (
         (SELECT "assignment.2"."#️⃣" AS "#️⃣",
                 "assignment.2"."⚙️" AS "⚙️",
                 "assignment.2"."node" AS "node",
                 "assignment.2"."depth" AS "depth"
          FROM   "assignment.2")
           UNION ALL
         (SELECT "assignment.3"."#️⃣" AS "#️⃣",
                 "assignment.3"."⚙️" AS "⚙️",
                 "assignment.3"."node" AS "node",
                 "assignment.3"."depth" AS "depth"
          FROM   "assignment.3")
       ),
       "merge.2"("#️⃣", "⚙️", "node", "depth") AS (
         (SELECT "merge.1"."#️⃣" AS "#️⃣",
                 "merge.1"."⚙️" AS "⚙️",
                 "merge.1"."node" AS "node",
                 "merge.1"."depth" AS "depth"
          FROM   "merge.1")
           UNION ALL
         (SELECT "start.2"."#️⃣" AS "#️⃣",
                 "start.2"."⚙️" AS "⚙️",
                 "start.2"."node" AS "node",
                 "start.2"."depth" AS "depth"
          FROM   "start.2")
       ),
       "fork.2"("#️⃣", "⚙️", "node", "depth", "original") AS (
         SELECT "merge.2"."#️⃣" AS "#️⃣",
                "merge.2"."⚙️" AS "⚙️",
                CAST(("ℚ"."node") AS INTEGER) AS "node",
                CAST(("ℚ"."depth") AS INTEGER) AS "depth",
                CAST(("ℚ"."original") AS BOOLEAN) AS "original"
         FROM   "merge.2",
                LATERAL (SELECT ("merge.2"."node"), ("merge.2"."depth"), true
                             UNION ALL
                         SELECT there, ("merge.2"."depth") + 1, false
                         FROM   edges AS e
                         WHERE  here = ("merge.2"."node")
                         AND    ("merge.2"."depth") IS NOT NULL) AS "ℚ"("node", "depth", "original")
       ),
       "gather.1"("#️⃣", "⚙️", "node", "depth", "old_depth") AS (
         SELECT "fork.2"."#️⃣" AS "#️⃣",
                "fork.2"."⚙️" AS "⚙️",
                "fork.2"."node" AS "node",
                CAST((min(("fork.2"."depth"))) AS INTEGER) AS "depth",
                CAST((first(("fork.2"."depth")) FILTER (WHERE ("fork.2"."original"))) AS INTEGER) AS "old_depth"
         FROM   "fork.2"
         GROUP  BY "fork.2"."#️⃣",
                   "fork.2"."node",
                   "fork.2"."⚙️"
         HAVING COUNT(*) > 0
       ),
       "assignment.4"("#️⃣", "⚙️", "🔍", "node", "depth") AS (
         SELECT "gather.1"."#️⃣" AS "#️⃣",
                "gather.1"."⚙️" AS "⚙️",
                CAST((bool_and(("gather.1"."depth") IS NOT DISTINCT FROM ("gather.1"."old_depth")) OVER ()) AS BOOLEAN) AS "🔍",
                "gather.1"."node" AS "node",
                "gather.1"."depth" AS "depth"
         FROM   "gather.1"
       ),
       "where.3"("#️⃣", "⚙️", "node", "depth") AS (
         SELECT "assignment.4"."#️⃣" AS "#️⃣",
                "assignment.4"."⚙️" AS "⚙️",
                "assignment.4"."node" AS "node",
                "assignment.4"."depth" AS "depth"
         FROM   "assignment.4"
         WHERE  "assignment.4"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "emit.1"("#️⃣", "⚙️", "📊.1", "📊.2") AS (
         SELECT "where.3"."#️⃣" AS "#️⃣",
                "where.3"."⚙️" AS "⚙️",
                "where.3"."node" AS "📊.1",
                "where.3"."depth" AS "📊.2"
         FROM   "where.3"
       ),
       "stop.1"("⚙️") AS (
         SELECT "emit.1"."⚙️"
         FROM   "emit.1"
         WHERE  FALSE
       ),
       "where.4"("#️⃣", "⚙️", "node", "depth") AS (
         SELECT "assignment.4"."#️⃣" AS "#️⃣",
                "assignment.4"."⚙️" AS "⚙️",
                "assignment.4"."node" AS "node",
                "assignment.4"."depth" AS "depth"
         FROM   "assignment.4"
         WHERE  "assignment.4"."🔍" IS DISTINCT FROM TRUE
       ),
       "jump.1"("#️⃣", "🏷️", "node", "depth") AS (
         SELECT "where.4"."#️⃣" AS "#️⃣",
                'start.2' AS "🏷️",
                "where.4"."node" AS "node",
                "where.4"."depth" AS "depth"
         FROM   "where.4"
       )
     (SELECT CAST(("emit.1"."#️⃣") AS INTEGER) AS "#️⃣",
             CAST((NULL) AS VARCHAR) AS "🏷️",
             CAST(("emit.1"."📊.1") AS INTEGER) AS "📊.1",
             CAST(("emit.1"."📊.2") AS INTEGER) AS "📊.2",
             CAST((NULL) AS INTEGER) AS "node",
             CAST((NULL) AS INTEGER) AS "depth"
      FROM   "emit.1")
       UNION ALL
     (SELECT CAST(("jump.1"."#️⃣" + 1) AS INTEGER) AS "#️⃣",
             CAST(("jump.1"."🏷️") AS VARCHAR) AS "🏷️",
             CAST((NULL) AS INTEGER) AS "📊.1",
             CAST((NULL) AS INTEGER) AS "📊.2",
             CAST(("jump.1"."node") AS INTEGER) AS "node",
             CAST(("jump.1"."depth") AS INTEGER) AS "depth"
      FROM   "jump.1"))
  )
SELECT count(*) AS nodes,
       count("🔄"."📊.2") AS reached,
       coalesce(max("🔄"."📊.2"), -1) AS max_depth,
       coalesce(sum("🔄"."📊.2"), 0)::BIGINT AS depth_sum
FROM   "🔄"
WHERE  "🔄"."🏷️" IS NULL;
