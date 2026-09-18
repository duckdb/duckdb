WITH RECURSIVE
  "🔄"("#️⃣", "🏷️", "📊.1", "k", "node", "active", "size") AS (
    (SELECT CAST((0) AS INTEGER) AS "#️⃣",
            CAST(('start.1') AS VARCHAR) AS "🏷️",
            CAST((NULL) AS INTEGER) AS "📊.1",
            CAST((NULL) AS INTEGER) AS "k",
            CAST((NULL) AS INTEGER) AS "node",
            CAST((NULL) AS BOOLEAN) AS "active",
            CAST((NULL) AS BIGINT) AS "size")
      UNION ALL
    (WITH
       "start.1"("#️⃣", "⚙️") AS (
         SELECT "🔄"."#️⃣" AS "#️⃣",
                NULL AS "⚙️"
         FROM   "🔄"
         WHERE  "🔄"."🏷️" IS NOT DISTINCT FROM 'start.1'
       ),
       "assignment.1"("#️⃣", "⚙️", "k") AS (
         SELECT "start.1"."#️⃣" AS "#️⃣",
                "start.1"."⚙️" AS "⚙️",
                CAST((2) AS INTEGER) AS "k"
         FROM   "start.1"
       ),
       "fork.1"("#️⃣", "⚙️", "k", "node") AS (
         SELECT "assignment.1"."#️⃣" AS "#️⃣",
                "assignment.1"."⚙️" AS "⚙️",
                "assignment.1"."k" AS "k",
                CAST(("ℚ"."node") AS INTEGER) AS "node"
         FROM   "assignment.1",
                (FROM nodes) AS "ℚ"("node")
       ),
       "assignment.2"("#️⃣", "⚙️", "k", "node", "active") AS (
         SELECT "fork.1"."#️⃣" AS "#️⃣",
                "fork.1"."⚙️" AS "⚙️",
                "fork.1"."k" AS "k",
                "fork.1"."node" AS "node",
                CAST((TRUE) AS BOOLEAN) AS "active"
         FROM   "fork.1"
       ),
       "assignment.3"("#️⃣", "⚙️", "k", "node", "active", "size") AS (
         SELECT "assignment.2"."#️⃣" AS "#️⃣",
                "assignment.2"."⚙️" AS "⚙️",
                "assignment.2"."k" AS "k",
                "assignment.2"."node" AS "node",
                "assignment.2"."active" AS "active",
                CAST((count(*) OVER ()) AS BIGINT) AS "size"
         FROM   "assignment.2"
       ),
       "start.2"("#️⃣", "⚙️", "k", "node", "active", "size") AS (
         SELECT "🔄"."#️⃣" AS "#️⃣",
                NULL AS "⚙️",
                "🔄"."k" AS "k",
                "🔄"."node" AS "node",
                "🔄"."active" AS "active",
                "🔄"."size" AS "size"
         FROM   "🔄"
         WHERE  "🔄"."🏷️" IS NOT DISTINCT FROM 'start.2'
       ),
       "merge.1"("#️⃣", "⚙️", "k", "node", "active", "size") AS (
         (SELECT "assignment.3"."#️⃣" AS "#️⃣",
                 "assignment.3"."⚙️" AS "⚙️",
                 "assignment.3"."k" AS "k",
                 "assignment.3"."node" AS "node",
                 "assignment.3"."active" AS "active",
                 "assignment.3"."size" AS "size"
          FROM   "assignment.3")
           UNION ALL
         (SELECT "start.2"."#️⃣" AS "#️⃣",
                 "start.2"."⚙️" AS "⚙️",
                 "start.2"."k" AS "k",
                 "start.2"."node" AS "node",
                 "start.2"."active" AS "active",
                 "start.2"."size" AS "size"
          FROM   "start.2")
       ),
       "fork.2"("#️⃣", "⚙️", "k", "node", "active", "size") AS (
         SELECT "merge.1"."#️⃣" AS "#️⃣",
                "merge.1"."⚙️" AS "⚙️",
                "merge.1"."k" AS "k",
                CAST(("ℚ"."node") AS INTEGER) AS "node",
                "merge.1"."active" AS "active",
                "merge.1"."size" AS "size"
         FROM   "merge.1",
                LATERAL (SELECT there
                         FROM   edges
                         WHERE  here = ("merge.1"."node")) AS "ℚ"("node")
       ),
       "gather.1"("#️⃣", "⚙️", "k", "node", "size", "degree") AS (
         SELECT "fork.2"."#️⃣" AS "#️⃣",
                "fork.2"."⚙️" AS "⚙️",
                "fork.2"."k" AS "k",
                "fork.2"."node" AS "node",
                "fork.2"."size" AS "size",
                CAST((countif(("fork.2"."active"))) AS HUGEINT) AS "degree"
         FROM   "fork.2"
         GROUP  BY "fork.2"."#️⃣",
                   "fork.2"."k",
                   "fork.2"."node",
                   "fork.2"."size",
                   "fork.2"."⚙️"
         HAVING COUNT(*) > 0
       ),
       "assignment.4"("#️⃣", "⚙️", "k", "node", "active", "size") AS (
         SELECT "gather.1"."#️⃣" AS "#️⃣",
                "gather.1"."⚙️" AS "⚙️",
                "gather.1"."k" AS "k",
                "gather.1"."node" AS "node",
                CAST((("gather.1"."degree") >= ("gather.1"."k") + 1) AS BOOLEAN) AS "active",
                "gather.1"."size" AS "size"
         FROM   "gather.1"
       ),
       "assignment.5"("#️⃣", "⚙️", "k", "node", "active", "size", "old_size") AS (
         SELECT "assignment.4"."#️⃣" AS "#️⃣",
                "assignment.4"."⚙️" AS "⚙️",
                "assignment.4"."k" AS "k",
                "assignment.4"."node" AS "node",
                "assignment.4"."active" AS "active",
                CAST((countif(("assignment.4"."active")) OVER ()) AS BIGINT) AS "size",
                CAST((("assignment.4"."size")) AS BIGINT) AS "old_size"
         FROM   "assignment.4"
       ),
       "assignment.6"("#️⃣", "⚙️", "🔍", "k", "node", "active", "size") AS (
         SELECT "assignment.5"."#️⃣" AS "#️⃣",
                "assignment.5"."⚙️" AS "⚙️",
                CAST((("assignment.5"."old_size") = ("assignment.5"."size")) AS BOOLEAN) AS "🔍",
                "assignment.5"."k" AS "k",
                "assignment.5"."node" AS "node",
                "assignment.5"."active" AS "active",
                "assignment.5"."size" AS "size"
         FROM   "assignment.5"
       ),
       "where.1"("#️⃣", "⚙️", "node", "active") AS (
         SELECT "assignment.6"."#️⃣" AS "#️⃣",
                "assignment.6"."⚙️" AS "⚙️",
                "assignment.6"."node" AS "node",
                "assignment.6"."active" AS "active"
         FROM   "assignment.6"
         WHERE  "assignment.6"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "assignment.7"("#️⃣", "⚙️", "🔍", "node") AS (
         SELECT "where.1"."#️⃣" AS "#️⃣",
                "where.1"."⚙️" AS "⚙️",
                CAST((("where.1"."active")) AS BOOLEAN) AS "🔍",
                "where.1"."node" AS "node"
         FROM   "where.1"
       ),
       "where.2"("#️⃣", "⚙️", "k", "node", "active", "size") AS (
         SELECT "assignment.6"."#️⃣" AS "#️⃣",
                "assignment.6"."⚙️" AS "⚙️",
                "assignment.6"."k" AS "k",
                "assignment.6"."node" AS "node",
                "assignment.6"."active" AS "active",
                "assignment.6"."size" AS "size"
         FROM   "assignment.6"
         WHERE  "assignment.6"."🔍" IS DISTINCT FROM TRUE
       ),
       "jump.1"("#️⃣", "🏷️", "k", "node", "active", "size") AS (
         SELECT "where.2"."#️⃣" AS "#️⃣",
                'start.2' AS "🏷️",
                "where.2"."k" AS "k",
                "where.2"."node" AS "node",
                "where.2"."active" AS "active",
                "where.2"."size" AS "size"
         FROM   "where.2"
       ),
       "where.3"("#️⃣", "⚙️", "node") AS (
         SELECT "assignment.7"."#️⃣" AS "#️⃣",
                "assignment.7"."⚙️" AS "⚙️",
                "assignment.7"."node" AS "node"
         FROM   "assignment.7"
         WHERE  "assignment.7"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "emit.1"("#️⃣", "⚙️", "📊.1") AS (
         SELECT "where.3"."#️⃣" AS "#️⃣",
                "where.3"."⚙️" AS "⚙️",
                "where.3"."node" AS "📊.1"
         FROM   "where.3"
       ),
       "stop.1"("⚙️") AS (
         SELECT "emit.1"."⚙️"
         FROM   "emit.1"
         WHERE  FALSE
       ),
       "where.4"("⚙️") AS (
         SELECT "assignment.7"."⚙️" AS "⚙️"
         FROM   "assignment.7"
         WHERE  "assignment.7"."🔍" IS DISTINCT FROM TRUE
       ),
       "stop.2"("⚙️") AS (
         SELECT "where.4"."⚙️"
         FROM   "where.4"
         WHERE  FALSE
       )
     (SELECT CAST(("jump.1"."#️⃣" + 1) AS INTEGER) AS "#️⃣",
             CAST(("jump.1"."🏷️") AS VARCHAR) AS "🏷️",
             CAST((NULL) AS INTEGER) AS "📊.1",
             CAST(("jump.1"."k") AS INTEGER) AS "k",
             CAST(("jump.1"."node") AS INTEGER) AS "node",
             CAST(("jump.1"."active") AS BOOLEAN) AS "active",
             CAST(("jump.1"."size") AS BIGINT) AS "size"
      FROM   "jump.1")
       UNION ALL
     (SELECT CAST(("emit.1"."#️⃣") AS INTEGER) AS "#️⃣",
             CAST((NULL) AS VARCHAR) AS "🏷️",
             CAST(("emit.1"."📊.1") AS INTEGER) AS "📊.1",
             CAST((NULL) AS INTEGER) AS "k",
             CAST((NULL) AS INTEGER) AS "node",
             CAST((NULL) AS BOOLEAN) AS "active",
             CAST((NULL) AS BIGINT) AS "size"
      FROM   "emit.1"))
  )
SELECT count(*) AS active_nodes,
       min("🔄"."📊.1") AS minimum,
       max("🔄"."📊.1") AS maximum,
       sum("🔄"."📊.1")::BIGINT AS total
FROM   "🔄"
WHERE  "🔄"."🏷️" IS NULL;
