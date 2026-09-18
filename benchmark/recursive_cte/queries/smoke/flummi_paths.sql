WITH RECURSIVE
  "🔄"("#️⃣", "🏷️", "📊.1", "first", "last", "length", "path") AS (
    (SELECT CAST((0) AS INTEGER) AS "#️⃣",
            CAST(('start.1') AS VARCHAR) AS "🏷️",
            CAST((NULL) AS INTEGER[]) AS "📊.1",
            CAST((NULL) AS INTEGER) AS "first",
            CAST((NULL) AS INTEGER) AS "last",
            CAST((NULL) AS INTEGER) AS "length",
            CAST((NULL) AS INTEGER[]) AS "path")
      UNION ALL
    (WITH
       "start.1"("#️⃣", "⚙️") AS (
         SELECT "🔄"."#️⃣" AS "#️⃣",
                NULL AS "⚙️"
         FROM   "🔄"
         WHERE  "🔄"."🏷️" IS NOT DISTINCT FROM 'start.1'
       ),
       "fork.1"("#️⃣", "⚙️", "first") AS (
         SELECT "start.1"."#️⃣" AS "#️⃣",
                "start.1"."⚙️" AS "⚙️",
                CAST(("ℚ"."first") AS INTEGER) AS "first"
         FROM   "start.1",
                (SELECT id FROM nodes) AS "ℚ"("first")
       ),
       "assignment.1"("#️⃣", "⚙️", "first", "last") AS (
         SELECT "fork.1"."#️⃣" AS "#️⃣",
                "fork.1"."⚙️" AS "⚙️",
                "fork.1"."first" AS "first",
                CAST((("fork.1"."first")) AS INTEGER) AS "last"
         FROM   "fork.1"
       ),
       "assignment.2"("#️⃣", "⚙️", "first", "last", "length") AS (
         SELECT "assignment.1"."#️⃣" AS "#️⃣",
                "assignment.1"."⚙️" AS "⚙️",
                "assignment.1"."first" AS "first",
                "assignment.1"."last" AS "last",
                CAST((0) AS INTEGER) AS "length"
         FROM   "assignment.1"
       ),
       "assignment.3"("#️⃣", "⚙️", "first", "last", "length", "path") AS (
         SELECT "assignment.2"."#️⃣" AS "#️⃣",
                "assignment.2"."⚙️" AS "⚙️",
                "assignment.2"."first" AS "first",
                "assignment.2"."last" AS "last",
                "assignment.2"."length" AS "length",
                CAST((array[("assignment.2"."last")]) AS INTEGER[]) AS "path"
         FROM   "assignment.2"
       ),
       "start.2"("#️⃣", "⚙️", "first", "last", "length", "path") AS (
         SELECT "🔄"."#️⃣" AS "#️⃣",
                NULL AS "⚙️",
                "🔄"."first" AS "first",
                "🔄"."last" AS "last",
                "🔄"."length" AS "length",
                "🔄"."path" AS "path"
         FROM   "🔄"
         WHERE  "🔄"."🏷️" IS NOT DISTINCT FROM 'start.2'
       ),
       "merge.1"("#️⃣", "⚙️", "first", "last", "length", "path") AS (
         (SELECT "assignment.3"."#️⃣" AS "#️⃣",
                 "assignment.3"."⚙️" AS "⚙️",
                 "assignment.3"."first" AS "first",
                 "assignment.3"."last" AS "last",
                 "assignment.3"."length" AS "length",
                 "assignment.3"."path" AS "path"
          FROM   "assignment.3")
           UNION ALL
         (SELECT "start.2"."#️⃣" AS "#️⃣",
                 "start.2"."⚙️" AS "⚙️",
                 "start.2"."first" AS "first",
                 "start.2"."last" AS "last",
                 "start.2"."length" AS "length",
                 "start.2"."path" AS "path"
          FROM   "start.2")
       ),
       "fork.2"("#️⃣", "⚙️", "first", "length", "path", "edge") AS (
         SELECT "merge.1"."#️⃣" AS "#️⃣",
                "merge.1"."⚙️" AS "⚙️",
                "merge.1"."first" AS "first",
                "merge.1"."length" AS "length",
                "merge.1"."path" AS "path",
                CAST(("ℚ"."edge") AS STRUCT(here INTEGER, there INTEGER, length INTEGER)) AS "edge"
         FROM   "merge.1",
                LATERAL (SELECT e FROM edges AS e WHERE here = ("merge.1"."last")) AS "ℚ"("edge")
       ),
       "assignment.4"("#️⃣", "⚙️", "first", "length", "path", "edge") AS (
         SELECT "fork.2"."#️⃣" AS "#️⃣",
                "fork.2"."⚙️" AS "⚙️",
                "fork.2"."first" AS "first",
                CAST((("fork.2"."length") + ("fork.2"."edge").length) AS INTEGER) AS "length",
                "fork.2"."path" AS "path",
                "fork.2"."edge" AS "edge"
         FROM   "fork.2"
       ),
       "assignment.5"("#️⃣", "⚙️", "first", "last", "length", "path") AS (
         SELECT "assignment.4"."#️⃣" AS "#️⃣",
                "assignment.4"."⚙️" AS "⚙️",
                "assignment.4"."first" AS "first",
                CAST((("assignment.4"."edge").there) AS INTEGER) AS "last",
                "assignment.4"."length" AS "length",
                "assignment.4"."path" AS "path"
         FROM   "assignment.4"
       ),
       "assignment.6"("#️⃣", "⚙️", "first", "last", "length", "path") AS (
         SELECT "assignment.5"."#️⃣" AS "#️⃣",
                "assignment.5"."⚙️" AS "⚙️",
                "assignment.5"."first" AS "first",
                "assignment.5"."last" AS "last",
                "assignment.5"."length" AS "length",
                CAST((("assignment.5"."path") || array[("assignment.5"."last")]) AS INTEGER[]) AS "path"
         FROM   "assignment.5"
       ),
       "fork.3"("#️⃣", "⚙️", "first", "last", "length", "path", "flip") AS (
         SELECT "assignment.6"."#️⃣" AS "#️⃣",
                "assignment.6"."⚙️" AS "⚙️",
                "assignment.6"."first" AS "first",
                "assignment.6"."last" AS "last",
                "assignment.6"."length" AS "length",
                "assignment.6"."path" AS "path",
                CAST(("ℚ"."flip") AS BOOLEAN) AS "flip"
         FROM   "assignment.6",
                (VALUES (TRUE), (FALSE)) AS "ℚ"("flip")
       ),
       "assignment.7"("#️⃣", "⚙️", "🔍", "first", "last", "length", "path") AS (
         SELECT "fork.3"."#️⃣" AS "#️⃣",
                "fork.3"."⚙️" AS "⚙️",
                CAST((("fork.3"."flip")) AS BOOLEAN) AS "🔍",
                "fork.3"."first" AS "first",
                "fork.3"."last" AS "last",
                "fork.3"."length" AS "length",
                "fork.3"."path" AS "path"
         FROM   "fork.3"
       ),
       "start.3"("#️⃣", "⚙️", "first", "last", "length", "path") AS (
         SELECT "🔄"."#️⃣" AS "#️⃣",
                NULL AS "⚙️",
                "🔄"."first" AS "first",
                "🔄"."last" AS "last",
                "🔄"."length" AS "length",
                "🔄"."path" AS "path"
         FROM   "🔄"
         WHERE  "🔄"."🏷️" IS NOT DISTINCT FROM 'start.3'
       ),
       "issynced.1"("#️⃣", "⚙️", "🔍", "first", "last", "length", "path") AS (
         SELECT "start.3"."#️⃣" AS "#️⃣",
                "start.3"."⚙️" AS "⚙️",
                NOT EXISTS ((SELECT NULL
                             FROM   "🔄"
                             WHERE  "🔄"."🏷️" <> 'start.3')) AS "🔍",
                "start.3"."first" AS "first",
                "start.3"."last" AS "last",
                "start.3"."length" AS "length",
                "start.3"."path" AS "path"
         FROM   "start.3"
       ),
       "where.1"("#️⃣", "⚙️", "first", "last", "length", "path") AS (
         SELECT "assignment.7"."#️⃣" AS "#️⃣",
                "assignment.7"."⚙️" AS "⚙️",
                "assignment.7"."first" AS "first",
                "assignment.7"."last" AS "last",
                "assignment.7"."length" AS "length",
                "assignment.7"."path" AS "path"
         FROM   "assignment.7"
         WHERE  "assignment.7"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "jump.2"("#️⃣", "🏷️", "first", "last", "length", "path") AS (
         SELECT "where.1"."#️⃣" AS "#️⃣",
                'start.3' AS "🏷️",
                "where.1"."first" AS "first",
                "where.1"."last" AS "last",
                "where.1"."length" AS "length",
                "where.1"."path" AS "path"
         FROM   "where.1"
       ),
       "where.2"("#️⃣", "⚙️", "first", "last", "length", "path") AS (
         SELECT "assignment.7"."#️⃣" AS "#️⃣",
                "assignment.7"."⚙️" AS "⚙️",
                "assignment.7"."first" AS "first",
                "assignment.7"."last" AS "last",
                "assignment.7"."length" AS "length",
                "assignment.7"."path" AS "path"
         FROM   "assignment.7"
         WHERE  "assignment.7"."🔍" IS DISTINCT FROM TRUE
       ),
       "jump.1"("#️⃣", "🏷️", "first", "last", "length", "path") AS (
         SELECT "where.2"."#️⃣" AS "#️⃣",
                'start.2' AS "🏷️",
                "where.2"."first" AS "first",
                "where.2"."last" AS "last",
                "where.2"."length" AS "length",
                "where.2"."path" AS "path"
         FROM   "where.2"
       ),
       "where.3"("#️⃣", "⚙️", "first", "last", "length", "path") AS (
         SELECT "issynced.1"."#️⃣" AS "#️⃣",
                "issynced.1"."⚙️" AS "⚙️",
                "issynced.1"."first" AS "first",
                "issynced.1"."last" AS "last",
                "issynced.1"."length" AS "length",
                "issynced.1"."path" AS "path"
         FROM   "issynced.1"
         WHERE  "issynced.1"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "gather.1"("#️⃣", "⚙️", "path") AS (
         SELECT "where.3"."#️⃣" AS "#️⃣",
                "where.3"."⚙️" AS "⚙️",
                CAST((ARGMIN(("where.3"."path"), ("where.3"."length"))) AS INTEGER[]) AS "path"
         FROM   "where.3"
         GROUP  BY "where.3"."#️⃣",
                   "where.3"."first",
                   "where.3"."last",
                   "where.3"."⚙️"
         HAVING COUNT(*) > 0
       ),
       "emit.1"("#️⃣", "⚙️", "📊.1") AS (
         SELECT "gather.1"."#️⃣" AS "#️⃣",
                "gather.1"."⚙️" AS "⚙️",
                "gather.1"."path" AS "📊.1"
         FROM   "gather.1"
       ),
       "stop.1"("⚙️") AS (
         SELECT "emit.1"."⚙️"
         FROM   "emit.1"
         WHERE  FALSE
       ),
       "where.4"("#️⃣", "⚙️", "first", "last", "length", "path") AS (
         SELECT "issynced.1"."#️⃣" AS "#️⃣",
                "issynced.1"."⚙️" AS "⚙️",
                "issynced.1"."first" AS "first",
                "issynced.1"."last" AS "last",
                "issynced.1"."length" AS "length",
                "issynced.1"."path" AS "path"
         FROM   "issynced.1"
         WHERE  "issynced.1"."🔍" IS DISTINCT FROM TRUE
       ),
       "jump.3"("#️⃣", "🏷️", "first", "last", "length", "path") AS (
         SELECT "where.4"."#️⃣" AS "#️⃣",
                'start.3' AS "🏷️",
                "where.4"."first" AS "first",
                "where.4"."last" AS "last",
                "where.4"."length" AS "length",
                "where.4"."path" AS "path"
         FROM   "where.4"
       )
     (SELECT CAST(("jump.2"."#️⃣" + 1) AS INTEGER) AS "#️⃣",
             CAST(("jump.2"."🏷️") AS VARCHAR) AS "🏷️",
             CAST((NULL) AS INTEGER[]) AS "📊.1",
             CAST(("jump.2"."first") AS INTEGER) AS "first",
             CAST(("jump.2"."last") AS INTEGER) AS "last",
             CAST(("jump.2"."length") AS INTEGER) AS "length",
             CAST(("jump.2"."path") AS INTEGER[]) AS "path"
      FROM   "jump.2")
       UNION ALL
     (SELECT CAST(("jump.1"."#️⃣" + 1) AS INTEGER) AS "#️⃣",
             CAST(("jump.1"."🏷️") AS VARCHAR) AS "🏷️",
             CAST((NULL) AS INTEGER[]) AS "📊.1",
             CAST(("jump.1"."first") AS INTEGER) AS "first",
             CAST(("jump.1"."last") AS INTEGER) AS "last",
             CAST(("jump.1"."length") AS INTEGER) AS "length",
             CAST(("jump.1"."path") AS INTEGER[]) AS "path"
      FROM   "jump.1")
       UNION ALL
     (SELECT CAST(("emit.1"."#️⃣") AS INTEGER) AS "#️⃣",
             CAST((NULL) AS VARCHAR) AS "🏷️",
             CAST(("emit.1"."📊.1") AS INTEGER[]) AS "📊.1",
             CAST((NULL) AS INTEGER) AS "first",
             CAST((NULL) AS INTEGER) AS "last",
             CAST((NULL) AS INTEGER) AS "length",
             CAST((NULL) AS INTEGER[]) AS "path"
      FROM   "emit.1")
       UNION ALL
     (SELECT CAST(("jump.3"."#️⃣" + 1) AS INTEGER) AS "#️⃣",
             CAST(("jump.3"."🏷️") AS VARCHAR) AS "🏷️",
             CAST((NULL) AS INTEGER[]) AS "📊.1",
             CAST(("jump.3"."first") AS INTEGER) AS "first",
             CAST(("jump.3"."last") AS INTEGER) AS "last",
             CAST(("jump.3"."length") AS INTEGER) AS "length",
             CAST(("jump.3"."path") AS INTEGER[]) AS "path"
      FROM   "jump.3"))
  )
SELECT count(*) AS paths,
       sum(len("🔄"."📊.1"))::BIGINT AS path_nodes,
       md5(string_agg("🔄"."📊.1"::VARCHAR, '|' ORDER BY "🔄"."📊.1"::VARCHAR)) AS path_md5
FROM   "🔄"
WHERE  "🔄"."🏷️" IS NULL;
