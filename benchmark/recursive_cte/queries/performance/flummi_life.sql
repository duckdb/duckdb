WITH RECURSIVE
  "🔄"("#️⃣", "🏷️", "📊.1", "height", "width", "iterations", "x", "y", "alive") AS (
    (SELECT CAST((0) AS INTEGER) AS "#️⃣",
            CAST(('start.1') AS VARCHAR) AS "🏷️",
            CAST((NULL) AS VARCHAR) AS "📊.1",
            CAST((NULL) AS INTEGER) AS "height",
            CAST((NULL) AS INTEGER) AS "width",
            CAST((NULL) AS INTEGER) AS "iterations",
            CAST((NULL) AS BIGINT) AS "x",
            CAST((NULL) AS BIGINT) AS "y",
            CAST((NULL) AS BOOLEAN) AS "alive")
      UNION ALL
    (WITH
       "start.1"("#️⃣", "⚙️") AS (
         SELECT "🔄"."#️⃣" AS "#️⃣",
                NULL AS "⚙️"
         FROM   "🔄"
         WHERE  "🔄"."🏷️" IS NOT DISTINCT FROM 'start.1'
       ),
       "assignment.1"("#️⃣", "⚙️", "height", "width", "iterations") AS (
         SELECT "start.1"."#️⃣" AS "#️⃣",
                "start.1"."⚙️" AS "⚙️",
                CAST((10) AS INTEGER) AS "height",
                CAST((10) AS INTEGER) AS "width",
                CAST((100) AS INTEGER) AS "iterations"
         FROM   "start.1"
       ),
       "fork.1"("#️⃣", "⚙️", "height", "width", "iterations", "x") AS (
         SELECT "assignment.1"."#️⃣" AS "#️⃣",
                "assignment.1"."⚙️" AS "⚙️",
                "assignment.1"."height" AS "height",
                "assignment.1"."width" AS "width",
                "assignment.1"."iterations" AS "iterations",
                CAST(("ℚ"."x") AS BIGINT) AS "x"
         FROM   "assignment.1",
                LATERAL (FROM range(("assignment.1"."width"))) AS "ℚ"("x")
       ),
       "fork.2"("#️⃣", "⚙️", "height", "width", "iterations", "x", "y") AS (
         SELECT "fork.1"."#️⃣" AS "#️⃣",
                "fork.1"."⚙️" AS "⚙️",
                "fork.1"."height" AS "height",
                "fork.1"."width" AS "width",
                "fork.1"."iterations" AS "iterations",
                "fork.1"."x" AS "x",
                CAST(("ℚ"."y") AS BIGINT) AS "y"
         FROM   "fork.1",
                LATERAL (FROM range(("fork.1"."height"))) AS "ℚ"("y")
       ),
       "assignment.2"("#️⃣", "⚙️", "height", "width", "iterations", "x", "y", "alive") AS (
         SELECT "fork.2"."#️⃣" AS "#️⃣",
                "fork.2"."⚙️" AS "⚙️",
                "fork.2"."height" AS "height",
                "fork.2"."width" AS "width",
                "fork.2"."iterations" AS "iterations",
                "fork.2"."x" AS "x",
                "fork.2"."y" AS "y",
                CAST(((("fork.2"."x"), ("fork.2"."y")) = ANY (VALUES
                                  ((1,0)),
                                           ((2,1)),
                         ((0,2)), ((1,2)), ((2,2))
                       )) AS BOOLEAN) AS "alive"
         FROM   "fork.2"
       ),
       "start.2"("#️⃣", "⚙️", "height", "width", "iterations", "x", "y", "alive") AS (
         SELECT "🔄"."#️⃣" AS "#️⃣",
                NULL AS "⚙️",
                "🔄"."height" AS "height",
                "🔄"."width" AS "width",
                "🔄"."iterations" AS "iterations",
                "🔄"."x" AS "x",
                "🔄"."y" AS "y",
                "🔄"."alive" AS "alive"
         FROM   "🔄"
         WHERE  "🔄"."🏷️" IS NOT DISTINCT FROM 'start.2'
       ),
       "merge.1"("#️⃣", "⚙️", "height", "width", "iterations", "x", "y", "alive") AS (
         (SELECT "assignment.2"."#️⃣" AS "#️⃣",
                 "assignment.2"."⚙️" AS "⚙️",
                 "assignment.2"."height" AS "height",
                 "assignment.2"."width" AS "width",
                 "assignment.2"."iterations" AS "iterations",
                 "assignment.2"."x" AS "x",
                 "assignment.2"."y" AS "y",
                 "assignment.2"."alive" AS "alive"
          FROM   "assignment.2")
           UNION ALL
         (SELECT "start.2"."#️⃣" AS "#️⃣",
                 "start.2"."⚙️" AS "⚙️",
                 "start.2"."height" AS "height",
                 "start.2"."width" AS "width",
                 "start.2"."iterations" AS "iterations",
                 "start.2"."x" AS "x",
                 "start.2"."y" AS "y",
                 "start.2"."alive" AS "alive"
          FROM   "start.2")
       ),
       "fork.3"("#️⃣", "⚙️", "height", "width", "iterations", "x", "y", "alive", "flip") AS (
         SELECT "merge.1"."#️⃣" AS "#️⃣",
                "merge.1"."⚙️" AS "⚙️",
                "merge.1"."height" AS "height",
                "merge.1"."width" AS "width",
                "merge.1"."iterations" AS "iterations",
                "merge.1"."x" AS "x",
                "merge.1"."y" AS "y",
                "merge.1"."alive" AS "alive",
                CAST(("ℚ"."flip") AS BOOLEAN) AS "flip"
         FROM   "merge.1",
                (VALUES (TRUE), (FALSE)) AS "ℚ"("flip")
       ),
       "assignment.3"("#️⃣", "⚙️", "🔍", "height", "width", "iterations", "x", "y", "alive") AS (
         SELECT "fork.3"."#️⃣" AS "#️⃣",
                "fork.3"."⚙️" AS "⚙️",
                CAST((("fork.3"."flip")) AS BOOLEAN) AS "🔍",
                "fork.3"."height" AS "height",
                "fork.3"."width" AS "width",
                "fork.3"."iterations" AS "iterations",
                "fork.3"."x" AS "x",
                "fork.3"."y" AS "y",
                "fork.3"."alive" AS "alive"
         FROM   "fork.3"
       ),
       "where.1"("#️⃣", "⚙️", "width", "x", "y", "alive") AS (
         SELECT "assignment.3"."#️⃣" AS "#️⃣",
                "assignment.3"."⚙️" AS "⚙️",
                "assignment.3"."width" AS "width",
                "assignment.3"."x" AS "x",
                "assignment.3"."y" AS "y",
                "assignment.3"."alive" AS "alive"
         FROM   "assignment.3"
         WHERE  "assignment.3"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "gather.2"("#️⃣", "⚙️", "width", "y", "buffer") AS (
         SELECT "where.1"."#️⃣" AS "#️⃣",
                "where.1"."⚙️" AS "⚙️",
                "where.1"."width" AS "width",
                "where.1"."y" AS "y",
                CAST((string_agg(if(("where.1"."alive"), '#', '.'), '' ORDER BY ("where.1"."x"))) AS VARCHAR) AS "buffer"
         FROM   "where.1"
         GROUP  BY "where.1"."#️⃣",
                   "where.1"."width",
                   "where.1"."y",
                   "where.1"."⚙️"
         HAVING COUNT(*) > 0
       ),
       "gather.3"("#️⃣", "⚙️", "width", "buffer") AS (
         SELECT "gather.2"."#️⃣" AS "#️⃣",
                "gather.2"."⚙️" AS "⚙️",
                "gather.2"."width" AS "width",
                CAST((string_agg(("gather.2"."buffer"), E'\n' ORDER BY ("gather.2"."y"))) AS VARCHAR) AS "buffer"
         FROM   "gather.2"
         GROUP  BY "gather.2"."#️⃣",
                   "gather.2"."width",
                   "gather.2"."⚙️"
         HAVING COUNT(*) > 0
       ),
       "emit.1"("#️⃣", "⚙️", "📊.1", "width") AS (
         SELECT "gather.3"."#️⃣" AS "#️⃣",
                "gather.3"."⚙️" AS "⚙️",
                "gather.3"."buffer" AS "📊.1",
                "gather.3"."width" AS "width"
         FROM   "gather.3"
       ),
       "assignment.11"("#️⃣", "⚙️", "buffer") AS (
         SELECT "emit.1"."#️⃣" AS "#️⃣",
                "emit.1"."⚙️" AS "⚙️",
                CAST((repeat('-', ("emit.1"."width"))) AS VARCHAR) AS "buffer"
         FROM   "emit.1"
       ),
       "emit.2"("#️⃣", "⚙️", "📊.1") AS (
         SELECT "assignment.11"."#️⃣" AS "#️⃣",
                "assignment.11"."⚙️" AS "⚙️",
                "assignment.11"."buffer" AS "📊.1"
         FROM   "assignment.11"
       ),
       "stop.2"("⚙️") AS (
         SELECT "emit.2"."⚙️"
         FROM   "emit.2"
         WHERE  FALSE
       ),
       "where.2"("#️⃣", "⚙️", "height", "width", "iterations", "x", "y", "alive") AS (
         SELECT "assignment.3"."#️⃣" AS "#️⃣",
                "assignment.3"."⚙️" AS "⚙️",
                "assignment.3"."height" AS "height",
                "assignment.3"."width" AS "width",
                "assignment.3"."iterations" AS "iterations",
                "assignment.3"."x" AS "x",
                "assignment.3"."y" AS "y",
                "assignment.3"."alive" AS "alive"
         FROM   "assignment.3"
         WHERE  "assignment.3"."🔍" IS DISTINCT FROM TRUE
       ),
       "assignment.4"("#️⃣", "⚙️", "🔍", "height", "width", "iterations", "x", "y", "alive") AS (
         SELECT "where.2"."#️⃣" AS "#️⃣",
                "where.2"."⚙️" AS "⚙️",
                CAST((("where.2"."iterations") = 0) AS BOOLEAN) AS "🔍",
                "where.2"."height" AS "height",
                "where.2"."width" AS "width",
                "where.2"."iterations" AS "iterations",
                "where.2"."x" AS "x",
                "where.2"."y" AS "y",
                "where.2"."alive" AS "alive"
         FROM   "where.2"
       ),
       "where.3"("⚙️") AS (
         SELECT "assignment.4"."⚙️" AS "⚙️"
         FROM   "assignment.4"
         WHERE  "assignment.4"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "stop.1"("⚙️") AS (
         SELECT "where.3"."⚙️"
         FROM   "where.3"
         WHERE  FALSE
       ),
       "where.4"("#️⃣", "⚙️", "height", "width", "iterations", "x", "y", "alive") AS (
         SELECT "assignment.4"."#️⃣" AS "#️⃣",
                "assignment.4"."⚙️" AS "⚙️",
                "assignment.4"."height" AS "height",
                "assignment.4"."width" AS "width",
                "assignment.4"."iterations" AS "iterations",
                "assignment.4"."x" AS "x",
                "assignment.4"."y" AS "y",
                "assignment.4"."alive" AS "alive"
         FROM   "assignment.4"
         WHERE  "assignment.4"."🔍" IS DISTINCT FROM TRUE
       ),
       "assignment.5"("#️⃣", "⚙️", "height", "width", "iterations", "x", "y", "alive") AS (
         SELECT "where.4"."#️⃣" AS "#️⃣",
                "where.4"."⚙️" AS "⚙️",
                "where.4"."height" AS "height",
                "where.4"."width" AS "width",
                CAST((("where.4"."iterations") - 1) AS INTEGER) AS "iterations",
                "where.4"."x" AS "x",
                "where.4"."y" AS "y",
                "where.4"."alive" AS "alive"
         FROM   "where.4"
       ),
       "fork.4"("#️⃣", "⚙️", "height", "width", "iterations", "x", "y", "alive", "dx") AS (
         SELECT "assignment.5"."#️⃣" AS "#️⃣",
                "assignment.5"."⚙️" AS "⚙️",
                "assignment.5"."height" AS "height",
                "assignment.5"."width" AS "width",
                "assignment.5"."iterations" AS "iterations",
                "assignment.5"."x" AS "x",
                "assignment.5"."y" AS "y",
                "assignment.5"."alive" AS "alive",
                CAST(("ℚ"."dx") AS BIGINT) AS "dx"
         FROM   "assignment.5",
                (FROM generate_series(-1, 1)) AS "ℚ"("dx")
       ),
       "fork.5"("#️⃣", "⚙️", "height", "width", "iterations", "x", "y", "alive", "dx", "dy") AS (
         SELECT "fork.4"."#️⃣" AS "#️⃣",
                "fork.4"."⚙️" AS "⚙️",
                "fork.4"."height" AS "height",
                "fork.4"."width" AS "width",
                "fork.4"."iterations" AS "iterations",
                "fork.4"."x" AS "x",
                "fork.4"."y" AS "y",
                "fork.4"."alive" AS "alive",
                "fork.4"."dx" AS "dx",
                CAST(("ℚ"."dy") AS BIGINT) AS "dy"
         FROM   "fork.4",
                (FROM generate_series(-1, 1)) AS "ℚ"("dy")
       ),
       "assignment.6"("#️⃣", "⚙️", "height", "width", "iterations", "x", "y", "alive", "dx", "dy") AS (
         SELECT "fork.5"."#️⃣" AS "#️⃣",
                "fork.5"."⚙️" AS "⚙️",
                "fork.5"."height" AS "height",
                "fork.5"."width" AS "width",
                "fork.5"."iterations" AS "iterations",
                CAST(((("fork.5"."width")  + ("fork.5"."x") + ("fork.5"."dx")) % ("fork.5"."width")) AS BIGINT) AS "x",
                CAST(((("fork.5"."height") + ("fork.5"."y") + ("fork.5"."dy")) % ("fork.5"."height")) AS BIGINT) AS "y",
                "fork.5"."alive" AS "alive",
                "fork.5"."dx" AS "dx",
                "fork.5"."dy" AS "dy"
         FROM   "fork.5"
       ),
       "assignment.7"("#️⃣", "⚙️", "height", "width", "iterations", "x", "y", "alive", "original") AS (
         SELECT "assignment.6"."#️⃣" AS "#️⃣",
                "assignment.6"."⚙️" AS "⚙️",
                "assignment.6"."height" AS "height",
                "assignment.6"."width" AS "width",
                "assignment.6"."iterations" AS "iterations",
                "assignment.6"."x" AS "x",
                "assignment.6"."y" AS "y",
                "assignment.6"."alive" AS "alive",
                CAST((("assignment.6"."dx") = 0 AND ("assignment.6"."dy") = 0) AS BOOLEAN) AS "original"
         FROM   "assignment.6"
       ),
       "gather.1"("#️⃣", "⚙️", "height", "width", "iterations", "x", "y", "alive", "neighbors") AS (
         SELECT "assignment.7"."#️⃣" AS "#️⃣",
                "assignment.7"."⚙️" AS "⚙️",
                "assignment.7"."height" AS "height",
                "assignment.7"."width" AS "width",
                "assignment.7"."iterations" AS "iterations",
                "assignment.7"."x" AS "x",
                "assignment.7"."y" AS "y",
                CAST((any_value(("assignment.7"."alive")) FILTER (WHERE ("assignment.7"."original"))) AS BOOLEAN) AS "alive",
                CAST((count(*) FILTER (WHERE ("assignment.7"."alive") AND NOT ("assignment.7"."original"))) AS BIGINT) AS "neighbors"
         FROM   "assignment.7"
         GROUP  BY "assignment.7"."#️⃣",
                   "assignment.7"."height",
                   "assignment.7"."iterations",
                   "assignment.7"."width",
                   "assignment.7"."x",
                   "assignment.7"."y",
                   "assignment.7"."⚙️"
         HAVING COUNT(*) > 0
       ),
       "assignment.8"("#️⃣", "⚙️", "🔍", "height", "width", "iterations", "x", "y", "neighbors") AS (
         SELECT "gather.1"."#️⃣" AS "#️⃣",
                "gather.1"."⚙️" AS "⚙️",
                CAST((("gather.1"."alive")) AS BOOLEAN) AS "🔍",
                "gather.1"."height" AS "height",
                "gather.1"."width" AS "width",
                "gather.1"."iterations" AS "iterations",
                "gather.1"."x" AS "x",
                "gather.1"."y" AS "y",
                "gather.1"."neighbors" AS "neighbors"
         FROM   "gather.1"
       ),
       "where.5"("#️⃣", "⚙️", "height", "width", "iterations", "x", "y", "neighbors") AS (
         SELECT "assignment.8"."#️⃣" AS "#️⃣",
                "assignment.8"."⚙️" AS "⚙️",
                "assignment.8"."height" AS "height",
                "assignment.8"."width" AS "width",
                "assignment.8"."iterations" AS "iterations",
                "assignment.8"."x" AS "x",
                "assignment.8"."y" AS "y",
                "assignment.8"."neighbors" AS "neighbors"
         FROM   "assignment.8"
         WHERE  "assignment.8"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "assignment.9"("#️⃣", "⚙️", "height", "width", "iterations", "x", "y", "alive") AS (
         SELECT "where.5"."#️⃣" AS "#️⃣",
                "where.5"."⚙️" AS "⚙️",
                "where.5"."height" AS "height",
                "where.5"."width" AS "width",
                "where.5"."iterations" AS "iterations",
                "where.5"."x" AS "x",
                "where.5"."y" AS "y",
                CAST((("where.5"."neighbors") BETWEEN 2 AND 3) AS BOOLEAN) AS "alive"
         FROM   "where.5"
       ),
       "jump.2"("#️⃣", "🏷️", "height", "width", "iterations", "x", "y", "alive") AS (
         SELECT "assignment.9"."#️⃣" AS "#️⃣",
                'start.2' AS "🏷️",
                "assignment.9"."height" AS "height",
                "assignment.9"."width" AS "width",
                "assignment.9"."iterations" AS "iterations",
                "assignment.9"."x" AS "x",
                "assignment.9"."y" AS "y",
                "assignment.9"."alive" AS "alive"
         FROM   "assignment.9"
       ),
       "where.6"("#️⃣", "⚙️", "height", "width", "iterations", "x", "y", "neighbors") AS (
         SELECT "assignment.8"."#️⃣" AS "#️⃣",
                "assignment.8"."⚙️" AS "⚙️",
                "assignment.8"."height" AS "height",
                "assignment.8"."width" AS "width",
                "assignment.8"."iterations" AS "iterations",
                "assignment.8"."x" AS "x",
                "assignment.8"."y" AS "y",
                "assignment.8"."neighbors" AS "neighbors"
         FROM   "assignment.8"
         WHERE  "assignment.8"."🔍" IS DISTINCT FROM TRUE
       ),
       "assignment.10"("#️⃣", "⚙️", "height", "width", "iterations", "x", "y", "alive") AS (
         SELECT "where.6"."#️⃣" AS "#️⃣",
                "where.6"."⚙️" AS "⚙️",
                "where.6"."height" AS "height",
                "where.6"."width" AS "width",
                "where.6"."iterations" AS "iterations",
                "where.6"."x" AS "x",
                "where.6"."y" AS "y",
                CAST((("where.6"."neighbors") = 3) AS BOOLEAN) AS "alive"
         FROM   "where.6"
       ),
       "jump.1"("#️⃣", "🏷️", "height", "width", "iterations", "x", "y", "alive") AS (
         SELECT "assignment.10"."#️⃣" AS "#️⃣",
                'start.2' AS "🏷️",
                "assignment.10"."height" AS "height",
                "assignment.10"."width" AS "width",
                "assignment.10"."iterations" AS "iterations",
                "assignment.10"."x" AS "x",
                "assignment.10"."y" AS "y",
                "assignment.10"."alive" AS "alive"
         FROM   "assignment.10"
       )
     (SELECT CAST(("emit.1"."#️⃣") AS INTEGER) AS "#️⃣",
             CAST((NULL) AS VARCHAR) AS "🏷️",
             CAST(("emit.1"."📊.1") AS VARCHAR) AS "📊.1",
             CAST((NULL) AS INTEGER) AS "height",
             CAST((NULL) AS INTEGER) AS "width",
             CAST((NULL) AS INTEGER) AS "iterations",
             CAST((NULL) AS BIGINT) AS "x",
             CAST((NULL) AS BIGINT) AS "y",
             CAST((NULL) AS BOOLEAN) AS "alive"
      FROM   "emit.1")
       UNION ALL
     (SELECT CAST(("emit.2"."#️⃣") AS INTEGER) AS "#️⃣",
             CAST((NULL) AS VARCHAR) AS "🏷️",
             CAST(("emit.2"."📊.1") AS VARCHAR) AS "📊.1",
             CAST((NULL) AS INTEGER) AS "height",
             CAST((NULL) AS INTEGER) AS "width",
             CAST((NULL) AS INTEGER) AS "iterations",
             CAST((NULL) AS BIGINT) AS "x",
             CAST((NULL) AS BIGINT) AS "y",
             CAST((NULL) AS BOOLEAN) AS "alive"
      FROM   "emit.2")
       UNION ALL
     (SELECT CAST(("jump.2"."#️⃣" + 1) AS INTEGER) AS "#️⃣",
             CAST(("jump.2"."🏷️") AS VARCHAR) AS "🏷️",
             CAST((NULL) AS VARCHAR) AS "📊.1",
             CAST(("jump.2"."height") AS INTEGER) AS "height",
             CAST(("jump.2"."width") AS INTEGER) AS "width",
             CAST(("jump.2"."iterations") AS INTEGER) AS "iterations",
             CAST(("jump.2"."x") AS BIGINT) AS "x",
             CAST(("jump.2"."y") AS BIGINT) AS "y",
             CAST(("jump.2"."alive") AS BOOLEAN) AS "alive"
      FROM   "jump.2")
       UNION ALL
     (SELECT CAST(("jump.1"."#️⃣" + 1) AS INTEGER) AS "#️⃣",
             CAST(("jump.1"."🏷️") AS VARCHAR) AS "🏷️",
             CAST((NULL) AS VARCHAR) AS "📊.1",
             CAST(("jump.1"."height") AS INTEGER) AS "height",
             CAST(("jump.1"."width") AS INTEGER) AS "width",
             CAST(("jump.1"."iterations") AS INTEGER) AS "iterations",
             CAST(("jump.1"."x") AS BIGINT) AS "x",
             CAST(("jump.1"."y") AS BIGINT) AS "y",
             CAST(("jump.1"."alive") AS BOOLEAN) AS "alive"
      FROM   "jump.1"))
  )
SELECT count(*) AS outputs,
       sum(length("🔄"."📊.1"))::BIGINT AS output_bytes,
       md5(string_agg("🔄"."📊.1", '' ORDER BY "🔄"."#️⃣", "🔄"."📊.1")) AS output_md5
FROM   "🔄"
WHERE  "🔄"."🏷️" IS NULL;
