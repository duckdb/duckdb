WITH RECURSIVE
  "🔄"("#️⃣", "🏷️", "📊.1", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
    (SELECT CAST((0) AS INTEGER) AS "#️⃣",
            CAST(('start.1') AS VARCHAR) AS "🏷️",
            CAST((NULL) AS VARCHAR) AS "📊.1",
            CAST((NULL) AS INTEGER) AS "height",
            CAST((NULL) AS INTEGER) AS "width",
            CAST((NULL) AS INTEGER) AS "iterations",
            CAST((NULL) AS INTEGER) AS "max_age",
            CAST((NULL) AS INTEGER[]) AS "birth_cond",
            CAST((NULL) AS "NULL"[]) AS "survive_cond",
            CAST((NULL) AS BIGINT) AS "x",
            CAST((NULL) AS BIGINT) AS "y",
            CAST((NULL) AS INTEGER) AS "age")
      UNION ALL
    (WITH
       "start.1"("#️⃣", "⚙️") AS (
         SELECT "🔄"."#️⃣" AS "#️⃣",
                NULL AS "⚙️"
         FROM   "🔄"
         WHERE  "🔄"."🏷️" IS NOT DISTINCT FROM 'start.1'
       ),
       "assignment.1"("#️⃣", "⚙️", "height", "width", "iterations", "max_age") AS (
         SELECT "start.1"."#️⃣" AS "#️⃣",
                "start.1"."⚙️" AS "⚙️",
                CAST((50) AS INTEGER) AS "height",
                CAST((50) AS INTEGER) AS "width",
                CAST((50) AS INTEGER) AS "iterations",
                CAST((3) AS INTEGER) AS "max_age"
         FROM   "start.1"
       ),
       "assignment.2"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond") AS (
         SELECT "assignment.1"."#️⃣" AS "#️⃣",
                "assignment.1"."⚙️" AS "⚙️",
                "assignment.1"."height" AS "height",
                "assignment.1"."width" AS "width",
                "assignment.1"."iterations" AS "iterations",
                "assignment.1"."max_age" AS "max_age",
                CAST(([2]) AS INTEGER[]) AS "birth_cond",
                CAST(([]) AS "NULL"[]) AS "survive_cond"
         FROM   "assignment.1"
       ),
       "fork.1"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x") AS (
         SELECT "assignment.2"."#️⃣" AS "#️⃣",
                "assignment.2"."⚙️" AS "⚙️",
                "assignment.2"."height" AS "height",
                "assignment.2"."width" AS "width",
                "assignment.2"."iterations" AS "iterations",
                "assignment.2"."max_age" AS "max_age",
                "assignment.2"."birth_cond" AS "birth_cond",
                "assignment.2"."survive_cond" AS "survive_cond",
                CAST(("ℚ"."x") AS BIGINT) AS "x"
         FROM   "assignment.2",
                LATERAL (FROM range(("assignment.2"."width"))) AS "ℚ"("x")
       ),
       "fork.2"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y") AS (
         SELECT "fork.1"."#️⃣" AS "#️⃣",
                "fork.1"."⚙️" AS "⚙️",
                "fork.1"."height" AS "height",
                "fork.1"."width" AS "width",
                "fork.1"."iterations" AS "iterations",
                "fork.1"."max_age" AS "max_age",
                "fork.1"."birth_cond" AS "birth_cond",
                "fork.1"."survive_cond" AS "survive_cond",
                "fork.1"."x" AS "x",
                CAST(("ℚ"."y") AS BIGINT) AS "y"
         FROM   "fork.1",
                LATERAL (FROM range(("fork.1"."height"))) AS "ℚ"("y")
       ),
       "assignment.3"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
         SELECT "fork.2"."#️⃣" AS "#️⃣",
                "fork.2"."⚙️" AS "⚙️",
                "fork.2"."height" AS "height",
                "fork.2"."width" AS "width",
                "fork.2"."iterations" AS "iterations",
                "fork.2"."max_age" AS "max_age",
                "fork.2"."birth_cond" AS "birth_cond",
                "fork.2"."survive_cond" AS "survive_cond",
                "fork.2"."x" AS "x",
                "fork.2"."y" AS "y",
                CAST(((("fork.2"."x" * 17 + "fork.2"."y" * 31) % 10 < 4) :: int) AS INTEGER) AS "age"
         FROM   "fork.2"
       ),
       "start.2"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
         SELECT "🔄"."#️⃣" AS "#️⃣",
                NULL AS "⚙️",
                "🔄"."height" AS "height",
                "🔄"."width" AS "width",
                "🔄"."iterations" AS "iterations",
                "🔄"."max_age" AS "max_age",
                "🔄"."birth_cond" AS "birth_cond",
                "🔄"."survive_cond" AS "survive_cond",
                "🔄"."x" AS "x",
                "🔄"."y" AS "y",
                "🔄"."age" AS "age"
         FROM   "🔄"
         WHERE  "🔄"."🏷️" IS NOT DISTINCT FROM 'start.2'
       ),
       "merge.1"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
         (SELECT "assignment.3"."#️⃣" AS "#️⃣",
                 "assignment.3"."⚙️" AS "⚙️",
                 "assignment.3"."height" AS "height",
                 "assignment.3"."width" AS "width",
                 "assignment.3"."iterations" AS "iterations",
                 "assignment.3"."max_age" AS "max_age",
                 "assignment.3"."birth_cond" AS "birth_cond",
                 "assignment.3"."survive_cond" AS "survive_cond",
                 "assignment.3"."x" AS "x",
                 "assignment.3"."y" AS "y",
                 "assignment.3"."age" AS "age"
          FROM   "assignment.3")
           UNION ALL
         (SELECT "start.2"."#️⃣" AS "#️⃣",
                 "start.2"."⚙️" AS "⚙️",
                 "start.2"."height" AS "height",
                 "start.2"."width" AS "width",
                 "start.2"."iterations" AS "iterations",
                 "start.2"."max_age" AS "max_age",
                 "start.2"."birth_cond" AS "birth_cond",
                 "start.2"."survive_cond" AS "survive_cond",
                 "start.2"."x" AS "x",
                 "start.2"."y" AS "y",
                 "start.2"."age" AS "age"
          FROM   "start.2")
       ),
       "fork.3"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age", "flip") AS (
         SELECT "merge.1"."#️⃣" AS "#️⃣",
                "merge.1"."⚙️" AS "⚙️",
                "merge.1"."height" AS "height",
                "merge.1"."width" AS "width",
                "merge.1"."iterations" AS "iterations",
                "merge.1"."max_age" AS "max_age",
                "merge.1"."birth_cond" AS "birth_cond",
                "merge.1"."survive_cond" AS "survive_cond",
                "merge.1"."x" AS "x",
                "merge.1"."y" AS "y",
                "merge.1"."age" AS "age",
                CAST(("ℚ"."flip") AS BOOLEAN) AS "flip"
         FROM   "merge.1",
                (VALUES (TRUE), (FALSE)) AS "ℚ"("flip")
       ),
       "assignment.4"("#️⃣", "⚙️", "🔍", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
         SELECT "fork.3"."#️⃣" AS "#️⃣",
                "fork.3"."⚙️" AS "⚙️",
                CAST((("fork.3"."flip")) AS BOOLEAN) AS "🔍",
                "fork.3"."height" AS "height",
                "fork.3"."width" AS "width",
                "fork.3"."iterations" AS "iterations",
                "fork.3"."max_age" AS "max_age",
                "fork.3"."birth_cond" AS "birth_cond",
                "fork.3"."survive_cond" AS "survive_cond",
                "fork.3"."x" AS "x",
                "fork.3"."y" AS "y",
                "fork.3"."age" AS "age"
         FROM   "fork.3"
       ),
       "where.1"("#️⃣", "⚙️", "width", "iterations", "x", "y", "age") AS (
         SELECT "assignment.4"."#️⃣" AS "#️⃣",
                "assignment.4"."⚙️" AS "⚙️",
                "assignment.4"."width" AS "width",
                "assignment.4"."iterations" AS "iterations",
                "assignment.4"."x" AS "x",
                "assignment.4"."y" AS "y",
                "assignment.4"."age" AS "age"
         FROM   "assignment.4"
         WHERE  "assignment.4"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "gather.2"("#️⃣", "⚙️", "width", "iterations", "y", "buffer") AS (
         SELECT "where.1"."#️⃣" AS "#️⃣",
                "where.1"."⚙️" AS "⚙️",
                "where.1"."width" AS "width",
                "where.1"."iterations" AS "iterations",
                "where.1"."y" AS "y",
                CAST((string_agg([' ', '#', '*'][("where.1"."age") + 1], '' ORDER BY ("where.1"."x"))) AS VARCHAR) AS "buffer"
         FROM   "where.1"
         GROUP  BY "where.1"."#️⃣",
                   "where.1"."iterations",
                   "where.1"."width",
                   "where.1"."y",
                   "where.1"."⚙️"
         HAVING COUNT(*) > 0
       ),
       "gather.3"("#️⃣", "⚙️", "width", "iterations", "buffer") AS (
         SELECT "gather.2"."#️⃣" AS "#️⃣",
                "gather.2"."⚙️" AS "⚙️",
                "gather.2"."width" AS "width",
                "gather.2"."iterations" AS "iterations",
                CAST((string_agg(("gather.2"."buffer"), E'\n' ORDER BY ("gather.2"."y"))) AS VARCHAR) AS "buffer"
         FROM   "gather.2"
         GROUP  BY "gather.2"."#️⃣",
                   "gather.2"."iterations",
                   "gather.2"."width",
                   "gather.2"."⚙️"
         HAVING COUNT(*) > 0
       ),
       "emit.1"("#️⃣", "⚙️", "📊.1", "width", "iterations") AS (
         SELECT "gather.3"."#️⃣" AS "#️⃣",
                "gather.3"."⚙️" AS "⚙️",
                "gather.3"."buffer" AS "📊.1",
                "gather.3"."width" AS "width",
                "gather.3"."iterations" AS "iterations"
         FROM   "gather.3"
       ),
       "assignment.16"("#️⃣", "⚙️", "width", "buffer") AS (
         SELECT "emit.1"."#️⃣" AS "#️⃣",
                "emit.1"."⚙️" AS "⚙️",
                "emit.1"."width" AS "width",
                CAST((("emit.1"."iterations")) AS VARCHAR) AS "buffer"
         FROM   "emit.1"
       ),
       "assignment.17"("#️⃣", "⚙️", "buffer") AS (
         SELECT "assignment.16"."#️⃣" AS "#️⃣",
                "assignment.16"."⚙️" AS "⚙️",
                CAST((("assignment.16"."buffer") || ' ' || repeat('-.', (("assignment.16"."width") - length(("assignment.16"."buffer") :: text) - 1) // 2)) AS VARCHAR) AS "buffer"
         FROM   "assignment.16"
       ),
       "emit.2"("#️⃣", "⚙️", "📊.1") AS (
         SELECT "assignment.17"."#️⃣" AS "#️⃣",
                "assignment.17"."⚙️" AS "⚙️",
                "assignment.17"."buffer" AS "📊.1"
         FROM   "assignment.17"
       ),
       "stop.2"("⚙️") AS (
         SELECT "emit.2"."⚙️"
         FROM   "emit.2"
         WHERE  FALSE
       ),
       "where.2"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
         SELECT "assignment.4"."#️⃣" AS "#️⃣",
                "assignment.4"."⚙️" AS "⚙️",
                "assignment.4"."height" AS "height",
                "assignment.4"."width" AS "width",
                "assignment.4"."iterations" AS "iterations",
                "assignment.4"."max_age" AS "max_age",
                "assignment.4"."birth_cond" AS "birth_cond",
                "assignment.4"."survive_cond" AS "survive_cond",
                "assignment.4"."x" AS "x",
                "assignment.4"."y" AS "y",
                "assignment.4"."age" AS "age"
         FROM   "assignment.4"
         WHERE  "assignment.4"."🔍" IS DISTINCT FROM TRUE
       ),
       "assignment.5"("#️⃣", "⚙️", "🔍", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
         SELECT "where.2"."#️⃣" AS "#️⃣",
                "where.2"."⚙️" AS "⚙️",
                CAST((("where.2"."iterations") = 0) AS BOOLEAN) AS "🔍",
                "where.2"."height" AS "height",
                "where.2"."width" AS "width",
                "where.2"."iterations" AS "iterations",
                "where.2"."max_age" AS "max_age",
                "where.2"."birth_cond" AS "birth_cond",
                "where.2"."survive_cond" AS "survive_cond",
                "where.2"."x" AS "x",
                "where.2"."y" AS "y",
                "where.2"."age" AS "age"
         FROM   "where.2"
       ),
       "where.3"("⚙️") AS (
         SELECT "assignment.5"."⚙️" AS "⚙️"
         FROM   "assignment.5"
         WHERE  "assignment.5"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "stop.1"("⚙️") AS (
         SELECT "where.3"."⚙️"
         FROM   "where.3"
         WHERE  FALSE
       ),
       "where.4"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
         SELECT "assignment.5"."#️⃣" AS "#️⃣",
                "assignment.5"."⚙️" AS "⚙️",
                "assignment.5"."height" AS "height",
                "assignment.5"."width" AS "width",
                "assignment.5"."iterations" AS "iterations",
                "assignment.5"."max_age" AS "max_age",
                "assignment.5"."birth_cond" AS "birth_cond",
                "assignment.5"."survive_cond" AS "survive_cond",
                "assignment.5"."x" AS "x",
                "assignment.5"."y" AS "y",
                "assignment.5"."age" AS "age"
         FROM   "assignment.5"
         WHERE  "assignment.5"."🔍" IS DISTINCT FROM TRUE
       ),
       "assignment.6"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
         SELECT "where.4"."#️⃣" AS "#️⃣",
                "where.4"."⚙️" AS "⚙️",
                "where.4"."height" AS "height",
                "where.4"."width" AS "width",
                CAST((("where.4"."iterations") - 1) AS INTEGER) AS "iterations",
                "where.4"."max_age" AS "max_age",
                "where.4"."birth_cond" AS "birth_cond",
                "where.4"."survive_cond" AS "survive_cond",
                "where.4"."x" AS "x",
                "where.4"."y" AS "y",
                "where.4"."age" AS "age"
         FROM   "where.4"
       ),
       "fork.4"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age", "dy") AS (
         SELECT "assignment.6"."#️⃣" AS "#️⃣",
                "assignment.6"."⚙️" AS "⚙️",
                "assignment.6"."height" AS "height",
                "assignment.6"."width" AS "width",
                "assignment.6"."iterations" AS "iterations",
                "assignment.6"."max_age" AS "max_age",
                "assignment.6"."birth_cond" AS "birth_cond",
                "assignment.6"."survive_cond" AS "survive_cond",
                "assignment.6"."x" AS "x",
                "assignment.6"."y" AS "y",
                "assignment.6"."age" AS "age",
                CAST(("ℚ"."dy") AS BIGINT) AS "dy"
         FROM   "assignment.6",
                (FROM generate_series(-1, +1)) AS "ℚ"("dy")
       ),
       "fork.5"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age", "dy", "dx") AS (
         SELECT "fork.4"."#️⃣" AS "#️⃣",
                "fork.4"."⚙️" AS "⚙️",
                "fork.4"."height" AS "height",
                "fork.4"."width" AS "width",
                "fork.4"."iterations" AS "iterations",
                "fork.4"."max_age" AS "max_age",
                "fork.4"."birth_cond" AS "birth_cond",
                "fork.4"."survive_cond" AS "survive_cond",
                "fork.4"."x" AS "x",
                "fork.4"."y" AS "y",
                "fork.4"."age" AS "age",
                "fork.4"."dy" AS "dy",
                CAST(("ℚ"."dx") AS BIGINT) AS "dx"
         FROM   "fork.4",
                (FROM generate_series(-1, +1)) AS "ℚ"("dx")
       ),
       "assignment.7"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age", "dy", "dx") AS (
         SELECT "fork.5"."#️⃣" AS "#️⃣",
                "fork.5"."⚙️" AS "⚙️",
                "fork.5"."height" AS "height",
                "fork.5"."width" AS "width",
                "fork.5"."iterations" AS "iterations",
                "fork.5"."max_age" AS "max_age",
                "fork.5"."birth_cond" AS "birth_cond",
                "fork.5"."survive_cond" AS "survive_cond",
                CAST(((("fork.5"."width")  + (("fork.5"."x") + ("fork.5"."dx")) % ("fork.5"."width") ) % ("fork.5"."width")) AS BIGINT) AS "x",
                CAST(((("fork.5"."height") + (("fork.5"."y") + ("fork.5"."dy")) % ("fork.5"."height")) % ("fork.5"."height")) AS BIGINT) AS "y",
                "fork.5"."age" AS "age",
                "fork.5"."dy" AS "dy",
                "fork.5"."dx" AS "dx"
         FROM   "fork.5"
       ),
       "assignment.8"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age", "original") AS (
         SELECT "assignment.7"."#️⃣" AS "#️⃣",
                "assignment.7"."⚙️" AS "⚙️",
                "assignment.7"."height" AS "height",
                "assignment.7"."width" AS "width",
                "assignment.7"."iterations" AS "iterations",
                "assignment.7"."max_age" AS "max_age",
                "assignment.7"."birth_cond" AS "birth_cond",
                "assignment.7"."survive_cond" AS "survive_cond",
                "assignment.7"."x" AS "x",
                "assignment.7"."y" AS "y",
                "assignment.7"."age" AS "age",
                CAST((("assignment.7"."dx") = 0 AND ("assignment.7"."dy") = 0) AS BOOLEAN) AS "original"
         FROM   "assignment.7"
       ),
       "gather.1"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age", "neighbors") AS (
         SELECT "assignment.8"."#️⃣" AS "#️⃣",
                "assignment.8"."⚙️" AS "⚙️",
                "assignment.8"."height" AS "height",
                "assignment.8"."width" AS "width",
                "assignment.8"."iterations" AS "iterations",
                "assignment.8"."max_age" AS "max_age",
                "assignment.8"."birth_cond" AS "birth_cond",
                "assignment.8"."survive_cond" AS "survive_cond",
                "assignment.8"."x" AS "x",
                "assignment.8"."y" AS "y",
                CAST((any_value(("assignment.8"."age"))   FILTER (WHERE     ("assignment.8"."original"))) AS INTEGER) AS "age",
                CAST((countif(("assignment.8"."age") = 1) FILTER (WHERE NOT ("assignment.8"."original"))) AS HUGEINT) AS "neighbors"
         FROM   "assignment.8"
         GROUP  BY "assignment.8"."#️⃣",
                   "assignment.8"."birth_cond",
                   "assignment.8"."height",
                   "assignment.8"."iterations",
                   "assignment.8"."max_age",
                   "assignment.8"."survive_cond",
                   "assignment.8"."width",
                   "assignment.8"."x",
                   "assignment.8"."y",
                   "assignment.8"."⚙️"
         HAVING COUNT(*) > 0
       ),
       "assignment.9"("#️⃣", "⚙️", "🔍", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age", "neighbors") AS (
         SELECT "gather.1"."#️⃣" AS "#️⃣",
                "gather.1"."⚙️" AS "⚙️",
                CAST((("gather.1"."age") = 0 AND     list_contains(("gather.1"."birth_cond"),   ("gather.1"."neighbors"))) AS BOOLEAN) AS "🔍",
                "gather.1"."height" AS "height",
                "gather.1"."width" AS "width",
                "gather.1"."iterations" AS "iterations",
                "gather.1"."max_age" AS "max_age",
                "gather.1"."birth_cond" AS "birth_cond",
                "gather.1"."survive_cond" AS "survive_cond",
                "gather.1"."x" AS "x",
                "gather.1"."y" AS "y",
                "gather.1"."age" AS "age",
                "gather.1"."neighbors" AS "neighbors"
         FROM   "gather.1"
       ),
       "where.5"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y") AS (
         SELECT "assignment.9"."#️⃣" AS "#️⃣",
                "assignment.9"."⚙️" AS "⚙️",
                "assignment.9"."height" AS "height",
                "assignment.9"."width" AS "width",
                "assignment.9"."iterations" AS "iterations",
                "assignment.9"."max_age" AS "max_age",
                "assignment.9"."birth_cond" AS "birth_cond",
                "assignment.9"."survive_cond" AS "survive_cond",
                "assignment.9"."x" AS "x",
                "assignment.9"."y" AS "y"
         FROM   "assignment.9"
         WHERE  "assignment.9"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "assignment.10"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
         SELECT "where.5"."#️⃣" AS "#️⃣",
                "where.5"."⚙️" AS "⚙️",
                "where.5"."height" AS "height",
                "where.5"."width" AS "width",
                "where.5"."iterations" AS "iterations",
                "where.5"."max_age" AS "max_age",
                "where.5"."birth_cond" AS "birth_cond",
                "where.5"."survive_cond" AS "survive_cond",
                "where.5"."x" AS "x",
                "where.5"."y" AS "y",
                CAST((1) AS INTEGER) AS "age"
         FROM   "where.5"
       ),
       "where.6"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age", "neighbors") AS (
         SELECT "assignment.9"."#️⃣" AS "#️⃣",
                "assignment.9"."⚙️" AS "⚙️",
                "assignment.9"."height" AS "height",
                "assignment.9"."width" AS "width",
                "assignment.9"."iterations" AS "iterations",
                "assignment.9"."max_age" AS "max_age",
                "assignment.9"."birth_cond" AS "birth_cond",
                "assignment.9"."survive_cond" AS "survive_cond",
                "assignment.9"."x" AS "x",
                "assignment.9"."y" AS "y",
                "assignment.9"."age" AS "age",
                "assignment.9"."neighbors" AS "neighbors"
         FROM   "assignment.9"
         WHERE  "assignment.9"."🔍" IS DISTINCT FROM TRUE
       ),
       "assignment.11"("#️⃣", "⚙️", "🔍", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
         SELECT "where.6"."#️⃣" AS "#️⃣",
                "where.6"."⚙️" AS "⚙️",
                CAST((("where.6"."age") = 1 AND NOT list_contains(("where.6"."survive_cond"), ("where.6"."neighbors"))) AS BOOLEAN) AS "🔍",
                "where.6"."height" AS "height",
                "where.6"."width" AS "width",
                "where.6"."iterations" AS "iterations",
                "where.6"."max_age" AS "max_age",
                "where.6"."birth_cond" AS "birth_cond",
                "where.6"."survive_cond" AS "survive_cond",
                "where.6"."x" AS "x",
                "where.6"."y" AS "y",
                "where.6"."age" AS "age"
         FROM   "where.6"
       ),
       "where.7"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y") AS (
         SELECT "assignment.11"."#️⃣" AS "#️⃣",
                "assignment.11"."⚙️" AS "⚙️",
                "assignment.11"."height" AS "height",
                "assignment.11"."width" AS "width",
                "assignment.11"."iterations" AS "iterations",
                "assignment.11"."max_age" AS "max_age",
                "assignment.11"."birth_cond" AS "birth_cond",
                "assignment.11"."survive_cond" AS "survive_cond",
                "assignment.11"."x" AS "x",
                "assignment.11"."y" AS "y"
         FROM   "assignment.11"
         WHERE  "assignment.11"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "assignment.12"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
         SELECT "where.7"."#️⃣" AS "#️⃣",
                "where.7"."⚙️" AS "⚙️",
                "where.7"."height" AS "height",
                "where.7"."width" AS "width",
                "where.7"."iterations" AS "iterations",
                "where.7"."max_age" AS "max_age",
                "where.7"."birth_cond" AS "birth_cond",
                "where.7"."survive_cond" AS "survive_cond",
                "where.7"."x" AS "x",
                "where.7"."y" AS "y",
                CAST((2) AS INTEGER) AS "age"
         FROM   "where.7"
       ),
       "where.8"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
         SELECT "assignment.11"."#️⃣" AS "#️⃣",
                "assignment.11"."⚙️" AS "⚙️",
                "assignment.11"."height" AS "height",
                "assignment.11"."width" AS "width",
                "assignment.11"."iterations" AS "iterations",
                "assignment.11"."max_age" AS "max_age",
                "assignment.11"."birth_cond" AS "birth_cond",
                "assignment.11"."survive_cond" AS "survive_cond",
                "assignment.11"."x" AS "x",
                "assignment.11"."y" AS "y",
                "assignment.11"."age" AS "age"
         FROM   "assignment.11"
         WHERE  "assignment.11"."🔍" IS DISTINCT FROM TRUE
       ),
       "assignment.13"("#️⃣", "⚙️", "🔍", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
         SELECT "where.8"."#️⃣" AS "#️⃣",
                "where.8"."⚙️" AS "⚙️",
                CAST((("where.8"."age") > 1) AS BOOLEAN) AS "🔍",
                "where.8"."height" AS "height",
                "where.8"."width" AS "width",
                "where.8"."iterations" AS "iterations",
                "where.8"."max_age" AS "max_age",
                "where.8"."birth_cond" AS "birth_cond",
                "where.8"."survive_cond" AS "survive_cond",
                "where.8"."x" AS "x",
                "where.8"."y" AS "y",
                "where.8"."age" AS "age"
         FROM   "where.8"
       ),
       "where.10"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
         SELECT "assignment.13"."#️⃣" AS "#️⃣",
                "assignment.13"."⚙️" AS "⚙️",
                "assignment.13"."height" AS "height",
                "assignment.13"."width" AS "width",
                "assignment.13"."iterations" AS "iterations",
                "assignment.13"."max_age" AS "max_age",
                "assignment.13"."birth_cond" AS "birth_cond",
                "assignment.13"."survive_cond" AS "survive_cond",
                "assignment.13"."x" AS "x",
                "assignment.13"."y" AS "y",
                "assignment.13"."age" AS "age"
         FROM   "assignment.13"
         WHERE  "assignment.13"."🔍" IS DISTINCT FROM TRUE
       ),
       "where.9"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
         SELECT "assignment.13"."#️⃣" AS "#️⃣",
                "assignment.13"."⚙️" AS "⚙️",
                "assignment.13"."height" AS "height",
                "assignment.13"."width" AS "width",
                "assignment.13"."iterations" AS "iterations",
                "assignment.13"."max_age" AS "max_age",
                "assignment.13"."birth_cond" AS "birth_cond",
                "assignment.13"."survive_cond" AS "survive_cond",
                "assignment.13"."x" AS "x",
                "assignment.13"."y" AS "y",
                "assignment.13"."age" AS "age"
         FROM   "assignment.13"
         WHERE  "assignment.13"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "assignment.14"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
         SELECT "where.9"."#️⃣" AS "#️⃣",
                "where.9"."⚙️" AS "⚙️",
                "where.9"."height" AS "height",
                "where.9"."width" AS "width",
                "where.9"."iterations" AS "iterations",
                "where.9"."max_age" AS "max_age",
                "where.9"."birth_cond" AS "birth_cond",
                "where.9"."survive_cond" AS "survive_cond",
                "where.9"."x" AS "x",
                "where.9"."y" AS "y",
                CAST((("where.9"."age") + 1) AS INTEGER) AS "age"
         FROM   "where.9"
       ),
       "merge.2"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
         (SELECT "assignment.10"."#️⃣" AS "#️⃣",
                 "assignment.10"."⚙️" AS "⚙️",
                 "assignment.10"."height" AS "height",
                 "assignment.10"."width" AS "width",
                 "assignment.10"."iterations" AS "iterations",
                 "assignment.10"."max_age" AS "max_age",
                 "assignment.10"."birth_cond" AS "birth_cond",
                 "assignment.10"."survive_cond" AS "survive_cond",
                 "assignment.10"."x" AS "x",
                 "assignment.10"."y" AS "y",
                 "assignment.10"."age" AS "age"
          FROM   "assignment.10")
           UNION ALL
         (SELECT "assignment.12"."#️⃣" AS "#️⃣",
                 "assignment.12"."⚙️" AS "⚙️",
                 "assignment.12"."height" AS "height",
                 "assignment.12"."width" AS "width",
                 "assignment.12"."iterations" AS "iterations",
                 "assignment.12"."max_age" AS "max_age",
                 "assignment.12"."birth_cond" AS "birth_cond",
                 "assignment.12"."survive_cond" AS "survive_cond",
                 "assignment.12"."x" AS "x",
                 "assignment.12"."y" AS "y",
                 "assignment.12"."age" AS "age"
          FROM   "assignment.12")
           UNION ALL
         (SELECT "assignment.14"."#️⃣" AS "#️⃣",
                 "assignment.14"."⚙️" AS "⚙️",
                 "assignment.14"."height" AS "height",
                 "assignment.14"."width" AS "width",
                 "assignment.14"."iterations" AS "iterations",
                 "assignment.14"."max_age" AS "max_age",
                 "assignment.14"."birth_cond" AS "birth_cond",
                 "assignment.14"."survive_cond" AS "survive_cond",
                 "assignment.14"."x" AS "x",
                 "assignment.14"."y" AS "y",
                 "assignment.14"."age" AS "age"
          FROM   "assignment.14")
           UNION ALL
         (SELECT "where.10"."#️⃣" AS "#️⃣",
                 "where.10"."⚙️" AS "⚙️",
                 "where.10"."height" AS "height",
                 "where.10"."width" AS "width",
                 "where.10"."iterations" AS "iterations",
                 "where.10"."max_age" AS "max_age",
                 "where.10"."birth_cond" AS "birth_cond",
                 "where.10"."survive_cond" AS "survive_cond",
                 "where.10"."x" AS "x",
                 "where.10"."y" AS "y",
                 "where.10"."age" AS "age"
          FROM   "where.10")
       ),
       "assignment.15"("#️⃣", "⚙️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
         SELECT "merge.2"."#️⃣" AS "#️⃣",
                "merge.2"."⚙️" AS "⚙️",
                "merge.2"."height" AS "height",
                "merge.2"."width" AS "width",
                "merge.2"."iterations" AS "iterations",
                "merge.2"."max_age" AS "max_age",
                "merge.2"."birth_cond" AS "birth_cond",
                "merge.2"."survive_cond" AS "survive_cond",
                "merge.2"."x" AS "x",
                "merge.2"."y" AS "y",
                CAST((("merge.2"."age") % ("merge.2"."max_age")) AS INTEGER) AS "age"
         FROM   "merge.2"
       ),
       "jump.1"("#️⃣", "🏷️", "height", "width", "iterations", "max_age", "birth_cond", "survive_cond", "x", "y", "age") AS (
         SELECT "assignment.15"."#️⃣" AS "#️⃣",
                'start.2' AS "🏷️",
                "assignment.15"."height" AS "height",
                "assignment.15"."width" AS "width",
                "assignment.15"."iterations" AS "iterations",
                "assignment.15"."max_age" AS "max_age",
                "assignment.15"."birth_cond" AS "birth_cond",
                "assignment.15"."survive_cond" AS "survive_cond",
                "assignment.15"."x" AS "x",
                "assignment.15"."y" AS "y",
                "assignment.15"."age" AS "age"
         FROM   "assignment.15"
       )
     (SELECT CAST(("emit.1"."#️⃣") AS INTEGER) AS "#️⃣",
             CAST((NULL) AS VARCHAR) AS "🏷️",
             CAST(("emit.1"."📊.1") AS VARCHAR) AS "📊.1",
             CAST((NULL) AS INTEGER) AS "height",
             CAST((NULL) AS INTEGER) AS "width",
             CAST((NULL) AS INTEGER) AS "iterations",
             CAST((NULL) AS INTEGER) AS "max_age",
             CAST((NULL) AS INTEGER[]) AS "birth_cond",
             CAST((NULL) AS "NULL"[]) AS "survive_cond",
             CAST((NULL) AS BIGINT) AS "x",
             CAST((NULL) AS BIGINT) AS "y",
             CAST((NULL) AS INTEGER) AS "age"
      FROM   "emit.1")
       UNION ALL
     (SELECT CAST(("emit.2"."#️⃣") AS INTEGER) AS "#️⃣",
             CAST((NULL) AS VARCHAR) AS "🏷️",
             CAST(("emit.2"."📊.1") AS VARCHAR) AS "📊.1",
             CAST((NULL) AS INTEGER) AS "height",
             CAST((NULL) AS INTEGER) AS "width",
             CAST((NULL) AS INTEGER) AS "iterations",
             CAST((NULL) AS INTEGER) AS "max_age",
             CAST((NULL) AS INTEGER[]) AS "birth_cond",
             CAST((NULL) AS "NULL"[]) AS "survive_cond",
             CAST((NULL) AS BIGINT) AS "x",
             CAST((NULL) AS BIGINT) AS "y",
             CAST((NULL) AS INTEGER) AS "age"
      FROM   "emit.2")
       UNION ALL
     (SELECT CAST(("jump.1"."#️⃣" + 1) AS INTEGER) AS "#️⃣",
             CAST(("jump.1"."🏷️") AS VARCHAR) AS "🏷️",
             CAST((NULL) AS VARCHAR) AS "📊.1",
             CAST(("jump.1"."height") AS INTEGER) AS "height",
             CAST(("jump.1"."width") AS INTEGER) AS "width",
             CAST(("jump.1"."iterations") AS INTEGER) AS "iterations",
             CAST(("jump.1"."max_age") AS INTEGER) AS "max_age",
             CAST(("jump.1"."birth_cond") AS INTEGER[]) AS "birth_cond",
             CAST(("jump.1"."survive_cond") AS "NULL"[]) AS "survive_cond",
             CAST(("jump.1"."x") AS BIGINT) AS "x",
             CAST(("jump.1"."y") AS BIGINT) AS "y",
             CAST(("jump.1"."age") AS INTEGER) AS "age"
      FROM   "jump.1"))
  )
SELECT count(*) AS outputs,
       sum(length("🔄"."📊.1"))::BIGINT AS output_bytes,
       md5(string_agg("🔄"."📊.1", '' ORDER BY "🔄"."#️⃣", "🔄"."📊.1")) AS output_md5
FROM   "🔄"
WHERE  "🔄"."🏷️" IS NULL;
