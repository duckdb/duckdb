WITH RECURSIVE
  "🔄"("#️⃣", "🏷️", "📊.1", "📊.2", "centroid", "centroids") AS (
    (SELECT CAST((0) AS INTEGER) AS "#️⃣",
            CAST(('start.1') AS VARCHAR) AS "🏷️",
            CAST((NULL) AS STRUCT(x FLOAT, y FLOAT)) AS "📊.1",
            CAST((NULL) AS STRUCT(x FLOAT, y FLOAT)[]) AS "📊.2",
            CAST((NULL) AS STRUCT(x FLOAT, y FLOAT)) AS "centroid",
            CAST((NULL) AS DOUBLE) AS "centroids")
      UNION ALL
    (WITH
       "start.1"("#️⃣", "⚙️") AS (
         SELECT "🔄"."#️⃣" AS "#️⃣",
                NULL AS "⚙️"
         FROM   "🔄"
         WHERE  "🔄"."🏷️" IS NOT DISTINCT FROM 'start.1'
       ),
       "fork.1"("#️⃣", "⚙️", "centroid") AS (
         SELECT "start.1"."#️⃣" AS "#️⃣",
                "start.1"."⚙️" AS "⚙️",
                CAST(("ℚ"."centroid") AS STRUCT(x FLOAT, y FLOAT)) AS "centroid"
         FROM   "start.1",
                (SELECT p
                 FROM   (SELECT p, row_number() OVER (ORDER BY p.x, p.y) AS rn
                         FROM points AS p)
                 WHERE  rn % 1000 = 1) AS "ℚ"("centroid")
       ),
       "assignment.1"("#️⃣", "⚙️", "centroid", "centroids") AS (
         SELECT "fork.1"."#️⃣" AS "#️⃣",
                "fork.1"."⚙️" AS "⚙️",
                "fork.1"."centroid" AS "centroid",
                CAST((sum(("fork.1"."centroid").x + ("fork.1"."centroid").y) OVER ()) AS DOUBLE) AS "centroids"
         FROM   "fork.1"
       ),
       "start.2"("#️⃣", "⚙️", "centroid", "centroids") AS (
         SELECT "🔄"."#️⃣" AS "#️⃣",
                NULL AS "⚙️",
                "🔄"."centroid" AS "centroid",
                "🔄"."centroids" AS "centroids"
         FROM   "🔄"
         WHERE  "🔄"."🏷️" IS NOT DISTINCT FROM 'start.2'
       ),
       "merge.1"("#️⃣", "⚙️", "centroid", "centroids") AS (
         (SELECT "assignment.1"."#️⃣" AS "#️⃣",
                 "assignment.1"."⚙️" AS "⚙️",
                 "assignment.1"."centroid" AS "centroid",
                 "assignment.1"."centroids" AS "centroids"
          FROM   "assignment.1")
           UNION ALL
         (SELECT "start.2"."#️⃣" AS "#️⃣",
                 "start.2"."⚙️" AS "⚙️",
                 "start.2"."centroid" AS "centroid",
                 "start.2"."centroids" AS "centroids"
          FROM   "start.2")
       ),
       "fork.2"("#️⃣", "⚙️", "centroid", "centroids", "point") AS (
         SELECT "merge.1"."#️⃣" AS "#️⃣",
                "merge.1"."⚙️" AS "⚙️",
                "merge.1"."centroid" AS "centroid",
                "merge.1"."centroids" AS "centroids",
                CAST(("ℚ"."point") AS STRUCT(x FLOAT, y FLOAT)) AS "point"
         FROM   "merge.1",
                (SELECT p FROM points AS p) AS "ℚ"("point")
       ),
       "assignment.2"("#️⃣", "⚙️", "centroid", "centroids", "point", "distance") AS (
         SELECT "fork.2"."#️⃣" AS "#️⃣",
                "fork.2"."⚙️" AS "⚙️",
                "fork.2"."centroid" AS "centroid",
                "fork.2"."centroids" AS "centroids",
                "fork.2"."point" AS "point",
                CAST((sqrt((("fork.2"."point").x - ("fork.2"."centroid").x) ** 2 + (("fork.2"."point").y - ("fork.2"."centroid").y) ** 2)) AS DOUBLE) AS "distance"
         FROM   "fork.2"
       ),
       "gather.1"("#️⃣", "⚙️", "centroid", "centroids", "point") AS (
         SELECT "assignment.2"."#️⃣" AS "#️⃣",
                "assignment.2"."⚙️" AS "⚙️",
                CAST((arg_min(("assignment.2"."centroid"), ("assignment.2"."distance"))) AS STRUCT(x FLOAT, y FLOAT)) AS "centroid",
                "assignment.2"."centroids" AS "centroids",
                "assignment.2"."point" AS "point"
         FROM   "assignment.2"
         GROUP  BY "assignment.2"."#️⃣",
                   "assignment.2"."centroids",
                   "assignment.2"."point",
                   "assignment.2"."⚙️"
         HAVING COUNT(*) > 0
       ),
       "gather.2"("#️⃣", "⚙️", "centroid", "centroids", "new_centroid") AS (
         SELECT "gather.1"."#️⃣" AS "#️⃣",
                "gather.1"."⚙️" AS "⚙️",
                "gather.1"."centroid" AS "centroid",
                "gather.1"."centroids" AS "centroids",
                CAST(({ 'x': avg(("gather.1"."point").x), 'y': avg(("gather.1"."point").y) }) AS STRUCT(x DOUBLE, y DOUBLE)) AS "new_centroid"
         FROM   "gather.1"
         GROUP  BY "gather.1"."#️⃣",
                   "gather.1"."centroid",
                   "gather.1"."centroids",
                   "gather.1"."⚙️"
         HAVING COUNT(*) > 0
       ),
       "assignment.3"("#️⃣", "⚙️", "centroid", "centroids", "new_centroid", "old_centroids") AS (
         SELECT "gather.2"."#️⃣" AS "#️⃣",
                "gather.2"."⚙️" AS "⚙️",
                "gather.2"."centroid" AS "centroid",
                CAST((sum(("gather.2"."new_centroid").x + ("gather.2"."new_centroid").y) OVER ()) AS DOUBLE) AS "centroids",
                "gather.2"."new_centroid" AS "new_centroid",
                CAST((("gather.2"."centroids")) AS DOUBLE) AS "old_centroids"
         FROM   "gather.2"
       ),
       "assignment.4"("#️⃣", "⚙️", "🔍", "centroid", "centroids", "new_centroid") AS (
         SELECT "assignment.3"."#️⃣" AS "#️⃣",
                "assignment.3"."⚙️" AS "⚙️",
                CAST((abs(("assignment.3"."old_centroids") - ("assignment.3"."centroids")) <= 0.000001) AS BOOLEAN) AS "🔍",
                "assignment.3"."centroid" AS "centroid",
                "assignment.3"."centroids" AS "centroids",
                "assignment.3"."new_centroid" AS "new_centroid"
         FROM   "assignment.3"
       ),
       "where.1"("#️⃣", "⚙️", "centroid") AS (
         SELECT "assignment.4"."#️⃣" AS "#️⃣",
                "assignment.4"."⚙️" AS "⚙️",
                "assignment.4"."centroid" AS "centroid"
         FROM   "assignment.4"
         WHERE  "assignment.4"."🔍" IS NOT DISTINCT FROM TRUE
       ),
       "fork.3"("#️⃣", "⚙️", "centroid", "point") AS (
         SELECT "where.1"."#️⃣" AS "#️⃣",
                "where.1"."⚙️" AS "⚙️",
                "where.1"."centroid" AS "centroid",
                CAST(("ℚ"."point") AS STRUCT(x FLOAT, y FLOAT)) AS "point"
         FROM   "where.1",
                (SELECT p FROM points AS p) AS "ℚ"("point")
       ),
       "assignment.6"("#️⃣", "⚙️", "centroid", "point", "distance") AS (
         SELECT "fork.3"."#️⃣" AS "#️⃣",
                "fork.3"."⚙️" AS "⚙️",
                "fork.3"."centroid" AS "centroid",
                "fork.3"."point" AS "point",
                CAST((sqrt((("fork.3"."point").x - ("fork.3"."centroid").x) ** 2 + (("fork.3"."point").y - ("fork.3"."centroid").y) ** 2)) AS DOUBLE) AS "distance"
         FROM   "fork.3"
       ),
       "gather.3"("#️⃣", "⚙️", "centroid", "point") AS (
         SELECT "assignment.6"."#️⃣" AS "#️⃣",
                "assignment.6"."⚙️" AS "⚙️",
                CAST((arg_min(("assignment.6"."centroid"), ("assignment.6"."distance"))) AS STRUCT(x FLOAT, y FLOAT)) AS "centroid",
                "assignment.6"."point" AS "point"
         FROM   "assignment.6"
         GROUP  BY "assignment.6"."#️⃣",
                   "assignment.6"."point",
                   "assignment.6"."⚙️"
         HAVING COUNT(*) > 0
       ),
       "gather.4"("#️⃣", "⚙️", "centroid", "cluster") AS (
         SELECT "gather.3"."#️⃣" AS "#️⃣",
                "gather.3"."⚙️" AS "⚙️",
                "gather.3"."centroid" AS "centroid",
                CAST((list(("gather.3"."point"))) AS STRUCT(x FLOAT, y FLOAT)[]) AS "cluster"
         FROM   "gather.3"
         GROUP  BY "gather.3"."#️⃣",
                   "gather.3"."centroid",
                   "gather.3"."⚙️"
         HAVING COUNT(*) > 0
       ),
       "emit.1"("#️⃣", "⚙️", "📊.1", "📊.2") AS (
         SELECT "gather.4"."#️⃣" AS "#️⃣",
                "gather.4"."⚙️" AS "⚙️",
                "gather.4"."centroid" AS "📊.1",
                "gather.4"."cluster" AS "📊.2"
         FROM   "gather.4"
       ),
       "stop.1"("⚙️") AS (
         SELECT "emit.1"."⚙️"
         FROM   "emit.1"
         WHERE  FALSE
       ),
       "where.2"("#️⃣", "⚙️", "centroids", "new_centroid") AS (
         SELECT "assignment.4"."#️⃣" AS "#️⃣",
                "assignment.4"."⚙️" AS "⚙️",
                "assignment.4"."centroids" AS "centroids",
                "assignment.4"."new_centroid" AS "new_centroid"
         FROM   "assignment.4"
         WHERE  "assignment.4"."🔍" IS DISTINCT FROM TRUE
       ),
       "assignment.5"("#️⃣", "⚙️", "centroid", "centroids") AS (
         SELECT "where.2"."#️⃣" AS "#️⃣",
                "where.2"."⚙️" AS "⚙️",
                CAST((("where.2"."new_centroid")) AS STRUCT(x FLOAT, y FLOAT)) AS "centroid",
                "where.2"."centroids" AS "centroids"
         FROM   "where.2"
       ),
       "jump.1"("#️⃣", "🏷️", "centroid", "centroids") AS (
         SELECT "assignment.5"."#️⃣" AS "#️⃣",
                'start.2' AS "🏷️",
                "assignment.5"."centroid" AS "centroid",
                "assignment.5"."centroids" AS "centroids"
         FROM   "assignment.5"
       )
     (SELECT CAST(("emit.1"."#️⃣") AS INTEGER) AS "#️⃣",
             CAST((NULL) AS VARCHAR) AS "🏷️",
             CAST(("emit.1"."📊.1") AS STRUCT(x FLOAT, y FLOAT)) AS "📊.1",
             CAST(("emit.1"."📊.2") AS STRUCT(x FLOAT, y FLOAT)[]) AS "📊.2",
             CAST((NULL) AS STRUCT(x FLOAT, y FLOAT)) AS "centroid",
             CAST((NULL) AS DOUBLE) AS "centroids"
      FROM   "emit.1")
       UNION ALL
     (SELECT CAST(("jump.1"."#️⃣" + 1) AS INTEGER) AS "#️⃣",
             CAST(("jump.1"."🏷️") AS VARCHAR) AS "🏷️",
             CAST((NULL) AS STRUCT(x FLOAT, y FLOAT)) AS "📊.1",
             CAST((NULL) AS STRUCT(x FLOAT, y FLOAT)[]) AS "📊.2",
             CAST(("jump.1"."centroid") AS STRUCT(x FLOAT, y FLOAT)) AS "centroid",
             CAST(("jump.1"."centroids") AS DOUBLE) AS "centroids"
      FROM   "jump.1"))
  )
SELECT count(*) AS clusters,
       sum(len("🔄"."📊.2"))::BIGINT AS points,
       round(sum(("🔄"."📊.1").x)::DOUBLE, 6) AS centroid_x_sum,
       round(sum(("🔄"."📊.1").y)::DOUBLE, 6) AS centroid_y_sum,
       min(len("🔄"."📊.2")) AS min_cluster_size,
       max(len("🔄"."📊.2")) AS max_cluster_size
FROM   "🔄"
WHERE  "🔄"."🏷️" IS NULL;
