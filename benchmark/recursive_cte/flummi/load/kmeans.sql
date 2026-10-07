CREATE TYPE point AS STRUCT(x REAL, y REAL);
CREATE TABLE points(x REAL, y REAL, PRIMARY KEY (x, y));

INSERT INTO points
SELECT ((i * 37) % 1000 + (i % 7) * 0.1)::REAL,
       ((i * 91 + (i * i) % 997) % 1000 + (i % 11) * 0.01)::REAL
FROM range(10000) points(i);
