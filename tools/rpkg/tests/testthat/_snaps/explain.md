# EXPLAIN gives reasonable output

    Code
      DBI::dbGetQuery(con, "EXPLAIN SELECT 1;")
    Output
      physical_plan
      ┌───────────────────────────┐
      │         PROJECTION        │
      │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
      │             1             │
      └─────────────┬─────────────┘                             
      ┌─────────────┴─────────────┐
      │         DUMMY_SCAN        │
      └───────────────────────────┘                             

# EXPLAIN shows logical, optimized and physical plan

    Code
      DBI::dbExecute(con, "PRAGMA explain_output='all';")
    Output
      [1] 0
    Code
      DBI::dbGetQuery(con, "EXPLAIN SELECT 1;")
    Output
      logical_plan
      ┌───────────────────────────┐
      │         PROJECTION        │
      │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
      │             1             │
      └─────────────┬─────────────┘                             
      ┌─────────────┴─────────────┐
      │         DUMMY_SCAN        │
      └───────────────────────────┘                             
      logical_opt
      ┌───────────────────────────┐
      │         PROJECTION        │
      │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
      │             1             │
      └─────────────┬─────────────┘                             
      ┌─────────────┴─────────────┐
      │         DUMMY_SCAN        │
      └───────────────────────────┘                             
      physical_plan
      ┌───────────────────────────┐
      │         PROJECTION        │
      │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
      │             1             │
      └─────────────┬─────────────┘                             
      ┌─────────────┴─────────────┐
      │         DUMMY_SCAN        │
      └───────────────────────────┘                             

# EXPLAIN ANALYZE outputs query tree

    Code
      DBI::dbGetQuery(con, "EXPLAIN ANALYZE SELECT 1;")
    Output
      analyzed_plan
      ┌─────────────────────────────────────┐
      │┌───────────────────────────────────┐│
      ││    Query Profiling Information    ││
      │└───────────────────────────────────┘│
      └─────────────────────────────────────┘
      EXPLAIN ANALYZE SELECT 1;
      ┌─────────────────────────────────────┐
      │┌───────────────────────────────────┐│
      ││        Total Time: 0.0001s        ││
      │└───────────────────────────────────┘│
      └─────────────────────────────────────┘
      ┌───────────────────────────┐
      │      EXPLAIN_ANALYZE      │
      │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
      │             0             │
      │          (0.00s)          │
      └─────────────┬─────────────┘                             
      ┌─────────────┴─────────────┐
      │         PROJECTION        │
      │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
      │             1             │
      │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
      │             1             │
      │          (0.00s)          │
      └─────────────┬─────────────┘                             
      ┌─────────────┴─────────────┐
      │         DUMMY_SCAN        │
      │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
      │             1             │
      │          (0.00s)          │
      └───────────────────────────┘                             

# wrong type of input forwards handling to the next method

    Code
      rs <- DBI::dbGetQuery(con, "SELECT 1;")
      class(rs) <- c("duckdb_explain", class(rs))
      rs
    Output
        1
      1 1
      

