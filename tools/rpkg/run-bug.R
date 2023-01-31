rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4)))
partitions <- list(duckdb:::expr_reference("b"))
sum <- duckdb:::expr_reference("a")
window_function <- duckdb:::rapi_rel_window_aggregation(rel_a, sum, partitions, list())
