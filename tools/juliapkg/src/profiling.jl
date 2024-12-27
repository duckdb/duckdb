


function profile_query(con::Connection, query::String)
    stmt = prepare(con, query)
    result = execute(stmt)


    profile_ptr = duckdb_get_profiling_info(con.handle)
    if profile_ptr == C_NULL
        return nothing
    end

    N = duckdb_profiling_info_get_child_count(profile_ptr)
    for i in 1:N
        child_ptr = duckdb_profiling_info_get_child(profile_ptr, i)
        metrics_map = duckdb_profiling_info_get_metrics(child_ptr)

        M = duckdb_get_map_size(metrics_map)


        metrics = Dict{String, Int64}()

    end

    duckdb_profiling_info_get_child
    return result
end
