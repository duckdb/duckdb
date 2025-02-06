
@testset "C API Type Checks" begin

    # Check struct sizes.
    # Timestamp struct size mismatch, eventually structs are stored as pointers. This happens if they are declared as mutable structs.
    @test sizeof(DuckDB.duckdb_timestamp_struct) ==
          sizeof(DuckDB.duckdb_date_struct) + sizeof(DuckDB.duckdb_time_struct)

    # Bot structs are equivalent and actually stored as a Union type in C.
    @test sizeof(DuckDB.duckdb_string_t) == sizeof(DuckDB.duckdb_string_t_ptr)

end
