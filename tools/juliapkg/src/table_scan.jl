struct TableBindInfo
    tbl::Any
    input_columns::Vector
    scan_types::Vector{Type}
    result_types::Vector{Type}
    scan_functions::Vector{Function}

    function TableBindInfo(
        tbl,
        input_columns::Vector,
        scan_types::Vector{Type},
        result_types::Vector{Type},
        scan_functions::Vector{Function}
    )
        return new(tbl, input_columns, scan_types, result_types, scan_functions)
    end
end

table_result_type(tbl, entry) = Core.Compiler.typesubtract(eltype(tbl[entry]), Missing, 1)

julia_to_duck_type(::Type{Date}) = Int32
julia_to_duck_type(::Type{Time}) = Int64
julia_to_duck_type(::Type{DateTime}) = Int64
julia_to_duck_type(::Type{T}) where {T} = T

value_to_duckdb(val::Date) = convert(Int32, Dates.date2epochdays(val) - ROUNDING_EPOCH_TO_UNIX_EPOCH_DAYS)
value_to_duckdb(val::Time) = convert(Int64, Dates.value(val) / 1000)
value_to_duckdb(val::DateTime) = convert(Int64, (Dates.datetime2epochms(val) - ROUNDING_EPOCH_TO_UNIX_EPOCH_MS) * 1000)
value_to_duckdb(val::AbstractString) = throw(
    NotImplementedException(
        "Cannot use value_to_duckdb to convert string values - use DuckDB.assign_string_element on a vector instead"
    )
)
value_to_duckdb(val) = val

function tbl_scan_column(
    input_column::AbstractVector{JL_TYPE},
    row_offset::Int64,
    col_idx::Int64,
    result_idx::Int64,
    scan_count::Int64,
    output::DuckDB.DataChunk,
    ::Type{DUCK_TYPE},
    ::Type{JL_TYPE}
) where {DUCK_TYPE, JL_TYPE}
    vector::Vec = DuckDB.get_vector(output, result_idx)
    result_array::Vector{DUCK_TYPE} = DuckDB.get_array(vector, DUCK_TYPE)
    validity::ValidityMask = DuckDB.get_validity(vector)
    for i::Int64 in 1:scan_count
        val = getindex(input_column, row_offset + i)
        if val === missing
            DuckDB.setinvalid(validity, i)
        else
            result_array[i] = value_to_duckdb(val)
        end
    end
end

function tbl_scan_string_column(
    input_column::AbstractVector{JL_TYPE},
    row_offset::Int64,
    col_idx::Int64,
    result_idx::Int64,
    scan_count::Int64,
    output::DuckDB.DataChunk,
    ::Type{DUCK_TYPE},
    ::Type{JL_TYPE}
) where {DUCK_TYPE, JL_TYPE}
    vector::Vec = DuckDB.get_vector(output, result_idx)
    validity::ValidityMask = DuckDB.get_validity(vector)
    for i::Int64 in 1:scan_count
        val = getindex(input_column, row_offset + i)
        if val === missing
            DuckDB.setinvalid(validity, i)
        else
            DuckDB.assign_string_element(vector, i, val)
        end
    end
end

function tbl_scan_function(tbl, entry)
    result_type = table_result_type(tbl, entry)
    if result_type <: AbstractString
        return tbl_scan_string_column
    end
    return tbl_scan_column
end

function tbl_bind_function(info::DuckDB.BindInfo)
    # fetch the tbl name from the function parameters
    parameter = DuckDB.get_parameter(info, 0)
    name = DuckDB.getvalue(parameter, String)
    # fetch the actual tbl using the function name
    extra_data = DuckDB.get_extra_data(info)
    tbl = extra_data[name]

    # set the cardinality
    row_count::UInt64 = Tables.rowcount(tbl)
    DuckDB.set_stats_cardinality(info, row_count, true)

    # register the result columns
    input_columns = Vector()
    scan_types::Vector{Type} = Vector()
    result_types::Vector{Type} = Vector()
    scan_functions::Vector{Function} = Vector()
    for entry in Tables.columnnames(tbl)
        result_type = table_result_type(tbl, entry)
        scan_function = tbl_scan_function(tbl, entry)
        push!(input_columns, tbl[entry])
        push!(scan_types, eltype(tbl[entry]))
        push!(result_types, julia_to_duck_type(result_type))
        push!(scan_functions, scan_function)

        DuckDB.add_result_column(info, string(entry), result_type)
    end
    return TableBindInfo(tbl, input_columns, scan_types, result_types, scan_functions)
end

mutable struct TableGlobalInfo
    pos::Int64
    global_lock::ReentrantLock

    function TableGlobalInfo()
        return new(0, ReentrantLock())
    end
end

mutable struct TableLocalInfo
    columns::Vector{Int64}
    current_pos::Int64
    end_pos::Int64

    function TableLocalInfo(columns)
        return new(columns, 0, 0)
    end
end

function tbl_global_init_function(info::DuckDB.InitInfo)
    bind_info = DuckDB.get_bind_info(info, TableBindInfo)
    # figure out the maximum number of threads to launch from the tbl size
    row_count::Int64 = Tables.rowcount(bind_info.tbl)
    max_threads::Int64 = ceil(row_count / DuckDB.ROW_GROUP_SIZE)
    DuckDB.set_max_threads(info, max_threads)
    return TableGlobalInfo()
end

function tbl_local_init_function(info::DuckDB.InitInfo)
    columns = DuckDB.get_projected_columns(info)
    return TableLocalInfo(columns)
end

function tbl_scan_function(info::DuckDB.FunctionInfo, output::DuckDB.DataChunk)
    bind_info = DuckDB.get_bind_info(info, TableBindInfo)
    global_info = DuckDB.get_init_info(info, TableGlobalInfo)
    local_info = DuckDB.get_local_info(info, TableLocalInfo)

    if local_info.current_pos >= local_info.end_pos
        # ran out of data to scan in the local info: fetch new rows from the global state (if any)
        # we can in increments of 100 vectors
        lock(global_info.global_lock) do
            row_count::Int64 = Tables.rowcount(bind_info.tbl)
            local_info.current_pos = global_info.pos
            total_scan_amount::Int64 = DuckDB.ROW_GROUP_SIZE
            if local_info.current_pos + total_scan_amount >= row_count
                total_scan_amount = row_count - local_info.current_pos
            end
            local_info.end_pos = local_info.current_pos + total_scan_amount
            return global_info.pos += total_scan_amount
        end
    end
    scan_count::Int64 = DuckDB.VECTOR_SIZE
    current_row::Int64 = local_info.current_pos
    if current_row + scan_count >= local_info.end_pos
        scan_count = local_info.end_pos - current_row
    end
    local_info.current_pos += scan_count

    result_idx::Int64 = 1
    for col_idx::Int64 in local_info.columns
        if col_idx == 0
            result_idx += 1
            continue
        end
        bind_info.scan_functions[col_idx](
            bind_info.input_columns[col_idx],
            current_row,
            col_idx,
            result_idx,
            scan_count,
            output,
            bind_info.result_types[col_idx],
            bind_info.scan_types[col_idx]
        )
        result_idx += 1
    end
    DuckDB.set_size(output, scan_count)
    return
end

function register_table(con::Connection, tbl, name::AbstractString)
    con.db.registered_objects[name] = columntable(tbl)
    DBInterface.execute(
        con,
        string("CREATE OR REPLACE VIEW \"", name, "\" AS SELECT * FROM julia_tbl_scan('", name, "')")
    )
    return
end
register_table(db::DB, tbl, name::AbstractString) = register_table(db.main_connection, tbl, name)

function unregister_table(con::Connection, name::AbstractString)
    pop!(con.db.registered_objects, name)
    DBInterface.execute(con, string("DROP VIEW IF EXISTS \"", name, "\""))
    return
end
unregister_table(db::DB, name::AbstractString) = unregister_table(db.main_connection, name)

# for backwards compatibility:
const register_data_frame = register_table
const unregister_data_frame = unregister_table


function _add_table_scan(db::DB)
    # add the table scan function
    DuckDB.create_table_function(
        db.main_connection,
        "julia_tbl_scan",
        [String],
        tbl_bind_function,
        tbl_global_init_function,
        tbl_scan_function,
        db.handle.registered_objects,
        true,
        tbl_local_init_function
    )
    return
end
