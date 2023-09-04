import Base.Threads.@spawn

mutable struct QueryResult
    handle::Ref{duckdb_result}
    names::Vector{Symbol}
    types::Vector{Type}
    tbl::Union{Missing, NamedTuple}
    chunk_index::UInt64

    function QueryResult(handle::Ref{duckdb_result})
        column_count = duckdb_column_count(handle)
        names::Vector{Symbol} = Vector()
        for i in 1:column_count
            name = sym(duckdb_column_name(handle, i))
            if name in view(names, 1:(i - 1))
                j = 1
                new_name = Symbol(name, :_, j)
                while new_name in view(names, 1:(i - 1))
                    j += 1
                    new_name = Symbol(name, :_, j)
                end
                name = new_name
            end
            push!(names, name)
        end
        types::Vector{Type} = Vector()
        for i in 1:column_count
            logical_type = LogicalType(duckdb_column_logical_type(handle, i))
            push!(types, Union{Missing, duckdb_type_to_julia_type(logical_type)})
        end

        result = new(handle, names, types, missing, 1)
        finalizer(_close_result, result)
        return result
    end
end

function _close_result(result::QueryResult)
    duckdb_destroy_result(result.handle)
    return
end

mutable struct ColumnConversionData
    chunks::Vector{DataChunk}
    col_idx::Int64
    logical_type::LogicalType
    conversion_data::Any
end

mutable struct ListConversionData
    conversion_func::Function
    conversion_loop_func::Function
    child_type::LogicalType
    internal_type::Type
    target_type::Type
    child_conversion_data::Any
end

mutable struct StructConversionData
    tuple_type::Any
    child_conversion_data::Vector{ListConversionData}
end

nop_convert(column_data::ColumnConversionData, val) = val

function convert_string(column_data::ColumnConversionData, val::Ptr{Cvoid}, idx::UInt64)
    base_ptr = val + (idx - 1) * sizeof(duckdb_string_t)
    length_ptr = Base.unsafe_convert(Ptr{Int32}, base_ptr)
    length = unsafe_load(length_ptr)
    if length <= STRING_INLINE_LENGTH
        prefix_ptr = Base.unsafe_convert(Ptr{UInt8}, base_ptr + sizeof(Int32))
        return unsafe_string(prefix_ptr, length)
    else
        ptr_ptr = Base.unsafe_convert(Ptr{Ptr{UInt8}}, base_ptr + sizeof(Int32) * 2)
        data_ptr = Base.unsafe_load(ptr_ptr)
        return unsafe_string(data_ptr, length)
    end
end

function convert_blob(column_data::ColumnConversionData, val::Ptr{Cvoid}, idx::UInt64)::Base.CodeUnits{UInt8, String}
    return Base.codeunits(convert_string(column_data, val, idx))
end

function convert_date(column_data::ColumnConversionData, val::Int32)::Date
    return Dates.epochdays2date(val + ROUNDING_EPOCH_TO_UNIX_EPOCH_DAYS)
end

function convert_time(column_data::ColumnConversionData, val::Int64)::Time
    return Dates.Time(Dates.Nanosecond(val * 1000))
end

function convert_timestamp(column_data::ColumnConversionData, val::Int64)::DateTime
    return Dates.epochms2datetime((val ÷ 1000) + ROUNDING_EPOCH_TO_UNIX_EPOCH_MS)
end

function convert_timestamp_s(column_data::ColumnConversionData, val::Int64)::DateTime
    return Dates.epochms2datetime((val * 1000) + ROUNDING_EPOCH_TO_UNIX_EPOCH_MS)
end

function convert_timestamp_ms(column_data::ColumnConversionData, val::Int64)::DateTime
    return Dates.epochms2datetime((val) + ROUNDING_EPOCH_TO_UNIX_EPOCH_MS)
end

function convert_timestamp_ns(column_data::ColumnConversionData, val::Int64)::DateTime
    return Dates.epochms2datetime((val ÷ 1000000) + ROUNDING_EPOCH_TO_UNIX_EPOCH_MS)
end

function convert_interval(column_data::ColumnConversionData, val::duckdb_interval)::Dates.CompoundPeriod
    return Dates.CompoundPeriod(Dates.Month(val.months), Dates.Day(val.days), Dates.Microsecond(val.micros))
end

function convert_hugeint(column_data::ColumnConversionData, val::duckdb_hugeint)::Int128
    return Int128(val.lower) + Int128(val.upper) << 64
end

function convert_uuid(column_data::ColumnConversionData, val::duckdb_hugeint)::UUID
    hugeint = convert_hugeint(column_data, val)
    base_value = Int128(170141183460469231731687303715884105727)
    if hugeint < 0
        return UUID(UInt128(hugeint + base_value) + 1)
    else
        return UUID(UInt128(hugeint) + base_value + 1)
    end
end

function convert_enum(column_data::ColumnConversionData, val)::String
    return column_data.conversion_data[val + 1]
end

function convert_decimal_hugeint(column_data::ColumnConversionData, val::duckdb_hugeint)
    return Base.reinterpret(column_data.conversion_data, convert_hugeint(column_data, val))
end

function convert_decimal(column_data::ColumnConversionData, val)
    return Base.reinterpret(column_data.conversion_data, val)
end

function convert_vector(
    column_data::ColumnConversionData,
    vector::Vec,
    size::UInt64,
    convert_func::Function,
    result,
    position,
    all_valid,
    ::Type{SRC},
    ::Type{DST}
) where {SRC, DST}
    array = get_array(vector, SRC, size)
    if !all_valid
        validity = get_validity(vector, size)
    end
    for i in 1:size
        if all_valid || isvalid(validity, i)
            result[position] = convert_func(column_data, array[i])
        end
        position += 1
    end
    return size
end

function convert_vector_string(
    column_data::ColumnConversionData,
    vector::Vec,
    size::UInt64,
    convert_func::Function,
    result,
    position,
    all_valid,
    ::Type{SRC},
    ::Type{DST}
) where {SRC, DST}
    raw_ptr = duckdb_vector_get_data(vector.handle)
    ptr = Base.unsafe_convert(Ptr{duckdb_string_t}, raw_ptr)
    if !all_valid
        validity = get_validity(vector, size)
    end
    for i in 1:size
        if all_valid || isvalid(validity, i)
            result[position] = convert_func(column_data, raw_ptr, i)
        end
        position += 1
    end
    return size
end

function convert_vector_list(
    column_data::ColumnConversionData,
    vector::Vec,
    size::UInt64,
    convert_func::Function,
    result,
    position,
    all_valid,
    ::Type{SRC},
    ::Type{DST}
) where {SRC, DST}
    child_vector = list_child(vector)
    lsize = list_size(vector)

    # convert the child vector
    ldata = column_data.conversion_data

    child_column_data =
        ColumnConversionData(column_data.chunks, column_data.col_idx, ldata.child_type, ldata.child_conversion_data)
    child_array = Array{Union{Missing, ldata.target_type}}(missing, lsize)
    ldata.conversion_loop_func(
        child_column_data,
        child_vector,
        lsize,
        ldata.conversion_func,
        child_array,
        1,
        false,
        ldata.internal_type,
        ldata.target_type
    )

    array = get_array(vector, SRC, size)
    if !all_valid
        validity = get_validity(vector, size)
    end
    for i in 1:size
        if all_valid || isvalid(validity, i)
            start_offset::UInt64 = array[i].offset + 1
            end_offset::UInt64 = array[i].offset + array[i].length
            result[position] = child_array[start_offset:end_offset]
        end
        position += 1
    end
    return size
end

function convert_struct_children(column_data::ColumnConversionData, vector::Vec, size::UInt64)
    # convert the child vectors of the struct
    child_count = get_struct_child_count(column_data.logical_type)
    child_arrays = Vector()
    for i in 1:child_count
        child_vector = struct_child(vector, i)
        ldata = column_data.conversion_data.child_conversion_data[i]

        child_column_data =
            ColumnConversionData(column_data.chunks, column_data.col_idx, ldata.child_type, ldata.child_conversion_data)
        child_array = Array{Union{Missing, ldata.target_type}}(missing, size)
        ldata.conversion_loop_func(
            child_column_data,
            child_vector,
            size,
            ldata.conversion_func,
            child_array,
            1,
            false,
            ldata.internal_type,
            ldata.target_type
        )
        push!(child_arrays, child_array)
    end
    return child_arrays
end


function convert_vector_struct(
    column_data::ColumnConversionData,
    vector::Vec,
    size::UInt64,
    convert_func::Function,
    result,
    position,
    all_valid,
    ::Type{SRC},
    ::Type{DST}
) where {SRC, DST}
    child_count = get_struct_child_count(column_data.logical_type)
    child_arrays = convert_struct_children(column_data, vector, size)

    if !all_valid
        validity = get_validity(vector, size)
    end
    for i in 1:size
        if all_valid || isvalid(validity, i)
            result_tuple = Vector()
            for child_idx in 1:child_count
                push!(result_tuple, child_arrays[child_idx][i])
            end
            result[position] = NamedTuple{column_data.conversion_data.tuple_type}(result_tuple)
        end
        position += 1
    end
    return size
end

function convert_vector_union(
    column_data::ColumnConversionData,
    vector::Vec,
    size::UInt64,
    convert_func::Function,
    result,
    position,
    all_valid,
    ::Type{SRC},
    ::Type{DST}
) where {SRC, DST}
    child_arrays = convert_struct_children(column_data, vector, size)

    if !all_valid
        validity = get_validity(vector, size)
    end
    for row in 1:size
        # For every row/record
        if all_valid || isvalid(validity, row)
            # Get the tag of this row
            tag::UInt64 = child_arrays[1][row]
            type::DataType = duckdb_type_to_julia_type(get_union_member_type(column_data.logical_type, tag + 1))
            # Get the value from the child array indicated by the tag
            # Offset by 1 because of julia
            # Offset by another 1 because of the tag vector
            value = child_arrays[tag + 2][row]
            result[position] = isequal(value, missing) ? missing : type(value)
        end
        position += 1
    end
    return size
end

function convert_vector_map(
    column_data::ColumnConversionData,
    vector::Vec,
    size::UInt64,
    convert_func::Function,
    result,
    position,
    all_valid,
    ::Type{SRC},
    ::Type{DST}
) where {SRC, DST}
    child_vector = list_child(vector)
    lsize = list_size(vector)

    # convert the child vector
    ldata = column_data.conversion_data

    child_column_data =
        ColumnConversionData(column_data.chunks, column_data.col_idx, ldata.child_type, ldata.child_conversion_data)
    child_array = Array{Union{Missing, ldata.target_type}}(missing, lsize)
    ldata.conversion_loop_func(
        child_column_data,
        child_vector,
        lsize,
        ldata.conversion_func,
        child_array,
        1,
        false,
        ldata.internal_type,
        ldata.target_type
    )
    child_arrays = convert_struct_children(child_column_data, child_vector, lsize)
    keys = child_arrays[1]
    values = child_arrays[2]

    array = get_array(vector, SRC, size)
    if !all_valid
        validity = get_validity(vector, size)
    end
    for i in 1:size
        if all_valid || isvalid(validity, i)
            result_dict = Dict()
            start_offset::UInt64 = array[i].offset + 1
            end_offset::UInt64 = array[i].offset + array[i].length
            for key_idx in start_offset:end_offset
                result_dict[keys[key_idx]] = values[key_idx]
            end
            result[position] = result_dict
        end
        position += 1
    end
    return size
end

function convert_column_loop(
    column_data::ColumnConversionData,
    convert_func::Function,
    ::Type{SRC},
    ::Type{DST},
    convert_vector_func::Function
) where {SRC, DST}
    # first check if there are null values in any chunks
    has_missing = false
    row_count = 0
    for chunk in column_data.chunks
        if !all_valid(chunk, column_data.col_idx)
            has_missing = true
        end
        row_count += get_size(chunk)
    end
    if has_missing
        # missing values
        result = Array{Union{Missing, DST}}(missing, row_count)
        position = 1
        for chunk in column_data.chunks
            position += convert_vector_func(
                column_data,
                get_vector(chunk, column_data.col_idx),
                get_size(chunk),
                convert_func,
                result,
                position,
                all_valid(chunk, column_data.col_idx),
                SRC,
                DST
            )
        end
    else
        # no missing values
        result = Array{DST}(undef, row_count)
        position = 1
        for chunk in column_data.chunks
            position += convert_vector_func(
                column_data,
                get_vector(chunk, column_data.col_idx),
                get_size(chunk),
                convert_func,
                result,
                position,
                true,
                SRC,
                DST
            )
        end
    end
    return result
end

function create_child_conversion_data(child_type::LogicalType)
    internal_type_id = get_internal_type_id(child_type)
    internal_type = duckdb_type_to_internal_type(internal_type_id)
    target_type = duckdb_type_to_julia_type(child_type)

    conversion_func = get_conversion_function(child_type)
    conversion_loop_func = get_conversion_loop_function(child_type)
    child_conversion_data = init_conversion_loop(child_type)
    return ListConversionData(
        conversion_func,
        conversion_loop_func,
        child_type,
        internal_type,
        target_type,
        child_conversion_data
    )
end

function init_conversion_loop(logical_type::LogicalType)
    type = get_type_id(logical_type)
    if type == DUCKDB_TYPE_DECIMAL
        return duckdb_type_to_julia_type(logical_type)
    elseif type == DUCKDB_TYPE_ENUM
        return get_enum_dictionary(logical_type)
    elseif type == DUCKDB_TYPE_LIST || type == DUCKDB_TYPE_MAP
        child_type = get_list_child_type(logical_type)
        return create_child_conversion_data(child_type)
    elseif type == DUCKDB_TYPE_STRUCT || type == DUCKDB_TYPE_UNION
        child_count_fun::Function = get_struct_child_count
        child_type_fun::Function = get_struct_child_type
        child_name_fun::Function = get_struct_child_name

        #if type == DUCKDB_TYPE_UNION
        #	child_count_fun = get_union_member_count
        #	child_type_fun = get_union_member_type
        #	child_name_fun = get_union_member_name
        #end

        child_count = child_count_fun(logical_type)
        child_symbols::Vector{Symbol} = Vector()
        child_data::Vector{ListConversionData} = Vector()
        for i in 1:child_count
            child_symbol = Symbol(child_name_fun(logical_type, i))
            child_type = child_type_fun(logical_type, i)
            child_conv_data = create_child_conversion_data(child_type)
            push!(child_symbols, child_symbol)
            push!(child_data, child_conv_data)
        end
        return StructConversionData(Tuple(x for x in child_symbols), child_data)
    else
        return nothing
    end
end

function get_conversion_function(logical_type::LogicalType)::Function
    type = get_type_id(logical_type)
    if type == DUCKDB_TYPE_VARCHAR
        return convert_string
    elseif type == DUCKDB_TYPE_BLOB || type == DUCKDB_TYPE_BIT
        return convert_blob
    elseif type == DUCKDB_TYPE_DATE
        return convert_date
    elseif type == DUCKDB_TYPE_TIME
        return convert_time
    elseif type == DUCKDB_TYPE_TIMESTAMP
        return convert_timestamp
    elseif type == DUCKDB_TYPE_TIMESTAMP_S
        return convert_timestamp_s
    elseif type == DUCKDB_TYPE_TIMESTAMP_MS
        return convert_timestamp_ms
    elseif type == DUCKDB_TYPE_TIMESTAMP_NS
        return convert_timestamp_ns
    elseif type == DUCKDB_TYPE_INTERVAL
        return convert_interval
    elseif type == DUCKDB_TYPE_HUGEINT
        return convert_hugeint
    elseif type == DUCKDB_TYPE_UUID
        return convert_uuid
    elseif type == DUCKDB_TYPE_DECIMAL
        internal_type_id = get_internal_type_id(logical_type)
        if internal_type_id == DUCKDB_TYPE_HUGEINT
            return convert_decimal_hugeint
        else
            return convert_decimal
        end
    elseif type == DUCKDB_TYPE_ENUM
        return convert_enum
    else
        return nop_convert
    end
end

function get_conversion_loop_function(logical_type::LogicalType)::Function
    type = get_type_id(logical_type)
    if type == DUCKDB_TYPE_VARCHAR || type == DUCKDB_TYPE_BLOB || type == DUCKDB_TYPE_BIT
        return convert_vector_string
    elseif type == DUCKDB_TYPE_LIST
        return convert_vector_list
    elseif type == DUCKDB_TYPE_STRUCT
        return convert_vector_struct
    elseif type == DUCKDB_TYPE_MAP
        return convert_vector_map
    elseif type == DUCKDB_TYPE_UNION
        return convert_vector_union
    else
        return convert_vector
    end
end

function convert_column(column_data::ColumnConversionData)
    internal_type_id = get_internal_type_id(column_data.logical_type)
    internal_type = duckdb_type_to_internal_type(internal_type_id)
    target_type = duckdb_type_to_julia_type(column_data.logical_type)

    conversion_func = get_conversion_function(column_data.logical_type)
    conversion_loop_func = get_conversion_loop_function(column_data.logical_type)

    column_data.conversion_data = init_conversion_loop(column_data.logical_type)
    return convert_column_loop(column_data, conversion_func, internal_type, target_type, conversion_loop_func)
end

function Tables.columns(q::QueryResult)
    if q.tbl === missing
        if q.chunk_index != 1
            throw(
                NotImplementedException(
                    "Materializing into a Julia table is not supported after calling nextDataChunk"
                )
            )
        end
        # gather all the data chunks
        column_count = duckdb_column_count(q.handle)
        chunks::Vector{DataChunk} = []
        while true
            # fetch the next chunk
            chunk = DuckDB.nextDataChunk(q)
            if chunk === missing
                # consumed all chunks
                break
            end
            push!(chunks, chunk)
        end

        q.tbl = NamedTuple{Tuple(q.names)}(ntuple(column_count) do i
            logical_type = LogicalType(duckdb_column_logical_type(q.handle, i))
            column_data = ColumnConversionData(chunks, i, logical_type, nothing)
            return convert_column(column_data)
        end)
    end
    return Tables.CopiedColumns(q.tbl)
end

mutable struct PendingQueryResult
    handle::duckdb_pending_result
    success::Bool

    function PendingQueryResult(stmt::Stmt)
        pending_handle = Ref{duckdb_pending_result}()
        ret = executePending(stmt.handle, pending_handle, stmt.result_type)
        result = new(pending_handle[], ret == DuckDBSuccess)
        finalizer(_close_pending_result, result)
        return result
    end
end

function executePending(
    handle::duckdb_prepared_statement,
    pending_handle::Ref{duckdb_pending_result},
    ::Type{MaterializedResult}
)
    return duckdb_pending_prepared(handle, pending_handle)
end

function executePending(
    handle::duckdb_prepared_statement,
    pending_handle::Ref{duckdb_pending_result},
    ::Type{StreamResult}
)
    return duckdb_pending_prepared_streaming(handle, pending_handle)
end

function _close_pending_result(pending::PendingQueryResult)
    if pending.handle == C_NULL
        return
    end
    duckdb_destroy_pending(pending.handle)
    pending.handle = C_NULL
    return
end

function fetch_error(stmt::Stmt, error_ptr)
    if error_ptr == C_NULL
        return string("Execute of query \"", stmt.sql, "\" failed: unknown error")
    else
        return string("Execute of query \"", stmt.sql, "\" failed: ", unsafe_string(error_ptr))
    end
end

function get_error(stmt::Stmt, pending::PendingQueryResult)
    error_ptr = duckdb_pending_error(pending.handle)
    error_message = fetch_error(stmt, error_ptr)
    _close_pending_result(pending)
    return error_message
end

# execute tasks from a pending query result in a loop
function pending_execute_tasks(pending::PendingQueryResult)::Bool
    ret = DUCKDB_PENDING_RESULT_NOT_READY
    while !duckdb_pending_execution_is_finished(ret)
        GC.safepoint()
        ret = duckdb_pending_execute_task(pending.handle)
    end
    return ret != DUCKDB_PENDING_ERROR
end

# execute background tasks in a loop, until task execution is finished
function execute_tasks(state::duckdb_task_state, con::Connection)
    while !duckdb_task_state_is_finished(state)
        duckdb_execute_n_tasks_state(state, 1)
        GC.safepoint()
        Base.yield()
        if duckdb_execution_is_finished(con.handle)
            break
        end
    end
    return
end

# cleanup background tasks
function cleanup_tasks(tasks, state)
    # mark execution as finished so the individual tasks will quit
    duckdb_finish_execution(state)
    # now wait for all tasks to finish executing
    exceptions = []
    for task in tasks
        try
            Base.wait(task)
        catch ex
            push!(exceptions, ex)
        end
    end
    # clean up the tasks and task state
    empty!(tasks)
    duckdb_destroy_task_state(state)

    # if any tasks threw, propagate the error upwards by throwing as well
    for ex in exceptions
        throw(ex)
    end
    return
end

function execute_singlethreaded(pending::PendingQueryResult)::Bool
    # Only when there are no additional threads, use the main thread to execute
    success = true
    try
        # now start executing tasks of the pending result in a loop
        success = pending_execute_tasks(pending)
    catch ex
        throw(ex)
    end
    return success
end

function execute_multithreaded(stmt::Stmt)
    # if multi-threading is enabled, launch background tasks
    task_state = duckdb_create_task_state(stmt.con.db.handle)

    tasks = []
    for _ in 1:Threads.nthreads()
        task_val = @spawn execute_tasks(task_state, stmt.con)
        push!(tasks, task_val)
    end

    # When we have additional worker threads, don't execute using the main thread
    while duckdb_execution_is_finished(stmt.con.handle) == false
        Base.yield()
        GC.safepoint()
    end

    # we finished execution of all tasks, cleanup the tasks
    return cleanup_tasks(tasks, task_state)
end

# this function is responsible for executing a statement and returning a result
function execute(stmt::Stmt, params::DBInterface.StatementParams = ())
    bind_parameters(stmt, params)

    # first create a pending query result
    pending = PendingQueryResult(stmt)
    if !pending.success
        throw(QueryException(get_error(stmt, pending)))
    end

    success = true
    if Threads.nthreads() == 1
        success = execute_singlethreaded(pending)
        # check if an error was thrown
        if !success
            throw(QueryException(get_error(stmt, pending)))
        end
    else
        execute_multithreaded(stmt)
    end

    handle = Ref{duckdb_result}()
    ret = duckdb_execute_pending(pending.handle, handle)
    if ret != DuckDBSuccess
        error_ptr = duckdb_result_error(handle)
        error_message = fetch_error(stmt, error_ptr)
        duckdb_destroy_result(handle)
        throw(QueryException(error_message))
    end
    return QueryResult(handle)
end

# explicitly close prepared statement
DBInterface.close!(stmt::Stmt) = _close_stmt(stmt)

function execute(con::Connection, sql::AbstractString, params::DBInterface.StatementParams)
    stmt = Stmt(con, sql, MaterializedResult)
    try
        return execute(stmt, params)
    finally
        _close_stmt(stmt) # immediately close, don't wait for GC
    end
end

execute(con::Connection, sql::AbstractString; kwargs...) = execute(con, sql, values(kwargs))
execute(db::DB, sql::AbstractString, params::DBInterface.StatementParams) = execute(db.main_connection, sql, params)
execute(db::DB, sql::AbstractString; kwargs...) = execute(db.main_connection, sql, values(kwargs))


Tables.istable(::Type{QueryResult}) = true
Tables.isrowtable(::Type{QueryResult}) = true
Tables.columnaccess(::Type{QueryResult}) = true
Tables.schema(q::QueryResult) = Tables.Schema(q.names, q.types)
Base.IteratorSize(::Type{QueryResult}) = Base.SizeUnknown()
Base.eltype(q::QueryResult) = Any

DBInterface.close!(q::QueryResult) = _close_result(q)

Base.iterate(q::QueryResult) = iterate(Tables.rows(Tables.columns(q)))
Base.iterate(q::QueryResult, state) = iterate(Tables.rows(Tables.columns(q)), state)

function nextDataChunk(q::QueryResult)::Union{Missing, DataChunk}
    if duckdb_result_is_streaming(q.handle[])
        chunk_handle = duckdb_stream_fetch_chunk(q.handle[])
        if chunk_handle == C_NULL
            return missing
        end
        chunk = DataChunk(chunk_handle, true)
        if get_size(chunk) == 0
            return missing
        end
        return chunk
    else
        chunk_count = duckdb_result_chunk_count(q.handle[])
        if q.chunk_index > chunk_count
            return missing
        end
        chunk = DataChunk(duckdb_result_get_chunk(q.handle[], q.chunk_index), true)
    end
    q.chunk_index += 1
    return chunk
end

"Return the last row insert id from the executed statement"
DBInterface.lastrowid(con::Connection) = throw(NotImplementedException("Unimplemented: lastrowid"))
DBInterface.lastrowid(db::DB) = DBInterface.lastrowid(db.main_connection)

"""
    DBInterface.prepare(db::DuckDB.DB, sql::AbstractString)

Prepare an SQL statement given as a string in the DuckDB database; returns a `DuckDB.Stmt` object.
See `DBInterface.execute`(@ref) for information on executing a prepared statement and passing parameters to bind.
A `DuckDB.Stmt` object can be closed (resources freed) using `DBInterface.close!`(@ref).
"""
DBInterface.prepare(con::Connection, sql::AbstractString, result_type::Type) = Stmt(con, sql, result_type)
DBInterface.prepare(con::Connection, sql::AbstractString) = DBInterface.prepare(con, sql, MaterializedResult)
DBInterface.prepare(db::DB, sql::AbstractString) = DBInterface.prepare(db.main_connection, sql)
DBInterface.prepare(db::DB, sql::AbstractString, result_type::Type) =
    DBInterface.prepare(db.main_connection, sql, result_type)

"""
    DBInterface.execute(db::DuckDB.DB, sql::String, [params])
    DBInterface.execute(stmt::SQLite.Stmt, [params])

Bind any positional (`params` as `Vector` or `Tuple`) or named (`params` as `NamedTuple` or `Dict`) parameters to an SQL statement, given by `db` and `sql` or
as an already prepared statement `stmt`, execute the query and return an iterator of result rows.

Note that the returned result row iterator only supports a single-pass, forward-only iteration of the result rows.
Calling `SQLite.reset!(result)` will re-execute the query and reset the iterator back to the beginning.

The resultset iterator supports the [Tables.jl](https://github.com/JuliaData/Tables.jl) interface, so results can be collected in any Tables.jl-compatible sink,
like `DataFrame(results)`, `CSV.write("results.csv", results)`, etc.
"""
DBInterface.execute(stmt::Stmt, params::DBInterface.StatementParams) = execute(stmt, params)
DBInterface.execute(con::Connection, sql::AbstractString, result_type::Type) = execute(Stmt(con, sql, result_type))
DBInterface.execute(con::Connection, sql::AbstractString) = DBInterface.execute(con, sql, MaterializedResult)
DBInterface.execute(db::DB, sql::AbstractString, result_type::Type) =
    DBInterface.execute(db.main_connection, sql, result_type)

Base.show(io::IO, result::DuckDB.QueryResult) = print(io, Tables.columntable(result))
