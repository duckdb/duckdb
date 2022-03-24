using DataFrames

mutable struct DFBindInfo
    df::DataFrame
    result_types::Vector{Type}
    scan_functions::Vector{Function}

    function DFBindInfo(df::DataFrame, result_types::Vector{Type}, scan_functions::Vector{Function})
        return new(df, result_types, scan_functions)
    end
end

function df_result_type(df, entry)
    column_type = eltype(df[!, entry])
    if typeof(column_type) == Union
        # remove Missing type from the union
        column_type = Core.Compiler.typesubtract(column_type, Missing, 1)
    end
    return column_type
end

function df_julia_type(column_type)
    if column_type == Date
        column_type = Int32
    elseif column_type == Time
        column_type = Int64
    elseif column_type == DateTime
        column_type = Int64
    end
    return column_type
end

value_to_duckdb(val::T) where {T <: Date} =
    convert(Int32, Dates.date2epochdays(val) - ROUNDING_EPOCH_TO_UNIX_EPOCH_DAYS)
value_to_duckdb(val::T) where {T <: Time} = convert(Int64, Dates.value(val) / 1000)
value_to_duckdb(val::T) where {T <: DateTime} =
    convert(Int64, (Dates.datetime2epochms(val) - ROUNDING_EPOCH_TO_UNIX_EPOCH_MS) * 1000)
function value_to_duckdb(val::T) where {T <: AbstractString}
    throw(
        NotImplementedException(
            "Cannot use value_to_duckdb to convert string values - use DuckDB.assign_string_element on a vector instead"
        )
    )
end
function value_to_duckdb(val::T) where {T}
    return val
end

function df_scan_column(
    df::DataFrame,
    df_offset::Int64,
    col_idx::Int64,
    scan_count::Int64,
    output::DuckDB.DataChunk,
    ::Type{T}
) where {T}
    vector = DuckDB.get_vector(output, col_idx)
    result_array::Vector{T} = DuckDB.get_array(vector, T)
    validity = DuckDB.get_validity(vector)
    input_column = df[!, col_idx]
    for i in 1:scan_count
        if input_column[df_offset + i] === missing
            DuckDB.setinvalid(validity, i)
        else
            result_array[i] = value_to_duckdb(input_column[df_offset + i])
        end
    end
end

function df_scan_string_column(
    df::DataFrame,
    df_offset::Int64,
    col_idx::Int64,
    scan_count::Int64,
    output::DuckDB.DataChunk,
    ::Type{T}
) where {T}
    vector = DuckDB.get_vector(output, col_idx)
    validity = DuckDB.get_validity(vector)
    input_column = df[!, col_idx]
    for i in 1:scan_count
        if input_column[df_offset + i] === missing
            DuckDB.setinvalid(validity, i)
        else
            DuckDB.assign_string_element(vector, i, input_column[df_offset + i])
        end
    end
end

function df_scan_function(df, entry)
    result_type = df_result_type(df, entry)
    if result_type <: AbstractString
        return df_scan_string_column
    end
    return df_scan_column
end

function df_bind_function(info::DuckDB.BindInfo)
    # fetch the df name from the function parameters
    parameter = DuckDB.get_parameter(info, 0)
    name = DuckDB.getvalue(parameter, String)
    # fetch the actual df using the function name
    extra_data = DuckDB.get_extra_data(info)
    df = extra_data[name]

    # register the result columns
    result_types::Vector{Type} = Vector()
    scan_functions::Vector{Function} = Vector()
    for entry in names(df)
        result_type = df_result_type(df, entry)
        scan_function = df_scan_function(df, entry)
        push!(result_types, df_julia_type(result_type))
        push!(scan_functions, scan_function)

        DuckDB.add_result_column(info, entry, result_type)
    end
    return DFBindInfo(df, result_types, scan_functions)
end

mutable struct DFInitInfo
    pos::Int64

    function DFInitInfo()
        return new(0)
    end
end

function df_init_function(info::DuckDB.InitInfo)
    return DFInitInfo()
end

function df_scan_function(info::DuckDB.FunctionInfo, output::DuckDB.DataChunk)
    bind_info = DuckDB.get_bind_info(info, DFBindInfo)
    init_info = DuckDB.get_init_info(info, DFInitInfo)

    column_count = size(names(bind_info.df), 1)
    row_count = size(bind_info.df, 1)
    scan_count = DuckDB.VECTOR_SIZE
    if init_info.pos + scan_count >= row_count
        scan_count = row_count - init_info.pos
    end

    for col_idx in 1:column_count
        bind_info.scan_functions[col_idx](
            bind_info.df,
            init_info.pos,
            col_idx,
            scan_count,
            output,
            bind_info.result_types[col_idx]
        )
    end
    init_info.pos += scan_count
    DuckDB.set_size(output, scan_count)
    return
end

function register_data_frame(con::Connection, df::DataFrame, name::AbstractString)
    con.db.registered_objects[name] = df
    DBInterface.execute(
        con,
        string("CREATE OR REPLACE VIEW \"", name, "\" AS SELECT * FROM julia_df_scan('", name, "')")
    )
    return
end
register_data_frame(db::DB, df::DataFrame, name::AbstractString) = register_data_frame(db.main_connection, df, name)

function unregister_data_frame(con::Connection, name::AbstractString)
    pop!(con.db.registered_objects, name)
    DBInterface.execute(con, string("DROP VIEW IF EXISTS \"", name, "\""))
    return
end
unregister_data_frame(db::DB, name::AbstractString) = unregister_data_frame(db.main_connection, name)


function _add_data_frame_scan(db::DB)
    # add the data frame scan function
    DuckDB.create_table_function(
        db.main_connection,
        "julia_df_scan",
        [String],
        df_bind_function,
        df_init_function,
        df_scan_function,
        db.handle.registered_objects
    )
    return
end
