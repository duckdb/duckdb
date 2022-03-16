using DataFrames

mutable struct DFBindInfo
    df::DataFrame
    result_types::Vector{Type}
    scan_functions::Vector{Function}

    function DFBindInfo(df::DataFrame, result_types::Vector{Type}, scan_functions::Vector{Function})
        return new(df, result_types, scan_functions)
    end
end

function DFScanColumn(
    df::DataFrame,
    df_offset::Int64,
    col_idx::Int64,
    scan_count::Int64,
    output::DuckDB.DataChunk,
    ::Type{T}
) where {T}
    result_array::Vector{T} = DuckDB.GetArray(output, col_idx, T)
    validity = DuckDB.GetValidity(output, col_idx)
    input_column = df[!, col_idx]
    for i in 1:scan_count
        if input_column[df_offset + i] === missing
            DuckDB.SetInvalid(validity, i)
        else
            result_array[i] = input_column[df_offset + i]
        end
    end
end

function DFResultType(df, entry)
    column_type = eltype(df[!, entry])
    if typeof(column_type) == Union
        # remove Missing type from the union
        column_type = Core.Compiler.typesubtract(column_type, Missing, 1)
    end
    return column_type
end

function DFScanFunction(df, entry)
    return DFScanColumn
end

function DFBindFunction(info::DuckDB.BindInfo)
    # fetch the df name from the function parameters
    parameter = DuckDB.GetParameter(info, 0)
    name = DuckDB.GetValue(parameter, String)
    # fetch the actual df using the function name
    extra_data = DuckDB.GetExtraData(info)
    df = extra_data[name]
    # register the result columns

    result_types::Vector{Type} = Vector()
    scan_functions::Vector{Function} = Vector()
    for entry in names(df)
        result_type = DFResultType(df, entry)
        scan_function = DFScanFunction(df, entry)
        push!(result_types, result_type)
        push!(scan_functions, scan_function)

        DuckDB.AddResultColumn(info, entry, result_type)
    end
    return DFBindInfo(df, result_types, scan_functions)
end

mutable struct DFInitInfo
    pos::Int64

    function DFInitInfo()
        return new(0)
    end
end

function DFInitFunction(info::DuckDB.InitInfo)
    return DFInitInfo()
end

function DFScanFunction(info::DuckDB.FunctionInfo, output::DuckDB.DataChunk)
    bind_info = DuckDB.GetBindInfo(info, DFBindInfo)
    init_info = DuckDB.GetInitInfo(info, DFInitInfo)

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
    DuckDB.SetSize(output, scan_count)
    return
end

function RegisterDataFrame(con::Connection, df::DataFrame, name::AbstractString)
    con.db.data_frames[name] = df
    return DBInterface.execute(con, string("CREATE VIEW \"", name, "\" AS SELECT * FROM julia_df_scan('", name, "')"))
end

function RegisterDataFrame(db::DB, df::DataFrame, name::AbstractString)
    return RegisterDataFrame(db.main_connection, df, name)
end

function AddDataFrameScan(db::DB)
    # add the data frame scan function
    DuckDB.CreateTableFunction(
        db.main_connection,
        "julia_df_scan",
        [String],
        DFBindFunction,
        DFInitFunction,
        DFScanFunction,
        db.handle.data_frames
    )
    return
end
