using DataFrames

mutable struct DFBindInfo
    df::DataFrame

    function DFBindInfo(df::DataFrame)
        return new(df)
    end
end

function DFBindFunction(info::DuckDB.BindInfo)
    # fetch the df name from the function parameters
    parameter = DuckDB.GetParameter(info, 0)
    name = DuckDB.GetValue(parameter, String)
    # fetch the actual df using the function name
    extra_data = DuckDB.GetExtraData(info)
    df = extra_data[name]
    # register the result columns
    for entry in names(df)
        # FIXME; actually get the type of the data frame
        DuckDB.AddResultColumn(info, entry, Int64)
    end
    return DFBindInfo(df)
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
    bind_info::DFBindInfo = DuckDB.GetBindInfo(info)
    init_info::DFInitInfo = DuckDB.GetInitInfo(info)

    column_count = size(names(bind_info.df), 1)
    row_count = size(bind_info.df, 1)
    scan_count = DuckDB.VECTOR_SIZE
    if init_info.pos + scan_count >= row_count
        scan_count = row_count - init_info.pos
    end

    for col_idx in 1:column_count
        result_array::Vector{Int64} = DuckDB.GetArray(output, col_idx - 1, Int64)
        input_column = bind_info.df[!, col_idx]
        for i in 1:scan_count
            result_array[i] = input_column[init_info.pos + i]
        end
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
    return DuckDB.CreateTableFunction(
        db.main_connection,
        "julia_df_scan",
        [String],
        DFBindFunction,
        DFInitFunction,
        DFScanFunction,
        db.handle.data_frames
    )
end
