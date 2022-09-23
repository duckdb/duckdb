"""
Configuration object
"""
mutable struct Config
    handle::duckdb_config

    function Config()
        handle = Ref{duckdb_connection}()
        duckdb_create_config(handle)

        result = new(handle[])
        finalizer(_destroy_config, result)
        return result
    end
end

function _destroy_config(config::Config)
    if config.handle != C_NULL
        duckdb_destroy_config(config.handle)
    end
    config.handle = C_NULL
    return
end

function set_config(config::Config, name::AbstractString, option::AbstractString)
    if duckdb_set_config(config.handle, name, option) != DuckDBSuccess
        throw(QueryException(string("Unrecognized configuration option \"", name, "\"")))
    end
end

DBInterface.close!(config::Config) = _destroy_config(config)
