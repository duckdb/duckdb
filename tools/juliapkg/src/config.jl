"""
Configuration object
"""
mutable struct Config
    handle::duckdb_config

    function Config(args...; kwargs...)
        handle = Ref{duckdb_connection}()
        duckdb_create_config(handle)

        result = new(handle[])
        finalizer(_destroy_config, result)

        _fill_config!(result, args...; kwargs...)

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

DBInterface.close!(config::Config) = _destroy_config(config)

function Base.setindex!(config::Config, option::AbstractString, name::AbstractString)
    if duckdb_set_config(config.handle, name, option) != DuckDBSuccess
        throw(QueryException(string("Unrecognized configuration option \"", name, "\"")))
    end
end

@deprecate set_config(config::Config, name::AbstractString, option::AbstractString) setindex!(config, option, name)

_fill_config!(config, options::AbstractVector) =
    for (name, option) in options
        config[name] = option
    end

_fill_config!(config, options::Union{NamedTuple, AbstractDict}) =
    for (name, option) in pairs(options)
        config[string(name)] = option
    end

_fill_config!(config; kwargs...) = _fill_config!(config, NamedTuple(kwargs))
