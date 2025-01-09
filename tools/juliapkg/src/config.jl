"""
Configuration object
"""
mutable struct Config
    handle::duckdb_config

    function Config(; kwargs...)
        handle = Ref{duckdb_config}()
        duckdb_create_config(handle)
        config = new(handle[])
        finalizer(_destroy_config, config)

        for (name, option) in kwargs
            set_config(config, string(name), to_config_value(option))
        end

        return config
    end
end

function Config(kwargs)
    config = Config()
    for (name, option) in kwargs
        set_config(config, name, option)
    end
    return config
end

function _destroy_config(config::Config)
    if config.handle != C_NULL
        duckdb_destroy_config(config.handle)
    end
    config.handle = C_NULL
    return
end

const ConfigKey = Union{AbstractString, Symbol}
const ConfigValue = Union{AbstractString, AbstractFloat, Bool, Integer, Dict}

to_config_value(value::ConfigValue) = string(value)
to_config_value(value::Dict) = JSON.json(value)

function set_config(config::Config, name::ConfigKey, option::ConfigValue)
    k = string(name)
    option_value = to_config_value(option)
    if duckdb_set_config(config.handle, k, option_value) != DuckDBSuccess
        throw(QueryException(string("Unrecognized configuration option \"", name, "\"")))
    end
end

DBInterface.close!(config::Config) = _destroy_config(config)
