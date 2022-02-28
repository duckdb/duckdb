mutable struct ConnectionException <: Exception
    var::String
end
mutable struct QueryException <: Exception
    var::String
end

Base.showerror(io::IO, e::ConnectionException) = print(io, e.var)
Base.showerror(io::IO, e::QueryException) = print(io, e.var)
