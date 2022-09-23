mutable struct ConnectionException <: Exception
    var::String
end
mutable struct QueryException <: Exception
    var::String
end
mutable struct NotImplementedException <: Exception
    var::String
end
mutable struct InvalidInputException <: Exception
    var::String
end

Base.showerror(io::IO, e::ConnectionException) = print(io, e.var)
Base.showerror(io::IO, e::QueryException) = print(io, e.var)
Base.showerror(io::IO, e::NotImplementedException) = print(io, e.var)
Base.showerror(io::IO, e::InvalidInputException) = print(io, e.var)
