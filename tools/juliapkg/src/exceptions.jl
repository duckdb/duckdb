mutable struct ConnectionException <: Exception
	var::String
end

Base.showerror(io::IO, e::ConnectionException) = print(io, e.var)
