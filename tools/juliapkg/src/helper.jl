
function esc_id end

esc_id(x::AbstractString) = "\"" * replace(x, "\"" => "\"\"") * "\""
esc_id(X::AbstractVector{S}) where {S <: AbstractString} = join(map(esc_id, X), ',')
