"""
DuckDB validity mask
"""
struct ValidityMask
    data::Vector{UInt64}

    function ValidityMask(data::Vector{UInt64})
        result = new(data)
        return result
    end
end

const BITS_PER_VALUE = 64;

function get_entry_index(row_idx)
    return ((row_idx - 1) ÷ BITS_PER_VALUE) + 1
end

function get_index_in_entry(row_idx)
    return (row_idx - 1) % BITS_PER_VALUE
end

function setinvalid(mask::ValidityMask, index)
    entry_idx = get_entry_index(index)
    index_in_entry = get_index_in_entry(index)
    mask.data[entry_idx] &= ~(1 << index_in_entry)
    return
end

function isvalid(mask::ValidityMask, index)::Bool
    entry_idx = get_entry_index(index)
    index_in_entry = get_index_in_entry(index)
    return (mask.data[entry_idx] & (1 << index_in_entry)) != 0
end
