"""
DuckDB validity mask
"""
mutable struct ValidityMask
    data::Vector{UInt64}

    function ValidityMask(data::Vector{UInt64})
        result = new(data)
        return result
    end
end

const BITS_PER_VALUE = 64;

function GetEntryIndex(row_idx)
    return ((row_idx - 1) ÷ BITS_PER_VALUE) + 1
end

function GetIndexInEntry(row_idx)
    return (row_idx - 1) % BITS_PER_VALUE
end

function SetInvalid(mask::ValidityMask, index)
    entry_idx = GetEntryIndex(index)
    index_in_entry = GetIndexInEntry(index)
    mask.data[entry_idx] &= ~(1 << index_in_entry)
    return
end

function IsValid(mask::ValidityMask, index)::Bool
    entry_idx = GetEntryIndex(index)
    index_in_entry = GetIndexInEntry(index)
    return (mask.data[entry_idx] & (1 << index_in_entry)) != 0
end
