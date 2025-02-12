//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/primitive_dictionary.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/allocator.hpp"

namespace duckdb {

template <class T>
class PrimitiveDictionary {
private:
	static constexpr uint32_t INVALID_OFFSET = static_cast<uint32_t>(-1);

	struct primitive_dictionary_entry_t {
		T value;
		uint32_t offset;
	};

public:
	PrimitiveDictionary(Allocator &allocator, idx_t maximum_size_p, idx_t plain_capacity_p)
	    : maximum_size(maximum_size_p), size(0), capacity(NextPowerOfTwo(maximum_size * 2)),
	      capacity_mask(capacity - 1), plain_capacity(plain_capacity_p), plain_offset(0),
	      allocated_dictionary(allocator.Allocate(capacity * sizeof(primitive_dictionary_entry_t))),
	      allocated_plain(allocator.Allocate(std::is_same<T, string_t>::value ? plain_capacity : capacity * sizeof(T))),
	      dictionary(reinterpret_cast<primitive_dictionary_entry_t *>(allocated_dictionary.get())),
	      plain(allocated_plain.get()) {
		// Initialize empty
		for (idx_t i = 0; i < capacity; i++) {
			dictionary[i].offset = INVALID_OFFSET;
		}
	}

public:
	bool Insert(T value, uint32_t &offset) {
		auto &entry = Lookup(value);
		bool success = size < capacity;
		if (entry.offset == INVALID_OFFSET) {
			success &= AddToPlain(value);
			entry.value = value;
			entry.offset = size++;
		}
		offset = entry.offset;
		return success;
	}

	uint32_t GetOffset(const T &value) const {
		return Lookup(value).offset;
	}

private:
	primitive_dictionary_entry_t &Lookup(const T &value) const {
		return dictionary[Hash(value) & capacity_mask];
	}

	bool AddToPlain(const T &value) {
		static_cast<T *const>(plain)[plain_offset++] = value;
		return true;
	}

	bool AddToPlain(string_t &value) {
		if (plain_offset + sizeof(uint32_t) + value.GetSize() > plain_capacity) {
			return false; // Out of capacity
		}

		// Store string length and increment offset
		Store<uint32_t>(UnsafeNumericCast<uint32_t>(value.GetSize()), plain + plain_offset);
		plain_offset += sizeof(uint32_t);

		// Copy over string data to plain, update "value" to point to it, and increment offset
		memcpy(plain + plain_offset, value.GetData(), value.GetSize());
		value = string_t(char_ptr_cast(plain + plain_offset), value.GetSize());
		plain_offset += value.GetSize();

		return true;
	}

private:
	//! Maximum size and current size
	const idx_t maximum_size;
	idx_t size;

	//! Capacity (power of two) and corresponding mask
	const idx_t capacity;
	const idx_t capacity_mask;

	//! Capacity/offset of plain encoded data
	const idx_t plain_capacity;
	idx_t plain_offset;

	//! Allocated regions for dictionary/plain
	AllocatedData allocated_dictionary;
	AllocatedData allocated_plain;

	//! Pointers to allocated regions for convenience
	primitive_dictionary_entry_t *const dictionary;
	data_ptr_t const plain;
};

} // namespace duckdb
