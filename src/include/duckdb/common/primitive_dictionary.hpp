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
#include "duckdb/common/serializer/memory_stream.hpp"

namespace duckdb {

template <class T>
class PrimitiveDictionary {
private:
	static constexpr idx_t LOAD_FACTOR = 2;

	static constexpr uint32_t INVALID_INDEX = static_cast<uint32_t>(-1);
	struct primitive_dictionary_entry_t {
		T value;
		uint32_t index;
		bool IsEmpty() const {
			return index == INVALID_INDEX;
		}
	};

public:
	static constexpr uint32_t MAXIMUM_POSSIBLE_SIZE = INVALID_INDEX - 1;

	//! PrimitiveDictionary is a fixed-size linear probing hash table for primitive types
	//! It is used to dictionary-encode data in, e.g., Parquet files
	PrimitiveDictionary(Allocator &allocator, idx_t maximum_size_p, idx_t plain_capacity_p)
	    : maximum_size(maximum_size_p), size(0), capacity(NextPowerOfTwo(maximum_size * LOAD_FACTOR)),
	      capacity_mask(capacity - 1), plain_capacity(plain_capacity_p), plain_offset(0),
	      allocated_dictionary(allocator.Allocate(capacity * sizeof(primitive_dictionary_entry_t))),
	      allocated_plain(allocator.Allocate(std::is_same<T, string_t>::value ? plain_capacity : capacity * sizeof(T))),
	      dictionary(reinterpret_cast<primitive_dictionary_entry_t *>(allocated_dictionary.get())),
	      plain(reinterpret_cast<T *>(allocated_plain.get())), plain_raw(allocated_plain.get()), full(false) {
		// Initialize empty
		for (idx_t i = 0; i < capacity; i++) {
			dictionary[i].index = INVALID_INDEX;
		}
	}

public:
	//! Insert value into dictionary (if not full)
	void Insert(T value) {
		if (full) {
			return;
		}
		auto &entry = Lookup(value);
		if (entry.IsEmpty()) {
			if (size + 1 > maximum_size || !AddToPlain(value)) {
				full = true;
				return;
			}
			entry.value = value;
			entry.index = size++;
		}
	}

	//! Get dictionary index of an already inserted value
	uint32_t GetIndex(const T &value) const {
		const auto &entry = Lookup(value);
		D_ASSERT(!entry.IsEmpty());
		return entry.index;
	}

	//! Iterates over inserted values
	template <typename U = T, typename std::enable_if<!std::is_same<U, string_t>::value, int>::type = 0>
	void IterateValues(const std::function<void(const T &)> &fun) const {
		for (idx_t i = 0; i < size; i++) {
			fun(plain[i]);
		}
	}

	//! Specialized template to iterate over string_t values
	template <typename U = T, typename std::enable_if<std::is_same<U, string_t>::value, int>::type = 0>
	void IterateValues(const std::function<void(const string_t &)> &fun) const {
		for (idx_t i = 0; i < capacity; i++) {
			auto &entry = dictionary[i];
			if (entry.IsEmpty()) {
				continue;
			}
			fun(entry.value);
		}
	}

	//! Get the number of unique values in the dictionary
	idx_t GetSize() const {
		return size;
	}

	//! If any of the inserts caused the dictionary to be full, this returns true
	bool IsFull() const {
		return full;
	}

	//! Get the plain written values as a memory stream (zero-copy)
	unique_ptr<MemoryStream> GetPlainMemoryStream() const {
		auto result = make_uniq<MemoryStream>(plain_raw, plain_capacity);
		result->SetPosition(plain_offset);
		return result;
	}

private:
	//! Looks up a value in the dictionary using linear probing
	primitive_dictionary_entry_t &Lookup(const T &value) const {
		auto offset = Hash(value) & capacity_mask;
		while (!dictionary[offset].IsEmpty() && dictionary[offset].value != value) {
			++offset &= capacity_mask;
		}
		return dictionary[offset];
	}

	//! Writes a value to the plain data
	bool AddToPlain(const T &value) {
		plain[size] = value;
		plain_offset += sizeof(T);
		return true;
	}

	//! Specialized template to add a string_t value to the plain data
	bool AddToPlain(string_t &value) {
		if (plain_offset + sizeof(uint32_t) + value.GetSize() > plain_capacity) {
			return false; // Out of capacity
		}

		// Store string length and increment offset
		Store<uint32_t>(UnsafeNumericCast<uint32_t>(value.GetSize()), plain_raw + plain_offset);
		plain_offset += sizeof(uint32_t);

		// Copy over string data to plain, update "value" to point to it, and increment offset
		memcpy(plain_raw + plain_offset, value.GetData(), value.GetSize());
		value = string_t(char_ptr_cast(plain_raw + plain_offset), value.GetSize());
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
	T *const plain;
	data_ptr_t const plain_raw;

	//! More values inserted than possible
	bool full;
};

} // namespace duckdb
