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

struct PrimitiveCastOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return TGT(input);
	}
};

template <class SRC, class TGT = SRC, class CAST_OP = PrimitiveCastOperator>
class PrimitiveDictionary {
private:
	static_assert(!std::is_same<SRC, string_t>::value ||
	                  (std::is_same<SRC, string_t>::value && std::is_same<TGT, string_t>::value),
	              "If SRC is string_t, TGT must also be string_t");

	static constexpr idx_t LOAD_FACTOR = 2;

	static constexpr uint32_t INVALID_INDEX = static_cast<uint32_t>(-1);
	struct primitive_dictionary_entry_t {
		SRC value;
		uint32_t index;
		bool IsEmpty() const {
			return index == INVALID_INDEX;
		}
	};

public:
	static constexpr uint32_t MAXIMUM_POSSIBLE_SIZE = INVALID_INDEX - 1;

	//! PrimitiveDictionary is a fixed-size linear probing hash table for primitive types
	//! It is used to dictionary-encode data in, e.g., Parquet files
	PrimitiveDictionary(Allocator &allocator, idx_t maximum_size_p, idx_t target_capacity_p)
	    : maximum_size(maximum_size_p), size(0), capacity(NextPowerOfTwo(maximum_size * LOAD_FACTOR)),
	      capacity_mask(capacity - 1), target_capacity(target_capacity_p), target_offset(0),
	      allocated_dictionary(allocator.Allocate(capacity * sizeof(primitive_dictionary_entry_t))),
	      allocated_target(
	          allocator.Allocate(std::is_same<TGT, string_t>::value ? target_capacity : capacity * sizeof(TGT))),
	      dictionary(reinterpret_cast<primitive_dictionary_entry_t *>(allocated_dictionary.get())),
	      target_values(reinterpret_cast<TGT *>(allocated_target.get())), target_raw(allocated_target.get()),
	      full(false) {
		// Initialize empty
		for (idx_t i = 0; i < capacity; i++) {
			dictionary[i].index = INVALID_INDEX;
		}
	}

public:
	//! Insert value into dictionary (if not full)
	void Insert(SRC value) {
		if (full) {
			return;
		}
		auto &entry = Lookup(value);
		if (entry.IsEmpty()) {
			if (size + 1 > maximum_size || !AddToTarget(value)) {
				full = true;
				return;
			}
			entry.value = value;
			entry.index = size++;
		}
	}

	//! Get dictionary index of an already inserted value
	uint32_t GetIndex(const SRC &value) const {
		const auto &entry = Lookup(value);
		D_ASSERT(!entry.IsEmpty());
		return entry.index;
	}

	//! Iterates over inserted values
	template <typename U = SRC, typename std::enable_if<!std::is_same<U, string_t>::value, int>::type = 0>
	void IterateValues(const std::function<void(const SRC &, const TGT &)> &fun) const {
		for (idx_t i = 0; i < capacity; i++) {
			auto &entry = dictionary[i];
			if (entry.IsEmpty()) {
				continue;
			}
			fun(entry.value, target_values[entry.index]);
		}
	}

	//! Specialized template to iterate over string_t values
	template <typename U = SRC, typename std::enable_if<std::is_same<U, string_t>::value, int>::type = 0>
	void IterateValues(const std::function<void(const SRC &, const TGT &)> &fun) const {
		for (idx_t i = 0; i < capacity; i++) {
			auto &entry = dictionary[i];
			if (entry.IsEmpty()) {
				continue;
			}
			fun(entry.value, entry.value);
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

	//! Get the target written values as a memory stream (zero-copy)
	unique_ptr<MemoryStream> GetTargetMemoryStream() const {
		auto result = make_uniq<MemoryStream>(target_raw, target_capacity);
		result->SetPosition(target_offset);
		return result;
	}

private:
	//! Looks up a value in the dictionary using linear probing
	primitive_dictionary_entry_t &Lookup(const SRC &value) const {
		auto offset = Hash(value) & capacity_mask;
		while (!dictionary[offset].IsEmpty() && dictionary[offset].value != value) {
			++offset &= capacity_mask;
		}
		return dictionary[offset];
	}

	//! Writes a value to the target data
	template <typename U = SRC, typename std::enable_if<!std::is_same<U, string_t>::value, int>::type = 0>
	bool AddToTarget(const SRC &src_value) {
		const auto tgt_value = CAST_OP::template Operation<SRC, TGT>(src_value);
		target_values[size] = tgt_value;
		target_offset += sizeof(TGT);
		return true;
	}

	//! Specialized template to add a string_t value to the target data
	template <typename U = SRC, typename std::enable_if<std::is_same<U, string_t>::value, int>::type = 0>
	bool AddToTarget(SRC &src_value) {
		if (target_offset + sizeof(uint32_t) + src_value.GetSize() > target_capacity) {
			return false; // Out of capacity
		}

		// Store string length and increment offset
		Store<uint32_t>(UnsafeNumericCast<uint32_t>(src_value.GetSize()), target_raw + target_offset);
		target_offset += sizeof(uint32_t);

		// Copy over string data to target, update "value" to point to it, and increment offset
		memcpy(target_raw + target_offset, src_value.GetData(), src_value.GetSize());
		if (!src_value.IsInlined()) {
			src_value.SetPointer(char_ptr_cast(target_raw + target_offset));
		}
		target_offset += src_value.GetSize();

		return true;
	}

private:
	//! Maximum size and current size
	const idx_t maximum_size;
	idx_t size;

	//! Capacity (power of two) and corresponding mask
	const idx_t capacity;
	const idx_t capacity_mask;

	//! Capacity/offset of target encoded data
	const idx_t target_capacity;
	idx_t target_offset;

	//! Allocated regions for dictionary/target
	AllocatedData allocated_dictionary;
	AllocatedData allocated_target;

	//! Pointers to allocated regions for convenience
	primitive_dictionary_entry_t *const dictionary;
	TGT *const target_values;
	data_ptr_t const target_raw;

	//! More values inserted than possible
	bool full;
};

} // namespace duckdb
