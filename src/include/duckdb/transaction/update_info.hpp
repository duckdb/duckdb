//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/update_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/transaction/transaction_data.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/transaction/undo_buffer_allocator.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class UpdateSegment;
struct DataTableInfo;
class DuckTableEntry;

//! Bitmask indicating for each tuple of an UpdateInfo whether the value before the update was the base value
struct UpdateBaseFlags {
	explicit UpdateBaseFlags(uint64_t *bits_p) : bits(bits_p) {
	}

	bool IsSet(idx_t idx) const {
		return bits[idx / 64] & (uint64_t(1) << (idx % 64));
	}
	void Set(idx_t idx, bool value) {
		auto bit = uint64_t(1) << (idx % 64);
		bits[idx / 64] = value ? bits[idx / 64] | bit : bits[idx / 64] & ~bit;
	}
	//! Whether any of the first "count" flags is set
	bool AnySet(idx_t count) const {
		return CountSet(count) > 0;
	}
	//! The amount of flags that are set among the first "count" flags
	idx_t CountSet(idx_t count) const {
		idx_t full_words = count / 64;
		idx_t result = 0;
		for (idx_t i = 0; i < full_words; i++) {
			result += PopCount(bits[i]);
		}
		auto remainder = count % 64;
		if (remainder) {
			result += PopCount(bits[full_words] & ((uint64_t(1) << remainder) - 1));
		}
		return result;
	}
	static idx_t WordCount(idx_t count) {
		return (count + 63) / 64;
	}
	static idx_t PopCount(uint64_t value) {
		value = value - ((value >> 1) & 0x5555555555555555ULL);
		value = (value & 0x3333333333333333ULL) + ((value >> 2) & 0x3333333333333333ULL);
		value = (value + (value >> 4)) & 0x0F0F0F0F0F0F0F0FULL;
		return idx_t((value * 0x0101010101010101ULL) >> 56);
	}

	uint64_t *bits;
};

//! Writes base flags in order, one word at a time
struct UpdateBaseFlagsAppender {
	explicit UpdateBaseFlagsAppender(uint64_t *bits_p) : bits(bits_p) {
	}

	void Append(bool value) {
		current |= uint64_t(value) << (count % 64);
		count++;
		if (count % 64 == 0) {
			bits[count / 64 - 1] = current;
			current = 0;
		}
	}
	void Finalize() {
		if (count % 64 != 0) {
			bits[count / 64] = current;
		}
	}

	uint64_t *bits;
	uint64_t current = 0;
	idx_t count = 0;
};

//! UpdateInfo is a class that represents a set of updates applied to a single vector.
//! The UpdateInfo struct contains metadata associated with the update.
//! The root UpdateInfo of a vector holds the latest values, and is laid out as:
//! [UpdateInfo][TUPLES (sel_t[max])][VALUES (T[max])]
//! The UpdateInfo of a transaction holds the values from before its updates. Tuples that had no update before have
//! their base flag set instead, and only the other tuples have a value (in order of the tuples):
//! [UpdateInfo][TUPLES (sel_t[max])][BASE FLAGS (uint64_t[max / 64])][VALUES (T[value_max])]
//! The required allocation size can be obtained using UpdateInfo::GetAllocSize
struct UpdateInfo {
	//! The update segment that this update info affects
	UpdateSegment *segment;
	//! The table this was update was made on
	DuckTableEntry *table;
	//! The column index of which column we are updating
	idx_t column_index;
	//! The start index of the row group
	idx_t row_group_start;
	//! The version number
	atomic<transaction_t> version_number;
	//! The vector index within the uncompressed segment
	idx_t vector_index;
	//! The amount of updated tuples
	sel_t N; // NOLINT
	//! The maximum amount of tuples that can fit into this UpdateInfo
	sel_t max;
	//! The previous update info (or nullptr if it is the base)
	UndoBufferPointer prev;
	//! The next update info in the chain (or nullptr if it is the last)
	UndoBufferPointer next;
	//! The offset of the base flags (or 0 for the root)
	idx_t base_flags_offset;
	//! The maximum amount of values that can fit into this UpdateInfo (equal to max for the root)
	sel_t value_max;

	//! The row ids of the tuples that have been updated. This should always be kept sorted!
	sel_t *GetTuples();

	//! The update values
	data_ptr_t GetValues();

	template <class T>
	T *GetData() {
		return reinterpret_cast<T *>(GetValues());
	}

	//! For each tuple, whether the value before this update was the base value - only set for transactions
	UpdateBaseFlags GetBaseFlags();

	bool AppliesToTransaction(const SnapshotView &view) {
		// these tuples are either committed outside this transaction's snapshot or not committed yet, use
		// tuples stored in this version
		if (version_number == MAX_COMMIT_ID) {
			// dummy transaction number for the root element - should always match
			return true;
		}
		return !view.Sees(version_number.load());
	}

	//! Collect the update infos after the root that are not visible to the transaction, newest to oldest
	static void GetInvisibleUpdates(UpdateInfo &root, const SnapshotView &view, vector<UndoBufferReference> &result) {
		auto update_ptr = root.next;
		while (update_ptr.IsSet()) {
			auto pin = update_ptr.Pin();
			auto &info = Get(pin);
			update_ptr = info.next;
			if (info.AppliesToTransaction(view)) {
				result.push_back(std::move(pin));
			}
		}
	}

	Value GetValue(idx_t index);
	string ToString();
	void Print();
	void Verify();
	bool HasPrev() const;
	bool HasNext() const;
	static UpdateInfo &Get(UndoBufferReference &entry);
	//! Returns the total allocation size for a root UpdateInfo entry with a specific capacity
	static idx_t GetAllocSize(idx_t type_size, idx_t capacity);
	//! Returns the total allocation size for the UpdateInfo entry of a transaction
	static idx_t GetAllocSize(idx_t type_size, idx_t capacity, idx_t value_capacity);
	//! Computes a compact capacity for a given count (rounds up with growth headroom)
	static idx_t GetCompactCapacity(idx_t count);
	//! Initialize a root UpdateInfo struct that has been allocated using GetAllocSize
	static void Initialize(UpdateInfo &info, DuckTableEntry &table_entry, transaction_t transaction_id,
	                       idx_t row_group_start, idx_t capacity);
	//! Initialize the UpdateInfo struct of a transaction that has been allocated using GetAllocSize
	static void Initialize(UpdateInfo &info, DuckTableEntry &table_entry, transaction_t transaction_id,
	                       idx_t row_group_start, idx_t capacity, idx_t value_capacity);
};

} // namespace duckdb
