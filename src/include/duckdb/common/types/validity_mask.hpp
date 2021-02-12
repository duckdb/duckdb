//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/validity_mask.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector_size.hpp"

namespace duckdb {

using validity_t = uint64_t;

struct ValidityData {
	static constexpr const int BITS_PER_VALUE = sizeof(validity_t) * 8;
	static constexpr const validity_t MAX_ENTRY = ~validity_t(0);
public:
	explicit ValidityData(idx_t count) {
		auto entry_count = EntryCount(count);
		owned_data = unique_ptr<validity_t[]>(new validity_t[entry_count]);
		for(size_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
			owned_data[entry_idx] = MAX_ENTRY;
		}
	}

	unique_ptr<validity_t[]> owned_data;

public:
	static inline idx_t EntryCount(idx_t count) {
		return (count + (BITS_PER_VALUE - 1)) / BITS_PER_VALUE;
	}
};

//! Type used for validity masks
struct ValidityMask {
	static constexpr const int BITS_PER_VALUE = sizeof(validity_t) * 8;

	ValidityMask() : validity_mask(nullptr) {
	}
	explicit ValidityMask(idx_t max_count) {
		Initialize(max_count);
	}

	bool AllValid() const {
		return !validity_mask;
	}
	validity_t *GetData() const {
		return validity_mask;
	}

	static inline idx_t EntryCount(idx_t count) {
		return ValidityData::EntryCount(count);
	}
	static inline idx_t BitsPerValue() {
		return ValidityData::BITS_PER_VALUE;
	}
	validity_t GetValidityEntry(idx_t entry_idx) const {
		if (!validity_mask) {
			return ValidityData::MAX_ENTRY;
		}
		return validity_mask[entry_idx];
	}
	static inline bool AllValid(validity_t entry) {
		return entry == ValidityData::MAX_ENTRY;
	}
	static inline bool NoneValid(validity_t entry) {
		return entry == 0;
	}
	static inline bool RowIsValid(validity_t entry, idx_t idx_in_entry) {
		return entry & (validity_t(1) << validity_t(idx_in_entry));
	}
	inline void GetEntryIndex(idx_t row_idx, idx_t &entry_idx, idx_t &idx_in_entry) const {
		entry_idx = row_idx / BitsPerValue();
		idx_in_entry = row_idx % BitsPerValue();
	}
	inline bool RowIsValid(idx_t row_idx) const {
		idx_t entry_idx, idx_in_entry;
		GetEntryIndex(row_idx, entry_idx, idx_in_entry);
		auto entry = GetValidityEntry(entry_idx);
		return RowIsValid(entry, idx_in_entry);
	}
	inline void SetValid(idx_t row_idx) {
		idx_t entry_idx, idx_in_entry;
		GetEntryIndex(row_idx, entry_idx, idx_in_entry);
		validity_mask[entry_idx] |= (validity_t(1) << validity_t(idx_in_entry));
	}
	inline void SetInvalid(idx_t row_idx) {
		idx_t entry_idx, idx_in_entry;
		GetEntryIndex(row_idx, entry_idx, idx_in_entry);
		validity_mask[entry_idx] &= ~(validity_t(1) << validity_t(idx_in_entry));
	}
	inline void Set(idx_t row_idx, bool valid) {
		if (valid) {
			SetValid(row_idx);
		} else {
			SetInvalid(row_idx);
		}
	}

	void Combine(const ValidityMask& other, idx_t count) {
		if (other.AllValid()) {
			// X & 1 = X
			return;
		}
		if (AllValid()) {
			// 1 & Y = Y
			Initialize(other);
			return;
		}
		if (validity_mask == other.validity_mask) {
			// X & X == X
			return;
		}
		// have to merge
		// create a new validity mask that contains the combined mask
		auto data = GetData();
		auto other_data = other.GetData();

		Initialize(count);
		auto result_data = GetData();

		auto entry_count = ValidityData::EntryCount(count);
		for(idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
			result_data[entry_idx] = data[entry_idx] & other_data[entry_idx];
		}
	}
	string ToString(idx_t count) const {
		string result = "Validity Mask (" + to_string(count) + ") [";
		for (idx_t i = 0; i < count; i++) {
			result += RowIsValid(i) ? "." : "X";
		}
		result += "]";
		return result;
	}

public:
	void Initialize(validity_t *validity) {
		validity_data.reset();
		validity_mask = validity;
	}
	void Initialize(const ValidityMask &other) {
		validity_mask = other.validity_mask;
		validity_data = other.validity_data;
	}
	void Initialize(idx_t count = STANDARD_VECTOR_SIZE) {
		validity_data = make_buffer<ValidityData>(count);
		validity_mask = validity_data->owned_data.get();
	}
private:
	validity_t *validity_mask;
	buffer_ptr<ValidityData> validity_data;
};

}

