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
#include "duckdb/common/to_string.hpp"

namespace duckdb {
struct ValidityMask;

template <typename V>
struct TemplatedValidityData {
	static constexpr const int BITS_PER_VALUE = sizeof(V) * 8;
	static constexpr const V MAX_ENTRY = ~V(0);

public:
	explicit TemplatedValidityData(idx_t count) {
		auto entry_count = EntryCount(count);
		owned_data = unique_ptr<V[]>(new V[entry_count]);
		for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
			owned_data[entry_idx] = MAX_ENTRY;
		}
	}
	TemplatedValidityData(const V *validity_mask, idx_t count) {
		D_ASSERT(validity_mask);
		auto entry_count = EntryCount(count);
		owned_data = unique_ptr<V[]>(new V[entry_count]);
		for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
			owned_data[entry_idx] = validity_mask[entry_idx];
		}
	}

	unique_ptr<V[]> owned_data;

public:
	static inline idx_t EntryCount(idx_t count) {
		return (count + (BITS_PER_VALUE - 1)) / BITS_PER_VALUE;
	}
};

using validity_t = uint64_t;

struct ValidityData : TemplatedValidityData<validity_t> {
public:
	DUCKDB_API explicit ValidityData(idx_t count);
	DUCKDB_API ValidityData(const ValidityMask &original, idx_t count);
};

//! Type used for validity masks
template <typename V>
struct TemplatedValidityMask {
	using ValidityBuffer = TemplatedValidityData<V>;

public:
	static constexpr const int BITS_PER_VALUE = ValidityBuffer::BITS_PER_VALUE;
	static constexpr const int STANDARD_ENTRY_COUNT = (STANDARD_VECTOR_SIZE + (BITS_PER_VALUE - 1)) / BITS_PER_VALUE;
	static constexpr const int STANDARD_MASK_SIZE = STANDARD_ENTRY_COUNT * sizeof(validity_t);

public:
	TemplatedValidityMask() : validity_mask(nullptr) {
	}
	explicit TemplatedValidityMask(idx_t max_count) {
		Initialize(max_count);
	}
	explicit TemplatedValidityMask(V *ptr) : validity_mask(ptr) {
	}
	TemplatedValidityMask(const TemplatedValidityMask &original, idx_t count) {
		Copy(original, count);
	}

	static inline idx_t ValidityMaskSize(idx_t count = STANDARD_VECTOR_SIZE) {
		return ValidityBuffer::EntryCount(count) * sizeof(V);
	}
	inline bool AllValid() const {
		return !validity_mask;
	}
	bool CheckAllValid(idx_t count) const {
		if (AllValid()) {
			return true;
		}
		idx_t entry_count = ValidityBuffer::EntryCount(count);
		idx_t valid_count = 0;
		for (idx_t i = 0; i < entry_count; i++) {
			valid_count += validity_mask[i] == ValidityBuffer::MAX_ENTRY;
		}
		return valid_count == entry_count;
	}
	inline V *GetData() const {
		return validity_mask;
	}
	void Reset() {
		validity_mask = nullptr;
		validity_data.reset();
	}

	static inline idx_t EntryCount(idx_t count) {
		return ValidityBuffer::EntryCount(count);
	}
	inline V GetValidityEntry(idx_t entry_idx) const {
		if (!validity_mask) {
			return ValidityBuffer::MAX_ENTRY;
		}
		return validity_mask[entry_idx];
	}
	static inline bool AllValid(V entry) {
		return entry == ValidityBuffer::MAX_ENTRY;
	}
	static inline bool NoneValid(V entry) {
		return entry == 0;
	}
	static inline bool RowIsValid(V entry, idx_t idx_in_entry) {
		return entry & (V(1) << V(idx_in_entry));
	}
	static inline void GetEntryIndex(idx_t row_idx, idx_t &entry_idx, idx_t &idx_in_entry) {
		entry_idx = row_idx / BITS_PER_VALUE;
		idx_in_entry = row_idx % BITS_PER_VALUE;
	}

	//! RowIsValidUnsafe should only be used if AllValid() is false: it achieves the same as RowIsValid but skips a
	//! not-null check
	inline bool RowIsValidUnsafe(idx_t row_idx) const {
		D_ASSERT(validity_mask);
		idx_t entry_idx, idx_in_entry;
		GetEntryIndex(row_idx, entry_idx, idx_in_entry);
		auto entry = GetValidityEntry(entry_idx);
		return RowIsValid(entry, idx_in_entry);
	}

	//! Returns true if a row is valid (i.e. not null), false otherwise
	inline bool RowIsValid(idx_t row_idx) const {
		if (!validity_mask) {
			return true;
		}
		return RowIsValidUnsafe(row_idx);
	}

	//! Same as SetValid, but skips a null check on validity_mask
	inline void SetValidUnsafe(idx_t row_idx) {
		D_ASSERT(validity_mask);
		idx_t entry_idx, idx_in_entry;
		GetEntryIndex(row_idx, entry_idx, idx_in_entry);
		validity_mask[entry_idx] |= (V(1) << V(idx_in_entry));
	}

	//! Marks the entry at the specified row index as valid (i.e. not-null)
	inline void SetValid(idx_t row_idx) {
		if (!validity_mask) {
			// if AllValid() we don't need to do anything
			// the row is already valid
			return;
		}
		SetValidUnsafe(row_idx);
	}

	//! Marks the bit at the specified entry as invalid (i.e. null)
	inline void SetInvalidUnsafe(idx_t entry_idx, idx_t idx_in_entry) {
		D_ASSERT(validity_mask);
		validity_mask[entry_idx] &= ~(V(1) << V(idx_in_entry));
	}

	//! Marks the bit at the specified row index as invalid (i.e. null)
	inline void SetInvalidUnsafe(idx_t row_idx) {
		idx_t entry_idx, idx_in_entry;
		GetEntryIndex(row_idx, entry_idx, idx_in_entry);
		SetInvalidUnsafe(entry_idx, idx_in_entry);
	}

	//! Marks the entry at the specified row index as invalid (i.e. null)
	inline void SetInvalid(idx_t row_idx) {
		if (!validity_mask) {
			D_ASSERT(row_idx <= STANDARD_VECTOR_SIZE);
			Initialize(STANDARD_VECTOR_SIZE);
		}
		SetInvalidUnsafe(row_idx);
	}

	//! Mark the entrry at the specified index as either valid or invalid (non-null or null)
	inline void Set(idx_t row_idx, bool valid) {
		if (valid) {
			SetValid(row_idx);
		} else {
			SetInvalid(row_idx);
		}
	}

	//! Ensure the validity mask is writable, allocating space if it is not initialized
	inline void EnsureWritable() {
		if (!validity_mask) {
			Initialize();
		}
	}

	//! Marks "count" entries in the validity mask as invalid (null)
	inline void SetAllInvalid(idx_t count) {
		EnsureWritable();
		for (idx_t i = 0; i < ValidityBuffer::EntryCount(count); i++) {
			validity_mask[i] = 0;
		}
	}

	//! Marks "count" entries in the validity mask as valid (not null)
	inline void SetAllValid(idx_t count) {
		EnsureWritable();
		for (idx_t i = 0; i < ValidityBuffer::EntryCount(count); i++) {
			validity_mask[i] = ValidityBuffer::MAX_ENTRY;
		}
	}

	inline bool IsMaskSet() const {
		if (validity_mask) {
			return true;
		}
		return false;
	}

public:
	void Initialize(validity_t *validity) {
		validity_data.reset();
		validity_mask = validity;
	}
	void Initialize(const TemplatedValidityMask &other) {
		validity_mask = other.validity_mask;
		validity_data = other.validity_data;
	}
	void Initialize(idx_t count = STANDARD_VECTOR_SIZE) {
		validity_data = make_buffer<ValidityBuffer>(count);
		validity_mask = validity_data->owned_data.get();
	}
	void Copy(const TemplatedValidityMask &other, idx_t count) {
		if (other.AllValid()) {
			validity_data = nullptr;
			validity_mask = nullptr;
		} else {
			validity_data = make_buffer<ValidityBuffer>(other.validity_mask, count);
			validity_mask = validity_data->owned_data.get();
		}
	}

protected:
	V *validity_mask;
	buffer_ptr<ValidityBuffer> validity_data;
};

struct ValidityMask : public TemplatedValidityMask<validity_t> {
public:
	ValidityMask() : TemplatedValidityMask(nullptr) {
	}
	explicit ValidityMask(idx_t max_count) : TemplatedValidityMask(max_count) {
	}
	explicit ValidityMask(validity_t *ptr) : TemplatedValidityMask(ptr) {
	}
	ValidityMask(const ValidityMask &original, idx_t count) : TemplatedValidityMask(original, count) {
	}

public:
	void Resize(idx_t old_size, idx_t new_size);

	void Slice(const ValidityMask &other, idx_t offset);
	void Combine(const ValidityMask &other, idx_t count);
	string ToString(idx_t count) const;
};

} // namespace duckdb
