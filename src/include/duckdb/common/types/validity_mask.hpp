//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/validity_mask.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector_size.hpp"

namespace duckdb {
struct ValidityMask;

template <typename V>
struct TemplatedValidityData {
	static constexpr const int BITS_PER_VALUE = sizeof(V) * 8;
	static constexpr const V MAX_ENTRY = ~V(0);

public:
	inline explicit TemplatedValidityData(idx_t count) {
		auto entry_count = EntryCount(count);
		owned_data = make_unsafe_uniq_array<V>(entry_count);
		for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
			owned_data[entry_idx] = MAX_ENTRY;
		}
	}
	inline TemplatedValidityData(const V *validity_mask, idx_t count) {
		D_ASSERT(validity_mask);
		auto entry_count = EntryCount(count);
		owned_data = make_unsafe_uniq_array<V>(entry_count);
		for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
			owned_data[entry_idx] = validity_mask[entry_idx];
		}
	}

	unsafe_unique_array<V> owned_data;

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
	inline TemplatedValidityMask() : validity_mask(nullptr) {
	}
	inline explicit TemplatedValidityMask(idx_t max_count) {
		Initialize(max_count);
	}
	inline explicit TemplatedValidityMask(V *ptr) : validity_mask(ptr) {
	}
	inline TemplatedValidityMask(const TemplatedValidityMask &original, idx_t count) {
		Copy(original, count);
	}

	static inline idx_t ValidityMaskSize(idx_t count = STANDARD_VECTOR_SIZE) {
		return ValidityBuffer::EntryCount(count) * sizeof(V);
	}
	inline bool AllValid() const {
		return !validity_mask;
	}
	inline bool CheckAllValid(idx_t count) const {
		return CountValid(count) == count;
	}

	inline bool CheckAllValid(idx_t to, idx_t from) const {
		if (AllValid()) {
			return true;
		}
		for (idx_t i = from; i < to; i++) {
			if (!RowIsValid(i)) {
				return false;
			}
		}
		return true;
	}

	idx_t CountValid(const idx_t count) const {
		if (AllValid() || count == 0) {
			return count;
		}

		idx_t valid = 0;
		const auto entry_count = EntryCount(count);
		for (idx_t entry_idx = 0; entry_idx < entry_count;) {
			auto entry = GetValidityEntry(entry_idx++);
			// Handle ragged end (if not exactly multiple of BITS_PER_VALUE)
			if (entry_idx == entry_count && count % BITS_PER_VALUE != 0) {
				idx_t idx_in_entry;
				GetEntryIndex(count, entry_idx, idx_in_entry);
				for (idx_t i = 0; i < idx_in_entry; ++i) {
					valid += idx_t(RowIsValid(entry, i));
				}
				break;
			}

			// Handle all set
			if (AllValid(entry)) {
				valid += BITS_PER_VALUE;
				continue;
			}

			// Count partial entry (Kernighan's algorithm)
			while (entry) {
				entry &= (entry - 1);
				++valid;
			}
		}

		return valid;
	}

	inline V *GetData() const {
		return validity_mask;
	}
	inline void Reset() {
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
	//! Get an entry that has first-n bits set as valid and rest set as invalid
	static inline V EntryWithValidBits(idx_t n) {
		if (n == 0) {
			return V(0);
		}
		return ValidityBuffer::MAX_ENTRY >> (BITS_PER_VALUE - n);
	}
	static inline idx_t SizeInBytes(idx_t n) {
		return (n + BITS_PER_VALUE - 1) / BITS_PER_VALUE;
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

	//! Mark the entry at the specified index as either valid or invalid (non-null or null)
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

	//! Marks exactly "count" bits in the validity mask as invalid (null)
	inline void SetAllInvalid(idx_t count) {
		EnsureWritable();
		if (count == 0) {
			return;
		}
		auto last_entry_index = ValidityBuffer::EntryCount(count) - 1;
		for (idx_t i = 0; i < last_entry_index; i++) {
			validity_mask[i] = 0;
		}
		auto last_entry_bits = count % static_cast<idx_t>(BITS_PER_VALUE);
		validity_mask[last_entry_index] = (last_entry_bits == 0) ? 0 : (ValidityBuffer::MAX_ENTRY << (last_entry_bits));
	}

	//! Marks exactly "count" bits in the validity mask as valid (not null)
	inline void SetAllValid(idx_t count) {
		EnsureWritable();
		if (count == 0) {
			return;
		}
		auto last_entry_index = ValidityBuffer::EntryCount(count) - 1;
		for (idx_t i = 0; i < last_entry_index; i++) {
			validity_mask[i] = ValidityBuffer::MAX_ENTRY;
		}
		auto last_entry_bits = count % static_cast<idx_t>(BITS_PER_VALUE);
		validity_mask[last_entry_index] |=
		    (last_entry_bits == 0) ? ValidityBuffer::MAX_ENTRY : ~(ValidityBuffer::MAX_ENTRY << (last_entry_bits));
	}

	inline bool IsMaskSet() const {
		if (validity_mask) {
			return true;
		}
		return false;
	}

public:
	inline void Initialize(validity_t *validity) {
		validity_data.reset();
		validity_mask = validity;
	}
	inline void Initialize(const TemplatedValidityMask &other) {
		validity_mask = other.validity_mask;
		validity_data = other.validity_data;
	}
	inline void Initialize(idx_t count = STANDARD_VECTOR_SIZE) {
		validity_data = make_buffer<ValidityBuffer>(count);
		validity_mask = validity_data->owned_data.get();
	}
	inline void Copy(const TemplatedValidityMask &other, idx_t count) {
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
	inline ValidityMask() : TemplatedValidityMask(nullptr) {
	}
	inline explicit ValidityMask(idx_t max_count) : TemplatedValidityMask(max_count) {
	}
	inline explicit ValidityMask(validity_t *ptr) : TemplatedValidityMask(ptr) {
	}
	inline ValidityMask(const ValidityMask &original, idx_t count) : TemplatedValidityMask(original, count) {
	}

public:
	DUCKDB_API void Resize(idx_t old_size, idx_t new_size);

	DUCKDB_API void SliceInPlace(const ValidityMask &other, idx_t target_offset, idx_t source_offset, idx_t count);
	DUCKDB_API void Slice(const ValidityMask &other, idx_t source_offset, idx_t count);
	DUCKDB_API void Combine(const ValidityMask &other, idx_t count);
	DUCKDB_API string ToString(idx_t count) const;

	DUCKDB_API static bool IsAligned(idx_t count);

	void Write(WriteStream &writer, idx_t count);
	void Read(ReadStream &reader, idx_t count);
};

} // namespace duckdb
