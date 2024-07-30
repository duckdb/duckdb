//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/ht_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

//! The ht_entry_t struct represents an individual entry within a hash table.
/*!
    This struct is used by the JoinHashTable and AggregateHashTable to store entries within the hash table. It stores
    a pointer to the data and a salt value in a single hash_t and can return or modify the pointer and salt
    individually.
*/
struct ht_entry_t { // NOLINT
public:
	//! Upper 16 bits are salt
	static constexpr const hash_t SALT_MASK = 0xFFFF000000000000;
	//! Lower 48 bits are the pointer
	static constexpr const hash_t POINTER_MASK = 0x0000FFFFFFFFFFFF;

	explicit inline ht_entry_t(hash_t value_p) noexcept : value(value_p) {
	}

	// Add a default constructor for 32-bit linux test case
	ht_entry_t() noexcept : value(0) {
	}

	inline bool IsOccupied() const {
		return value != 0;
	}

	// Returns a pointer based on the stored value without checking cell occupancy.
	// This can return a nullptr if the cell is not occupied.
	inline data_ptr_t GetPointerOrNull() const {
		return reinterpret_cast<data_ptr_t>(value & POINTER_MASK);
	}

	// Returns a pointer based on the stored value if the cell is occupied
	inline data_ptr_t GetPointer() const {
		D_ASSERT(IsOccupied());
		return reinterpret_cast<data_ptr_t>(value & POINTER_MASK);
	}

	inline void SetPointer(const data_ptr_t &pointer) {
		// Pointer shouldn't use upper bits
		D_ASSERT((reinterpret_cast<uint64_t>(pointer) & SALT_MASK) == 0);
		// Value should have all 1's in the pointer area
		D_ASSERT((value & POINTER_MASK) == POINTER_MASK);
		// Set upper bits to 1 in pointer so the salt stays intact
		value &= reinterpret_cast<uint64_t>(pointer) | SALT_MASK;
	}

	// Returns the salt, leaves upper salt bits intact, sets lower bits to all 1's
	static inline hash_t ExtractSalt(const hash_t &hash) {
		return hash | POINTER_MASK;
	}

	// Returns the salt, leaves upper salt bits intact, sets lower bits to all 0's
	static inline hash_t ExtractSaltWithNulls(const hash_t &hash) {
		return hash & SALT_MASK;
	}

	inline hash_t GetSalt() const {
		return ExtractSalt(value);
	}

	inline void SetSalt(const hash_t &salt) {
		// Shouldn't be occupied when we set this
		D_ASSERT(!IsOccupied());
		// Salt should have all 1's in the pointer field
		D_ASSERT((salt & POINTER_MASK) == POINTER_MASK);
		// No need to mask, just put the whole thing there
		value = salt;
	}

	static inline ht_entry_t GetDesiredEntry(const data_ptr_t &pointer, const hash_t &salt) {
		auto desired = reinterpret_cast<uint64_t>(pointer) | (salt & SALT_MASK);
		return ht_entry_t(desired);
	}

	static inline ht_entry_t GetEmptyEntry() {
		return ht_entry_t(0);
	}

private:
	hash_t value;
};

// uses an AND operation to apply the modulo operation instead of an if condition that could be branch mispredicted
inline void IncrementAndWrap(idx_t &offset, const uint64_t &capacity_mask) {
	++offset &= capacity_mask; // leave the salt bits unchanged
}

} // namespace duckdb
