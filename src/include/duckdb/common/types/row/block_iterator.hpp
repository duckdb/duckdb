//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/block_iterator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class TupleDataCollection;

enum class BlockIteratorStateType : int8_t {
	//! For fixed_in_memory_block_iterator_state_t
	FIXED_IN_MEMORY,
	//! For variable_in_memory_block_iterator_state_t
	VARIABLE_IN_MEMORY,
	//! For fixed_external_block_iterator_state_t
	FIXED_EXTERNAL,
	//! For variable_external_block_iterator_state_t
	VARIABLE_EXTERNAL,
};

BlockIteratorStateType GetBlockIteratorStateType(const bool &fixed_blocks, const bool &external);

class fixed_in_memory_block_iterator_state_t { // NOLINT: match stl case
public:
	explicit fixed_in_memory_block_iterator_state_t(const TupleDataCollection &data);

public:
	template <class T>
	T &GetValueAtIndex(const idx_t &block_idx, const idx_t &tuple_idx) const {
		return reinterpret_cast<T *const>(block_ptrs[block_idx])[tuple_idx];
	}

	template <class T>
	T &GetValueAtIndex(const idx_t &n) const {
		D_ASSERT(n < tuple_count);
		const auto quotient = fast_mod.Div(n);
		return reinterpret_cast<T *const>(block_ptrs[quotient])[fast_mod.Mod(n, quotient)];
	}

	void RandomAccess(idx_t &block_idx, idx_t &tuple_idx, const idx_t &index) const {
		block_idx = fast_mod.Div(index);
		tuple_idx = fast_mod.Mod(index, block_idx);
	}

	void Add(idx_t &block_idx, idx_t &tuple_idx, const idx_t &value) const {
		if (tuple_idx + value < fast_mod.GetDivisor()) {
			tuple_idx += value;
			return;
		}

		const auto index = GetIndex(block_idx, tuple_idx) + value;
		RandomAccess(block_idx, tuple_idx, index);
	}

	void Subtract(idx_t &block_idx, idx_t &tuple_idx, const idx_t &value) const {
		if (tuple_idx >= value) {
			tuple_idx -= value;
			return;
		}

		const auto index = GetIndex(block_idx, tuple_idx) - value;
		RandomAccess(block_idx, tuple_idx, index);
	}

	void Increment(idx_t &block_idx, idx_t &tuple_idx) const {
		if (++tuple_idx == fast_mod.GetDivisor()) {
			++block_idx;
			tuple_idx = 0;
		}
	}

	void Decrement(idx_t &block_idx, idx_t &tuple_idx) const {
		if (--tuple_idx == DConstants::INVALID_INDEX) {
			--block_idx;
			tuple_idx = fast_mod.GetDivisor() - 1;
		}
	}

	idx_t GetIndex(const idx_t &block_idx, const idx_t &tuple_idx) const {
		return block_idx * fast_mod.GetDivisor() + tuple_idx;
	}

private:
	static unsafe_vector<const data_ptr_t> ConvertBlockPointers(const vector<data_ptr_t> &block_ptrs);

private:
	const unsafe_vector<const data_ptr_t> block_ptrs;
	const FastMod<idx_t> fast_mod;
	const idx_t tuple_count;
};

class variable_in_memory_block_iterator_state_t { // NOLINT: match stl case
};

class fixed_external_block_iterator_state_t { // NOLINT: match stl case
};

class variable_external_block_iterator_state_t { // NOLINT: match stl case
};

//! Utility so we can get the state using the type
template <BlockIteratorStateType T>
using block_iterator_state_t = typename std::conditional<
    T == BlockIteratorStateType::FIXED_IN_MEMORY, fixed_in_memory_block_iterator_state_t,
    typename std::conditional<
        T == BlockIteratorStateType::VARIABLE_IN_MEMORY, variable_in_memory_block_iterator_state_t,
        typename std::conditional<T == BlockIteratorStateType::FIXED_EXTERNAL, fixed_external_block_iterator_state_t,
                                  typename std::conditional<T == BlockIteratorStateType::VARIABLE_EXTERNAL,
                                                            variable_external_block_iterator_state_t,
                                                            void // Throws error if we get here
                                                            >::type>::type>::type>::type;

//! Iterator for data spread out over multiple blocks
template <class STATE, class T>
class block_iterator_t { // NOLINT: match stl case
public:
	using iterator_category = std::random_access_iterator_tag;
	using value_type = T;
	using pointer = value_type *;
	using reference = value_type &;
	using difference_type = idx_t;
	using traits = std::iterator_traits<block_iterator_t>;

public:
	explicit block_iterator_t(STATE &state_p) : state(state_p), block_idx(0), tuple_idx(0) {
	}

	block_iterator_t(STATE &state_p, const idx_t &index) : state(state_p) {
		state.get().RandomAccess(block_idx, tuple_idx, index);
	}

	block_iterator_t(STATE &state_p, const idx_t &block_idx_p, const idx_t &tuple_idx_p)
	    : state(state_p), block_idx(block_idx_p), tuple_idx(tuple_idx_p) {
	}

	block_iterator_t(const block_iterator_t &other)
	    : state(other.state), block_idx(other.block_idx), tuple_idx(other.tuple_idx) {
	}

	block_iterator_t &operator=(const block_iterator_t &other) {
		D_ASSERT(RefersToSameObject(state, other.state));
		block_idx = other.block_idx;
		tuple_idx = other.tuple_idx;
		return *this;
	}

public:
	//! (De-)referencing
	reference operator*() const {
		return state.get().template GetValueAtIndex<T>(block_idx, tuple_idx);
	}
	pointer operator->() const {
		return &operator*();
	}

	//! Prefix and postfix increment and decrement
	block_iterator_t &operator++() {
		state.get().Increment(block_idx, tuple_idx);
		return *this;
	}
	block_iterator_t operator++(int) {
		block_iterator_t tmp = *this;
		++(*this);
		return tmp;
	}
	block_iterator_t &operator--() {
		state.get().Decrement(block_idx, tuple_idx);
		return *this;
	}
	block_iterator_t operator--(int) {
		block_iterator_t tmp = *this;
		--(*this);
		return tmp;
	}

	//! Random access
	block_iterator_t &operator+=(const difference_type &n) {
		state.get().Add(block_idx, tuple_idx, n);
		return *this;
	}
	block_iterator_t &operator-=(const difference_type &n) {
		state.get().Subtract(block_idx, tuple_idx, n);
		return *this;
	}
	block_iterator_t operator+(const difference_type &n) const {
		idx_t new_block_idx = block_idx;
		idx_t new_tuple_idx = tuple_idx;
		state.get().Add(new_block_idx, new_tuple_idx, n);
		return block_iterator_t(state, new_block_idx, new_tuple_idx);
	}
	block_iterator_t operator-(const difference_type &n) const {
		idx_t new_block_idx = block_idx;
		idx_t new_tuple_idx = tuple_idx;
		state.get().Subtract(new_block_idx, new_tuple_idx, n);
		return block_iterator_t(state, new_block_idx, new_tuple_idx);
	}

	reference operator[](const difference_type &n) const {
		return state.get().template GetValueAtIndex<T>(n);
	}

	//! Difference between iterators
	difference_type operator-(const block_iterator_t &other) const {
		return state.get().GetIndex(block_idx, tuple_idx) -
		       other.state.get().GetIndex(other.block_idx, other.tuple_idx);
	}

	//! Comparison operators
	bool operator==(const block_iterator_t &other) const {
		return block_idx == other.block_idx && tuple_idx == other.tuple_idx;
	}
	bool operator!=(const block_iterator_t &other) const {
		return block_idx != other.block_idx || tuple_idx != other.tuple_idx;
	}
	bool operator<(const block_iterator_t &other) const {
		return block_idx == other.block_idx ? tuple_idx < other.tuple_idx : block_idx < other.block_idx;
	}
	bool operator>(const block_iterator_t &other) const {
		return block_idx == other.block_idx ? tuple_idx > other.tuple_idx : block_idx > other.block_idx;
	}
	bool operator<=(const block_iterator_t &other) const {
		return block_idx == other.block_idx ? tuple_idx <= other.tuple_idx : block_idx <= other.block_idx;
	}
	bool operator>=(const block_iterator_t &other) const {
		return block_idx == other.block_idx ? tuple_idx >= other.tuple_idx : block_idx >= other.block_idx;
	}

private:
	std::reference_wrapper<STATE> state;
	idx_t block_idx;
	idx_t tuple_idx;
};

} // namespace duckdb
