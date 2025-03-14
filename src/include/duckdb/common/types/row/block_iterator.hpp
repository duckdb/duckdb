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

class fixed_in_memory_block_iterator_state_t { // NOLINT: not using camelcase on purpose here
public:
	explicit fixed_in_memory_block_iterator_state_t(const TupleDataCollection &data);

public:
	template <class T>
	T &GetValueAtIndex(const idx_t &n) const {
		D_ASSERT(n < tuple_count);
		const auto quotient = fast_mod.Div(n);
		return reinterpret_cast<T *const>(block_ptrs[quotient])[fast_mod.Mod(n, quotient)];
	}

private:
	static unsafe_vector<const data_ptr_t> ConvertBlockPointers(const vector<data_ptr_t> &block_ptrs);

private:
	const unsafe_vector<const data_ptr_t> block_ptrs;
	const FastMod<idx_t> fast_mod;
	const idx_t tuple_count;
};

class variable_in_memory_block_iterator_state_t { // NOLINT: not using camelcase on purpose here
};

class fixed_external_block_iterator_state_t { // NOLINT: not using camelcase on purpose here
};

class variable_external_block_iterator_state_t { // NOLINT: not using camelcase on purpose here
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
class block_iterator_t { // NOLINT: not using camelcase on purpose here
public:
	using iterator_category = std::random_access_iterator_tag;
	using value_type = T;
	using pointer = value_type *;
	using reference = value_type &;
	using difference_type = idx_t;
	using traits = std::iterator_traits<block_iterator_t>;

public:
	explicit block_iterator_t(STATE &state_p) : state(state_p), index(0) {
	}

	block_iterator_t(STATE &state_p, const difference_type &index_p) : state(state_p), index(index_p) {
	}

	block_iterator_t(const block_iterator_t &other) : state(other.state), index(other.index) {
	}

	block_iterator_t &operator=(const block_iterator_t &other) {
		D_ASSERT(RefersToSameObject(state, other.state));
		index = other.index;
		return *this;
	}

public:
	//! (De-)referencing
	reference operator*() const {
		return operator[](index);
	}
	pointer operator->() const {
		return &operator*();
	}

	//! Prefix and postfix increment and decrement
	block_iterator_t &operator++() {
		index++;
		return *this;
	}
	block_iterator_t operator++(int) {
		block_iterator_t tmp = *this;
		++(*this);
		return tmp;
	}
	block_iterator_t &operator--() {
		index--;
		return *this;
	}
	block_iterator_t operator--(int) {
		block_iterator_t tmp = *this;
		--(*this);
		return tmp;
	}

	//! Random access
	block_iterator_t &operator+=(const difference_type &n) {
		index += n;
		return *this;
	}
	block_iterator_t &operator-=(const difference_type &n) {
		D_ASSERT(index >= n);
		index -= n;
		return *this;
	}
	block_iterator_t operator+(const difference_type &n) const {
		return block_iterator_t(state, index + n);
	}
	block_iterator_t operator-(const difference_type &n) const {
		D_ASSERT(index >= n);
		return block_iterator_t(state, index - n);
	}

	reference operator[](const difference_type &n) const {
		return state.template GetValueAtIndex<T>(n);
	}

	//! Difference between iterators
	difference_type operator-(const block_iterator_t &other) const {
		D_ASSERT(index >= other.index);
		return index - other.index;
	}

	//! Comparison operators
	bool operator==(const block_iterator_t &other) const {
		return index == other.index;
	}
	bool operator!=(const block_iterator_t &other) const {
		return index != other.index;
	}
	bool operator<(const block_iterator_t &other) const {
		return index < other.index;
	}
	bool operator>(const block_iterator_t &other) const {
		return index > other.index;
	}
	bool operator<=(const block_iterator_t &other) const {
		return index <= other.index;
	}
	bool operator>=(const block_iterator_t &other) const {
		return index >= other.index;
	}

private:
	STATE &state;
	difference_type index;
};

} // namespace duckdb
