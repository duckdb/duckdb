//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/sort_key_prefix.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

//! Temporarily reuse a range's shared inline word, restoring canonical keys on scope exit.
template <class ITERATOR>
class SortKeyPrefixScope {
public:
	SortKeyPrefixScope(const ITERATOR &begin_p, const ITERATOR &end_p)
	    : begin(begin_p), end(end_p), original_part0((*begin).part0) {
	}

	SortKeyPrefixScope(const SortKeyPrefixScope &) = delete;
	SortKeyPrefixScope &operator=(const SortKeyPrefixScope &) = delete;

	~SortKeyPrefixScope() {
		for (auto it = begin; it != end; ++it) {
			(*it).part0 = original_part0;
		}
	}

	template <class SORT_KEY>
	void Set(SORT_KEY &key, uint64_t word) const {
		D_ASSERT(key.part0 == original_part0);
		key.part0 = word;
	}

private:
	ITERATOR begin;
	ITERATOR end;
	const uint64_t original_part0;
};

} // namespace duckdb
