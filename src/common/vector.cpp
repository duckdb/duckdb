#include "duckdb/common/exception.hpp"

namespace duckdb {

void AssertIndexInBounds(idx_t index, idx_t size) {
	if (index < size) {
		return;
	}
	throw InternalException("Attempted to access index %ld within vector of size %ld", index, size);
}

} // namespace duckdb
