#include "duckdb/common/vector.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb_vector_exceptions {

void ThrowIndexOutOfBoundsException(duckdb::idx_t index, duckdb::idx_t size) {
	throw duckdb::InternalException("Attempted to access index %ld within vector of size %ld", index, size);
}

void ThrowBackOnEmptyVectorException() {
	throw duckdb::InternalException("'back' called on an empty vector!");
}

void ThrowEraseAtException(duckdb::idx_t index, duckdb::idx_t size) {
	throw duckdb::InternalException("Can't remove offset %d from vector of size %d", index, size);
}

} // namespace duckdb_vector_exceptions
