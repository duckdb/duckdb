#include "duckdb/common/vector.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb_vector_exceptions {

void ThrowIndexOutOfBoundsException(idx_t index, idx_t size) {
	throw InternalException("Attempted to access index %ld within vector of size %ld", index, size);
}

void ThrowBackOnEmptyVectorException() {
	throw InternalException("'back' called on an empty vector!");
}

void ThrowEraseAtException(idx_t index, idx_t size) {
	throw InternalException("Can't remove offset %d from vector of size %d", index, size);
}

} // duckdb_vector_exceptions
