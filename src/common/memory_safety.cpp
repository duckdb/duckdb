#include "duckdb/common/memory_safety.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

void ThrowNullDereference(const char *pointer_type) {
	throw InternalException("Attempted to dereference %s that is NULL!", pointer_type);
}

void ThrowOptionalPointerNotSet() {
	throw InternalException("Attempting to dereference an optional pointer that is not set");
}

void ThrowIndexOutOfBounds(const char *container, idx_t index, idx_t size) {
	throw InternalException("Attempted to access index %lld within %s of size %lld", index, container, size);
}

void ThrowEmptyContainer(const char *operation, const char *container) {
	throw InternalException("'%s' called on an empty %s!", operation, container);
}

void ThrowVectorError(const char *message) {
	throw InternalException(message);
}

} // namespace duckdb
