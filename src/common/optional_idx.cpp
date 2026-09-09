#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

void optional_idx::ThrowInvalidInitialization() {
	throw InternalException("optional_idx cannot be initialized with an invalid index");
}

void optional_idx::ThrowNotSet() {
	throw InternalException("Attempting to get the index of an optional_idx that is not set");
}

} // namespace duckdb
