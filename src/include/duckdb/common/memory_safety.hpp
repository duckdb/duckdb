#pragma once

#include "duckdb/common/winapi.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

template <bool IS_ENABLED>
struct MemorySafety {
#ifdef DEBUG
	// In DEBUG mode safety is always on
	static constexpr bool ENABLED = true;
#else
	static constexpr bool ENABLED = IS_ENABLED;
#endif
};

//! Reporters for a violated safety check. These are what the inline pointer and container accessors call when their
//! check fails. Building the exception - not the throw - is what pushes an accessor past the inliner's budget, so it
//! is kept out-of-line here and the accessors stay small enough to inline into their callers.
[[noreturn]] DUCKDB_API void ThrowNullDereference(const char *pointer_type);
[[noreturn]] DUCKDB_API void ThrowOptionalPointerNotSet();
[[noreturn]] DUCKDB_API void ThrowIndexOutOfBounds(const char *container, idx_t index, idx_t size);
[[noreturn]] DUCKDB_API void ThrowEmptyContainer(const char *operation, const char *container);
[[noreturn]] DUCKDB_API void ThrowVectorError(const char *message);

} // namespace duckdb
