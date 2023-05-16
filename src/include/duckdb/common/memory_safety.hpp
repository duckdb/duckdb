#pragma once

namespace duckdb {

template <bool ENABLED>
struct MemorySafety {
#ifdef DEBUG
	// In DEBUG mode safety is always on
	static constexpr bool enabled = true;
#else
	static constexpr bool enabled = ENABLED;
#endif
};

} // namespace duckdb
