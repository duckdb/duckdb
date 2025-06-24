#pragma once

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

} // namespace duckdb
