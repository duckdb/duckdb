#include "duckdb/common/chrono.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/windows_util.hpp"

namespace duckdb {

// If we fail to read a timestamp (for whatever reasion) this returns zero.
int64_t GetCoarseMonotonicMillisecondTimestamp() {
#ifdef DUCKDB_WINDOWS
	return WindowsUtil::GetTickCount();
#else
#ifdef CLOCK_MONOTONIC_COARSE
	constexpr clockid_t clock = CLOCK_MONOTONIC_COARSE;
#else
	constexpr clockid_t clock = CLOCK_MONOTONIC;
#endif
	struct timespec t;
	if (int ret = clock_gettime(clock, &t)) {
		D_ASSERT(ret == 0);
		(void)ret;
		return 0;
	} else {
		return (int64_t)(1000 * t.tv_sec) + (t.tv_nsec >> 20);
	}
#endif
}

} // namespace duckdb
