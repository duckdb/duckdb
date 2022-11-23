#include "duckdb/common/limits.hpp"

#include <limits>

namespace duckdb {

using std::numeric_limits;

hugeint_t NumericLimits<hugeint_t>::Minimum() {
	hugeint_t result;
	result.lower = 1;
	result.upper = numeric_limits<int64_t>::lowest();
	return result;
}

hugeint_t NumericLimits<hugeint_t>::Maximum() {
	hugeint_t result;
	result.lower = numeric_limits<uint64_t>::max();
	result.upper = numeric_limits<int64_t>::max();
	return result;
}

} // namespace duckdb
