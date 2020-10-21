#include "duckdb/storage/table/numeric_statistics.hpp"

namespace duckdb {

template<>
string NumericStatistics<hugeint_t>::ToString() {
	return StringUtil::Format("Numeric Statistics<hugeint_t> [Has Null: %s, Min: %s, Max: %s]",
		has_null ? "true" : "false", min.ToString(), max.ToString());
}

template<>
string NumericStatistics<float>::ToString() {
	return StringUtil::Format("Numeric Statistics<float> [Has Null: %s, Min: %lf, Max: %lf]",
		has_null ? "true" : "false", min, max);
}

template<>
string NumericStatistics<double>::ToString() {
	return StringUtil::Format("Numeric Statistics<double> [Has Null: %s, Min: %lf, Max: %lf]",
		has_null ? "true" : "false", min, max);
}

}