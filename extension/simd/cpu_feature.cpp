#include "duckdb/common/cpu_feature.hpp"
namespace duckdb {

string CPUFeatureToString(CPUFeature feature) {
	switch (feature) {
	case CPUFeature::DUCKDB_CPU_FALLBACK:
		return "DUCKDB_CPU_FALLBACK";
	case CPUFeature::DUCKDB_CPU_FEATURE_X86_AVX2:
		return "DUCKDB_CPU_FEATURE_X86_AVX2";
	case CPUFeature::DUCKDB_CPU_FEATURE_X86_AVX512F:
		return "DUCKDB_CPU_FEATURE_X86_AVX512F";
	default:
		return "UNDEFINED";
	}
}

} // namespace duckdb