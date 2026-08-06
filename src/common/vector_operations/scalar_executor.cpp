#include "duckdb/common/vector_operations/scalar_executor.hpp"

namespace duckdb {

bool ScalarExecutor::PrepareGenericResultValidity(const UnifiedVectorFormat *formats, idx_t format_count,
                                                  Vector &result, idx_t count, bool preserve_result_validity,
                                                  bool adds_nulls, ValidityMask &input_validity) {
	bool initialized = false;
	for (idx_t input_idx = 0; input_idx < format_count; input_idx++) {
		if (formats[input_idx].validity.CannotHaveNull()) {
			continue;
		}
		ValidityMask mapped(count);
		mapped.CopySel(formats[input_idx].validity, *formats[input_idx].sel, 0, 0, count);
		if (!initialized) {
			input_validity.Initialize(mapped);
			initialized = true;
		} else {
			input_validity.Combine(mapped, count);
		}
	}

	auto &result_validity = FlatVector::ValidityMutable(result);
	if (initialized) {
		result_validity.Initialize(input_validity);
	} else if (!preserve_result_validity) {
		result_validity.Reset(count);
	} else if (adds_nulls && result_validity.CanHaveNull()) {
		ValidityMask preserved(result_validity, count);
		result_validity.Initialize(preserved);
	}
	return initialized;
}

} // namespace duckdb
