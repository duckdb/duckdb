//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/table_filter_functions
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "table_filter_function_helpers.hpp"

#include "duckdb/common/exception/binder_exception.hpp"

namespace duckdb {

// LCOV_EXCL_START
unique_ptr<FunctionData> TableFilterFunctions::Bind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {
	throw BinderException("Table filter functions are for internal use only!");
}

bool TableFilterFunctions::IsTableFilterFunction(const string &name) {
	static const char *const TABLE_FILTER_FUNCTIONS[] = {
	    BloomFilterScalarFun::NAME,     DynamicFilterScalarFun::NAME, OptionalFilterScalarFun::NAME,
	    PerfectHashJoinScalarFun::NAME, PrefixRangeScalarFun::NAME,   SelectivityOptionalFilterScalarFun::NAME};
	for (auto function_name : TABLE_FILTER_FUNCTIONS) {
		if (name == function_name) {
			return true;
		}
	}
	return false;
}
// LCOV_EXCL_STOP

void GetThresholdAndVectorsToCheck(SelectivityOptionalFilterType type, float &selectivity_threshold,
                                   idx_t &n_vectors_to_check) {
	static constexpr float MIN_MAX_THRESHOLD = 0.9f;
	static constexpr float BF_THRESHOLD = 0.5f;
	static constexpr float PHJ_THRESHOLD = 0.3f;
	static constexpr float PRF_THRESHOLD = 0.5f;

	static constexpr idx_t MIN_MAX_CHECK_N = 6;
	static constexpr idx_t BF_CHECK_N = 6;
	static constexpr idx_t PHJ_CHECK_N = 6;
	static constexpr idx_t PRF_CHECK_N = 6;

	switch (type) {
	case SelectivityOptionalFilterType::MIN_MAX:
		selectivity_threshold = MIN_MAX_THRESHOLD;
		n_vectors_to_check = MIN_MAX_CHECK_N;
		return;
	case SelectivityOptionalFilterType::BF:
		selectivity_threshold = BF_THRESHOLD;
		n_vectors_to_check = BF_CHECK_N;
		return;
	case SelectivityOptionalFilterType::PHJ:
		selectivity_threshold = PHJ_THRESHOLD;
		n_vectors_to_check = PHJ_CHECK_N;
		return;
	case SelectivityOptionalFilterType::PRF:
		selectivity_threshold = PRF_THRESHOLD;
		n_vectors_to_check = PRF_CHECK_N;
		return;
	default:
		throw NotImplementedException("GetThresholdAndVectorsToCheck");
	}
}

void TableFilterFunctionSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                  const ScalarFunction &function) {
	// Runtime state cannot be serialized - write nothing
}

unique_ptr<FunctionData> TableFilterFunctionDeserialize(Deserializer &deserializer, ScalarFunction &function) {
	auto key_type = function.arguments.empty() ? LogicalType::ANY : function.arguments[0];
	if (function.name == BloomFilterScalarFun::NAME) {
		return make_uniq<BloomFilterFunctionData>(nullptr, false, string(), key_type, 0.0f, idx_t(0));
	}
	if (function.name == PerfectHashJoinScalarFun::NAME) {
		return make_uniq<PerfectHashJoinFunctionData>(nullptr, string(), 0.0f, idx_t(0));
	}
	if (function.name == PrefixRangeScalarFun::NAME) {
		return make_uniq<PrefixRangeFunctionData>(nullptr, string(), key_type, 0.0f, idx_t(0));
	}
	if (function.name == DynamicFilterScalarFun::NAME) {
		return make_uniq<DynamicFilterFunctionData>(nullptr);
	}
	throw InternalException("Unsupported table filter function \"%s\" during deserialization", function.name);
}

} // namespace duckdb
