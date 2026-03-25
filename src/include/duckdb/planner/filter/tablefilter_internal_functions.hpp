//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/tablefilter_internal_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"

namespace duckdb {

class BloomFilter;
class PerfectHashJoinExecutor;
class PrefixRangeFilter;
struct DynamicFilterData;

//! Bind function that prevents user access to internal tablefilter functions
struct TableFilterInternalFunctions {
	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments);
};

//! FunctionData for bloom filter internal function
struct BloomFilterFunctionData : public FunctionData {
	BloomFilterFunctionData(BloomFilter &filter_p, bool filters_null_values_p, const string &key_column_name_p,
	                        const LogicalType &key_type_p, float selectivity_threshold_p, idx_t n_vectors_to_check_p);

	BloomFilter &filter;
	bool filters_null_values;
	string key_column_name;
	LogicalType key_type;
	float selectivity_threshold;
	idx_t n_vectors_to_check;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

//! FunctionData for perfect hash join internal function
struct PerfectHashJoinFunctionData : public FunctionData {
	PerfectHashJoinFunctionData(optional_ptr<const PerfectHashJoinExecutor> executor_p, const string &key_column_name_p,
	                            float selectivity_threshold_p, idx_t n_vectors_to_check_p);

	optional_ptr<const PerfectHashJoinExecutor> executor;
	string key_column_name;
	float selectivity_threshold;
	idx_t n_vectors_to_check;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

//! FunctionData for prefix range internal function
struct PrefixRangeFunctionData : public FunctionData {
	PrefixRangeFunctionData(optional_ptr<PrefixRangeFilter> filter_p, const string &key_column_name_p,
	                        const LogicalType &key_type_p, float selectivity_threshold_p, idx_t n_vectors_to_check_p);

	optional_ptr<PrefixRangeFilter> filter;
	string key_column_name;
	LogicalType key_type;
	float selectivity_threshold;
	idx_t n_vectors_to_check;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

//! FunctionData for dynamic filter internal function
struct DynamicFilterFunctionData : public FunctionData {
	explicit DynamicFilterFunctionData(shared_ptr<DynamicFilterData> filter_data_p);

	shared_ptr<DynamicFilterData> filter_data;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

//! FunctionData for optional filter internal function (always returns TRUE, used for statistics only)
struct OptionalFilterFunctionData : public FunctionData {
	//! The child filter expression, used for CheckStatistics via FilterPruneCallback
	unique_ptr<Expression> child_filter_expr;

	explicit OptionalFilterFunctionData(unique_ptr<Expression> child_filter_expr_p);

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

//! FunctionData for selectivity-optional filter internal function
struct SelectivityOptionalFilterFunctionData : public FunctionData {
	//! The child filter expression, executed only while it remains selective enough
	unique_ptr<Expression> child_filter_expr;
	float selectivity_threshold;
	idx_t n_vectors_to_check;

	SelectivityOptionalFilterFunctionData(unique_ptr<Expression> child_filter_expr_p, float selectivity_threshold_p,
	                                      idx_t n_vectors_to_check_p);

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

//! Factory for bloom filter internal function
struct BloomFilterScalarFun {
	static constexpr const char *NAME = "__internal_tablefilter_bloom_filter";
	static constexpr const char *Name = "__internal_tablefilter_bloom_filter";
	static constexpr const char *Parameters = "";
	static constexpr const char *Description = "";
	static constexpr const char *Example = "";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
};

//! Factory for perfect hash join internal function
struct PerfectHashJoinScalarFun {
	static constexpr const char *NAME = "__internal_tablefilter_perfect_hash_join";
	static constexpr const char *Name = "__internal_tablefilter_perfect_hash_join";
	static constexpr const char *Parameters = "";
	static constexpr const char *Description = "";
	static constexpr const char *Example = "";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
};

//! Factory for prefix range internal function
struct PrefixRangeScalarFun {
	static constexpr const char *NAME = "__internal_tablefilter_prefix_range";
	static constexpr const char *Name = "__internal_tablefilter_prefix_range";
	static constexpr const char *Parameters = "";
	static constexpr const char *Description = "";
	static constexpr const char *Example = "";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
};

//! Factory for dynamic filter internal function
struct DynamicFilterScalarFun {
	static constexpr const char *NAME = "__internal_tablefilter_dynamic";
	static constexpr const char *Name = "__internal_tablefilter_dynamic";
	static constexpr const char *Parameters = "";
	static constexpr const char *Description = "";
	static constexpr const char *Example = "";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
};

//! Factory for optional filter internal function (always returns TRUE)
struct OptionalFilterScalarFun {
	static constexpr const char *NAME = "__internal_tablefilter_optional";
	static constexpr const char *Name = "__internal_tablefilter_optional";
	static constexpr const char *Parameters = "";
	static constexpr const char *Description = "";
	static constexpr const char *Example = "";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
};

//! Factory for selectivity-optional filter internal function
struct SelectivityOptionalFilterScalarFun {
	static constexpr const char *NAME = "__internal_tablefilter_selectivity_optional";
	static constexpr const char *Name = "__internal_tablefilter_selectivity_optional";
	static constexpr const char *Parameters = "";
	static constexpr const char *Description = "";
	static constexpr const char *Example = "";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
};

} // namespace duckdb
