//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/table_filter_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/scalar/tablefilter_functions.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

class BaseStatistics;
class Expression;
class PerfectHashJoinExecutor;
class BloomFilter;
class PrefixRangeFilter;
struct DynamicFilterData;
enum class SelectivityOptionalFilterType : uint8_t;

void GetThresholdAndVectorsToCheck(SelectivityOptionalFilterType type, float &selectivity_threshold,
                                   idx_t &n_vectors_to_check);

unique_ptr<Expression> CreateOptionalFilterExpression(unique_ptr<Expression> child_expr,
                                                      const LogicalType &target_type);
unique_ptr<Expression> CreateSelectivityOptionalFilterExpression(unique_ptr<Expression> child_expr,
                                                                 const LogicalType &target_type,
                                                                 float selectivity_threshold, idx_t n_vectors_to_check);
unique_ptr<Expression> CreateDynamicFilterExpression(shared_ptr<DynamicFilterData> filter_data,
                                                     const LogicalType &target_type);

//! Bind function that prevents user access to internal tablefilter functions
struct TableFilterFunctions {
	static unique_ptr<FunctionData> Bind(BindScalarFunctionInput &input);
	static bool IsTableFilterFunction(const string &name);
	static bool IsTableFilterFunction(const ScalarFunction &function) {
		return IsTableFilterFunction(function.name);
	}
};

//! FunctionData for bloom filter internal function
struct BloomFilterFunctionData : public FunctionData {
	BloomFilterFunctionData(optional_ptr<BloomFilter> filter_p, bool filters_null_values_p,
	                        const string &key_column_name_p, const LogicalType &key_type_p,
	                        float selectivity_threshold_p, idx_t n_vectors_to_check_p);

	optional_ptr<BloomFilter> filter;
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

//! Runtime dynamic-filter state shared by internal tablefilter functions.
struct DynamicFilterData {
public:
	DynamicFilterData(ExpressionType comparison_type, Value constant);

	mutex lock;
	ExpressionType comparison_type;
	Value constant;
	atomic<bool> initialized = {false};

	unique_ptr<Expression> ToExpression(const Expression &column) const;

	void SetValue(Value val);
	void Reset();
	static bool CompareValue(ExpressionType comparison_type, const Value &constant, const Value &value);
	static FilterPropagateResult CheckStatistics(BaseStatistics &stats, ExpressionType comparison_type,
	                                             const Value &constant);
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
struct BloomFilterScalarFun : public TableFilterBloomFilterFun {
	using TableFilterBloomFilterFun::GetFunction;
	static constexpr const char *NAME = TableFilterBloomFilterFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
	static string ToString(const string &column_name, const string &key_column_name);
};

//! Factory for perfect hash join internal function
struct PerfectHashJoinScalarFun : public TableFilterPerfectHashJoinFun {
	using TableFilterPerfectHashJoinFun::GetFunction;
	static constexpr const char *NAME = TableFilterPerfectHashJoinFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
	static string ToString(const string &column_name, const string &key_column_name);
};

//! Factory for prefix range internal function
struct PrefixRangeScalarFun : public TableFilterPrefixRangeFun {
	using TableFilterPrefixRangeFun::GetFunction;
	static constexpr const char *NAME = TableFilterPrefixRangeFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
	static string ToString(const string &column_name, const string &key_column_name);
};

//! Factory for dynamic filter internal function
struct DynamicFilterScalarFun : public TableFilterDynamicFun {
	using TableFilterDynamicFun::GetFunction;
	static constexpr const char *NAME = TableFilterDynamicFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
	static string ToString(const string &column_name, bool has_filter_data);
};

//! Factory for optional filter internal function (always returns TRUE)
struct OptionalFilterScalarFun : public TableFilterOptionalFun {
	using TableFilterOptionalFun::GetFunction;
	static constexpr const char *NAME = TableFilterOptionalFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
	static string ToString(const string &child_filter_string);
};

//! Factory for selectivity-optional filter internal function
struct SelectivityOptionalFilterScalarFun : public TableFilterSelectivityOptionalFun {
	using TableFilterSelectivityOptionalFun::GetFunction;
	static constexpr const char *NAME = TableFilterSelectivityOptionalFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
	static string ToString(const string &child_filter_string);
};

} // namespace duckdb
