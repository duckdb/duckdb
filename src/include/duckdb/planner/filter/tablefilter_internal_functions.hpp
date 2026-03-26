//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/tablefilter_internal_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/function/scalar/tablefilter_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

class BaseStatistics;
class PerfectHashJoinExecutor;
class PrefixRangeFilter;

class BloomFilter {
public:
	BloomFilter() = default;
	void Initialize(ClientContext &context_p, idx_t number_of_rows);

	void InsertHashes(const Vector &hashes_v, idx_t count) const;
	idx_t LookupHashes(const Vector &hashes_v, SelectionVector &result_sel, idx_t count) const;

	void InsertOne(hash_t hash) const;
	bool LookupOne(hash_t hash) const;

	bool IsInitialized() const {
		return initialized;
	}

private:
	idx_t num_sectors;
	uint64_t bitmask; // num_sectors - 1 -> used to get the sector offset

	bool initialized = false;
	AllocatedData buf_;
	uint64_t *bf;
};

struct DynamicFilterData {
	DynamicFilterData() : comparison_type(ExpressionType::INVALID) {
	}
	DynamicFilterData(ExpressionType comparison_type_p, Value constant_p);

	mutex lock;
	ExpressionType comparison_type;
	Value constant;
	atomic<bool> initialized = {false};

	void SetValue(Value val);
	void Reset();
	static bool CompareValue(ExpressionType comparison_type, const Value &constant, const Value &value);
	static FilterPropagateResult CheckStatistics(BaseStatistics &stats, ExpressionType comparison_type,
	                                             const Value &constant);
};

//! Bind function that prevents user access to internal tablefilter functions
struct TableFilterInternalFunctions {
	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments);
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
struct BloomFilterScalarFun : public InternalTableFilterBloomFilterFun {
	using InternalTableFilterBloomFilterFun::GetFunction;
	static constexpr const char *NAME = InternalTableFilterBloomFilterFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
};

//! Factory for perfect hash join internal function
struct PerfectHashJoinScalarFun : public InternalTableFilterPerfectHashJoinFun {
	using InternalTableFilterPerfectHashJoinFun::GetFunction;
	static constexpr const char *NAME = InternalTableFilterPerfectHashJoinFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
};

//! Factory for prefix range internal function
struct PrefixRangeScalarFun : public InternalTableFilterPrefixRangeFun {
	using InternalTableFilterPrefixRangeFun::GetFunction;
	static constexpr const char *NAME = InternalTableFilterPrefixRangeFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
};

//! Factory for dynamic filter internal function
struct DynamicFilterScalarFun : public InternalTableFilterDynamicFun {
	using InternalTableFilterDynamicFun::GetFunction;
	static constexpr const char *NAME = InternalTableFilterDynamicFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
};

//! Factory for optional filter internal function (always returns TRUE)
struct OptionalFilterScalarFun : public InternalTableFilterOptionalFun {
	using InternalTableFilterOptionalFun::GetFunction;
	static constexpr const char *NAME = InternalTableFilterOptionalFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
};

//! Factory for selectivity-optional filter internal function
struct SelectivityOptionalFilterScalarFun : public InternalTableFilterSelectivityOptionalFun {
	using InternalTableFilterSelectivityOptionalFun::GetFunction;
	static constexpr const char *NAME = InternalTableFilterSelectivityOptionalFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
};

} // namespace duckdb
