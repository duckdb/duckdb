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
#include "duckdb/planner/table_filter_state.hpp"

namespace duckdb {

class BaseStatistics;
class Expression;
class PerfectHashJoinExecutor;
class PrefixRangeFilter;
struct DynamicFilterData;

struct SelectivityOptionalFilterState final : public TableFilterState {
	enum class FilterStatus { ACTIVE, PAUSED_DUE_TO_HIGH_SELECTIVITY };

	struct SelectivityStats {
		SelectivityStats(idx_t n_vectors_to_check, float selectivity_threshold);

		void Update(idx_t accepted, idx_t processed);
		bool IsActive() const;
		double GetSelectivity() const;

		//! Configuration
		const idx_t n_vectors_to_check;
		const float selectivity_threshold;

		//! For computing selectivity stats
		idx_t tuples_accepted;
		idx_t tuples_processed;
		idx_t vectors_processed;

		//! Whether currently paused
		FilterStatus status;

		//! For increasing pause if filter is not selective enough
		idx_t pause_multiplier;
	};

	unique_ptr<TableFilterState> child_state;
	SelectivityStats stats;

	explicit SelectivityOptionalFilterState(unique_ptr<TableFilterState> child_state, const idx_t n_vectors_to_check,
	                                        const float selectivity_threshold)
	    : child_state(std::move(child_state)), stats(n_vectors_to_check, selectivity_threshold) {
	}
};

enum class SelectivityOptionalFilterType : uint8_t { MIN_MAX, BF, PHJ, PRF };

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
	static bool IsTableFilterFunction(const BoundScalarFunction &function) {
		return IsTableFilterFunction(function.GetName());
	}
};

//! Runtime bloom-filter state used by join pushdown and internal tablefilter functions.
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

//! Runtime prefix-range filter state used by join pushdown and internal tablefilter functions.
class PrefixRangeFilter {
public:
	struct BuildState {
		virtual ~BuildState() = default;
		template <class TARGET>

		TARGET &Cast() {
			DynamicCastCheck<TARGET>(this);
			return reinterpret_cast<TARGET &>(*this);
		}
		template <class TARGET>
		const TARGET &Cast() const {
			DynamicCastCheck<TARGET>(this);
			return reinterpret_cast<const TARGET &>(*this);
		}
	};

	virtual ~PrefixRangeFilter() = default;
	virtual void Initialize(ClientContext &context, idx_t number_of_rows, Value min, Value max) = 0;
	virtual unique_ptr<BuildState> InitializeBuildState(ClientContext &context) const = 0;
	virtual void InsertKeys(Vector &keys, idx_t count, BuildState &state) const = 0;
	virtual void MergeBuildState(BuildState &state) = 0;
	virtual idx_t LookupKeys(Vector &keys, SelectionVector &result_sel, idx_t count) const = 0;
	virtual FilterPropagateResult LookupRange(const Value &lower_bound, const Value &upper_bound) const = 0;
	virtual bool IsInitialized() const = 0;
	static bool SupportedType(const LogicalType &type);
	static unique_ptr<PrefixRangeFilter> CreatePrefixRangeFilter(const LogicalType &key_type);
	static bool TryComputeSpan(const Value &lower_bound, const Value &upper_bound, uhugeint_t &result);
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
