//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/tablefilter_internal_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/function/scalar/tablefilter_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

class BaseStatistics;
class PerfectHashJoinExecutor;
struct BloomFilterFunctionData;
struct PerfectHashJoinFunctionData;
struct PrefixRangeFunctionData;

enum class SelectivityOptionalFilterType : uint8_t { MIN_MAX, BF, PHJ, PRF };
void GetThresholdAndVectorsToCheck(SelectivityOptionalFilterType type, float &selectivity_threshold,
                                   idx_t &n_vectors_to_check);

class BloomFilter {
public:
	BloomFilter() = default;
	void Initialize(ClientContext &context_p, idx_t number_of_rows);

	void InsertHashes(const Vector &hashes_v, idx_t count) const;
	idx_t LookupHashes(const Vector &hashes_v, SelectionVector &result_sel, idx_t count) const;

	void InsertOne(hash_t hash) const {
		D_ASSERT(initialized);
		const uint64_t bf_offset = hash & bitmask;
		const uint64_t mask = GetMask(hash);
		atomic<uint64_t> &slot = *reinterpret_cast<atomic<uint64_t> *>(&bf[bf_offset]);
		slot.fetch_or(mask, std::memory_order_relaxed);
	}

	bool LookupOne(hash_t hash) const {
		D_ASSERT(initialized);
		const uint64_t bf_offset = hash & bitmask;
		const uint64_t mask = GetMask(hash);
		atomic<uint64_t> &slot = *reinterpret_cast<atomic<uint64_t> *>(&bf[bf_offset]);
		auto bf_entry = slot.load(std::memory_order_relaxed);
		return (bf_entry & mask) == mask;
	}

	bool IsInitialized() const {
		return initialized;
	}

private:
	static constexpr idx_t LOG_SECTOR_SIZE = 6;             // a sector is 64 bits, log2(64) = 6
	static constexpr idx_t SHIFT_MASK = 0x3F3F3F3F3F3F3F3F; // 6 bits for 64 positions
	static constexpr idx_t N_BITS = 4;                      // the number of bits to set per hash

	static uint64_t GetMask(hash_t hash) {
		const uint64_t shifts = hash & SHIFT_MASK;
		const auto shifts_8 = reinterpret_cast<const uint8_t *>(&shifts);

		uint64_t mask = 0;
		for (idx_t bit_idx = 8 - N_BITS; bit_idx < 8; bit_idx++) {
			const uint8_t bit_pos = shifts_8[bit_idx];
			mask |= (1ULL << bit_pos);
		}
		return mask;
	}

	idx_t num_sectors;
	uint64_t bitmask; // num_sectors - 1 -> used to get the sector offset

	bool initialized = false;
	AllocatedData buf_;
	uint64_t *bf;
};

class PrefixRangeFilter {
public:
	struct BuildState {
		virtual ~BuildState() = default;
	};

	virtual ~PrefixRangeFilter() = default;
	virtual void Initialize(ClientContext &context, idx_t number_of_rows, Value min, Value max) = 0;
	virtual unique_ptr<BuildState> InitializeBuildState(ClientContext &context) const = 0;
	virtual void InsertKeys(Vector &keys, idx_t count, BuildState &state) const = 0;
	virtual void MergeBuildState(BuildState &state) = 0;
	virtual idx_t LookupKeys(Vector &keys, SelectionVector &result_sel, idx_t count) const = 0;
	virtual bool LookupOneValue(const Value &key) const = 0;
	virtual FilterPropagateResult LookupRange(const Value &lower_bound, const Value &upper_bound) const = 0;
	virtual bool IsInitialized() const = 0;
	static unique_ptr<PrefixRangeFilter> CreatePrefixRangeFilter(const LogicalType &key_type);
	static bool TryComputeSpan(const Value &lower_bound, const Value &upper_bound, uhugeint_t &result);
	static bool SupportedType(const LogicalType &type);
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
	static bool IsInternalTableFilterFunction(const string &name);
	static bool IsInternalTableFilterFunction(const ScalarFunction &function) {
		return IsInternalTableFilterFunction(function.name);
	}
};

idx_t SelectBloomFilter(Vector &input, const BloomFilterFunctionData &func_data, SelectionVector &result_sel,
                        idx_t count);
idx_t SelectPerfectHashJoin(Vector &input, const PerfectHashJoinFunctionData &func_data, SelectionVector &result_sel,
                            idx_t count);
idx_t SelectPrefixRange(Vector &input, const PrefixRangeFunctionData &func_data, SelectionVector &result_sel,
                        idx_t count);

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
	static string ToString(const string &column_name, const string &key_column_name);
};

//! Factory for perfect hash join internal function
struct PerfectHashJoinScalarFun : public InternalTableFilterPerfectHashJoinFun {
	using InternalTableFilterPerfectHashJoinFun::GetFunction;
	static constexpr const char *NAME = InternalTableFilterPerfectHashJoinFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
	static string ToString(const string &column_name, const string &key_column_name);
};

//! Factory for prefix range internal function
struct PrefixRangeScalarFun : public InternalTableFilterPrefixRangeFun {
	using InternalTableFilterPrefixRangeFun::GetFunction;
	static constexpr const char *NAME = InternalTableFilterPrefixRangeFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
	static string ToString(const string &column_name, const string &key_column_name);
};

//! Factory for dynamic filter internal function
struct DynamicFilterScalarFun : public InternalTableFilterDynamicFun {
	using InternalTableFilterDynamicFun::GetFunction;
	static constexpr const char *NAME = InternalTableFilterDynamicFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
	static string ToString(const string &column_name, bool has_filter_data);
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
