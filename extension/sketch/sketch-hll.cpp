/*
 * Implements HLL sketch operations for cardinality estimation and merging. Uses the apache
 * datasketches library (see https://datasketches.apache.org/ for more info).
 * SQL functions implemented are roughly based on the BigQuery HLL sketch functinos
 *   (https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions)
 *
 *   hll_count_init: Aggregation that returns a sketch (BLOB) containing summary information
 *                   for distinct values.
 *   hll_count_extract: Scalar function that returns cardinality estimation from a sketch.
 *   hll_count_merge:  Aggregation that merges multiple sketches into
 *                    a single one.
 *
 * Why is this useful?
 *   Imagine you are computing unique user visits to your website. You don't want to store
 *   each individual visit, you want to summarize by day. However, if you have 200k unique
 *   users on thursday, and 250k on friday, how do you combine those to figure out how
 *   many you have in the week? (Distinct values don't add). You can create a sketch
 *   for each day, which lets you summarize fairly concisely, and then merge the sketches
 *   the final sketch will allow you to figure out how many unique users you had for the
 *   week.
 */

#include "include/sketch-hll.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/function/aggregate/algebraic_functions.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/aggregate/sum_helpers.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"

#include "third_party/apache-datasketches/hll/include/hll.hpp"

#include <iostream>

namespace duckdb {

// Macro to delete a field if present and set it to nullptr.
#define DESTROY_FIELD(_x_)                                                                                             \
	if (this->_x_ != nullptr) {                                                                                        \
		delete (this->_x_);                                                                                            \
		this->_x_ = nullptr;                                                                                           \
	}

/**
 * State of an HLL computation. Either in one of two modes:
 * union mode: combining multiple sketches into a single one
 * update mode: adding values to a sketch.
 * Only one sketch should be active at a time, either a value sketch
 * (when in update mode) or a union sketch (when in combine mode).
 * When switching from one mode to the other, the existing sketch is
 * copied.
 */
class HllStateBase {
	// Detemines the size of the sketch that we create. Consider doing this
	// dymanically based on the type or cardinality estimates of underlying
	// tables.
	static const uint8_t lg_sketch_rows = 12;
	// Keep a static empty sketch so we don't have to recreate it over and
	// over again.
	static const vector<uint8_t> empty_sketch_buffer;

public:
	bool isset;
	void Initialize() {
		this->isset = false;
		this->value_sketch = nullptr;
		this->union_sketch = nullptr;
		this->serialized = nullptr;
	}

	void Merge(string_t input) {
		auto other = datasketches::hll_sketch::deserialize(input.GetDataUnsafe(), input.GetSize());

		if (!other.is_empty()) {
			this->isset = true;
			GetUnionSketch().update(other);
		}
	}

	void Merge(const HllStateBase &other) {
		this->isset |= other.isset;
		if (other.isset && !other.GetSketch().is_empty()) {
			GetUnionSketch().update(other.GetSketch());
		}
	}

	string_t Serialize() {
		if (!isset) {
			return GetEmptySketch();
		} else if (serialized != nullptr) {
			return string_t((char *)this->serialized->data(), this->serialized->size());
		} else {
			auto result = GetSketch();
			// Keep the serialized result around because we'll need to hold on to the memory until
			// state can be properly destroyed.
			this->serialized = new vector<uint8_t>(result.serialize_compact());
			return string_t((char *)this->serialized->data(), this->serialized->size());
		}
	}

	void Destroy() {
		DESTROY_FIELD(value_sketch);
		DESTROY_FIELD(union_sketch);
		DESTROY_FIELD(serialized);
	}

protected:
	datasketches::hll_sketch &GetSketch() const {
		// We shouldn't be doing any sketch operations after serialization, since the
		// serialized buffer is memoized.
		D_ASSERT(serialized == nullptr);

		if (this->union_sketch != nullptr) {
			// Switch from unioning sketches to adding values.
			this->value_sketch = new datasketches::hll_sketch(this->union_sketch->get_result());
			delete (this->union_sketch);
			this->union_sketch = nullptr;
		} else if (this->value_sketch == nullptr) {
			this->value_sketch = new datasketches::hll_sketch(lg_sketch_rows);
		}
		return *this->value_sketch;
	}

private:
	static string_t GetEmptySketch() {
		return string_t((char *)empty_sketch_buffer.data(), empty_sketch_buffer.size());
	}

	datasketches::hll_union &GetUnionSketch() const {
		// We shouldn't be doing any sketch operations after serialization, since the
		// serialized buffer is memoized.
		D_ASSERT(serialized == nullptr);

		if (this->union_sketch == nullptr) {
			this->union_sketch = new datasketches::hll_union(lg_sketch_rows);
		}
		if (this->value_sketch != nullptr) {
			// Switch from adding values to unioning sketches mode.
			this->union_sketch->update(*this->value_sketch);
			delete (this->value_sketch);
			this->value_sketch = nullptr;
		}
		return *this->union_sketch;
	}

private:
	mutable datasketches::hll_sketch *value_sketch;
	mutable datasketches::hll_union *union_sketch;
	vector<uint8_t> *serialized;
};

// Save the empty sketch so we don't have to recreate it.
const vector<uint8_t> HllStateBase::empty_sketch_buffer = datasketches::hll_sketch(lg_sketch_rows).serialize_compact();

/**
 * Specializations of HllState for various types. Anything that
 * isn't dependent on type should go in the base
 */
template <class T>
class HllState : public HllStateBase {
public:
	inline void Update(T value) {
		this->isset = true;
		this->GetSketch().update(value);
	}
};

template <>
class HllState<hugeint_t> : public HllStateBase {
public:
	inline void Update(hugeint_t value) {
		this->isset = true;
		this->GetSketch().update(&value, sizeof(value));
	}
};

template <>
class HllState<string_t> : public HllStateBase {
public:
	inline void Update(string_t value) {
		this->isset = true;
		this->GetSketch().update(value.GetDataUnsafe(), value.GetSize());
	}
};

struct HllInitOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->Initialize();
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		target->Merge(source);
	}

	template <class T, class STATE>
	static void Finalize(Vector &, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		// Want to return a sketch even if all of the values are null.
		target[idx] = state->Serialize();
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static inline void Operation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &, idx_t idx) {
		// TODO: This can almost certainly be made faster by operating over
		// a chunk at a time.
		state->Update(input[idx]);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &, idx_t count) {
		state->Update(*input);
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		// This is an anarchast's dream; to not just destroy the state
		// but to cause the state to destroy itself.
		state->Destroy();
	}

	static bool IgnoreNull() {
		return false;
	}
};

struct HllMergeOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->Initialize();
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &aggr_input_data) {
		target->Merge(source);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		target[idx] = state->Serialize();
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
		state->Merge(input[idx]);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask,
	                              idx_t count) {
		state->Merge(*input);
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		state->Destroy();
	}

	static bool IgnoreNull() {
		return false;
	}
};

AggregateFunction GetInitAggregate(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT128: {
		auto function =
		    AggregateFunction::UnaryAggregateDestructor<HllState<hugeint_t>, hugeint_t, string_t, HllInitOperation>(
		        LogicalType::HUGEINT, LogicalType::BLOB);
		return function;
	}
	case PhysicalType::INT64: {
		auto function =
		    AggregateFunction::UnaryAggregateDestructor<HllState<int64_t>, int64_t, string_t, HllInitOperation>(
		        LogicalType::BIGINT, LogicalType::BLOB);
		return function;
	}
	case PhysicalType::INT32: {
		auto function =
		    AggregateFunction::UnaryAggregateDestructor<HllState<int32_t>, int32_t, string_t, HllInitOperation>(
		        LogicalType::INTEGER, LogicalType::BLOB);
		return function;
	}
	case PhysicalType::INT16: {
		auto function =
		    AggregateFunction::UnaryAggregateDestructor<HllState<int16_t>, int16_t, string_t, HllInitOperation>(
		        LogicalType::SMALLINT, LogicalType::BLOB);
		return function;
	}
	case PhysicalType::DOUBLE: {
		auto function =
		    AggregateFunction::UnaryAggregateDestructor<HllState<double>, double, string_t, HllInitOperation>(
		        LogicalType::DOUBLE, LogicalType::BLOB);
		return function;
	}
	case PhysicalType::VARCHAR: {
		auto function =
		    AggregateFunction::UnaryAggregateDestructor<HllState<string_t>, string_t, string_t, HllInitOperation>(
		        LogicalType::VARCHAR, LogicalType::BLOB);
		return function;
	}
	default:
		throw InternalException("Unimplemented HLL init aggregate");
	}
}

unique_ptr<FunctionData> BindDecimalHllInit(ClientContext &context, AggregateFunction &function,
                                            vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	function = GetInitAggregate(decimal_type.InternalType());
	function.name = "hll_count_init";
	function.arguments[0] = decimal_type;
	function.return_type = LogicalType::DECIMAL(Decimal::MAX_WIDTH_DECIMAL, DecimalType::GetScale(decimal_type));
	return nullptr;
}

struct ExtractOperator {
	template <class TA, class TR>
	static TR Operation(TA input) {
		auto sketch = datasketches::hll_sketch::deserialize(input.GetDataUnsafe(), input.GetSize());
		return sketch.get_estimate();
	}
};

void SketchHll::RegisterFunction(ClientContext &context) {
	AggregateFunctionSet init("hll_count_init");
	init.AddFunction(GetInitAggregate(PhysicalType::INT128));
	init.AddFunction(GetInitAggregate(PhysicalType::INT64));
	init.AddFunction(GetInitAggregate(PhysicalType::INT32));
	init.AddFunction(GetInitAggregate(PhysicalType::INT16));
	init.AddFunction(GetInitAggregate(PhysicalType::DOUBLE));
	init.AddFunction(GetInitAggregate(PhysicalType::VARCHAR));
	init.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr,
	                                   nullptr, nullptr, FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr,
	                                   BindDecimalHllInit));

	ScalarFunctionSet extract("hll_count_extract");
	extract.AddFunction(ScalarFunction({LogicalType::BLOB}, LogicalType::BIGINT,
	                                   ScalarFunction::UnaryFunction<string_t, int64_t, ExtractOperator>));

	AggregateFunctionSet merge("hll_count_merge");
	merge.AddFunction(
	    AggregateFunction::UnaryAggregateDestructor<HllState<string_t>, string_t, string_t, HllMergeOperation>(
	        LogicalType::BLOB, LogicalType::BLOB));

	auto &catalog = Catalog::GetCatalog(context);
	CreateAggregateFunctionInfo init_info(move(init));
	catalog.AddFunction(context, &init_info);
	CreateScalarFunctionInfo extract_info(move(extract));
	catalog.AddFunction(context, &extract_info);
	CreateAggregateFunctionInfo merge_info(move(merge));
	catalog.AddFunction(context, &merge_info);
}

} // namespace duckdb
