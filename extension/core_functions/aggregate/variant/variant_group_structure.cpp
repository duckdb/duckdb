#include "core_functions/aggregate/variant_functions.hpp"
#include "duckdb/common/types/variant/variant_builder.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/variant/variant_structure.hpp"

namespace duckdb {

namespace {

struct VariantGroupStructureState {
	//! The combined structure of the aggregated rows, null if there are none
	VariantStructure *structure;

	VariantStructure &GetOrCreateStructure() {
		if (!structure) {
			structure = new VariantStructure();
		}
		return *structure;
	}
};

struct VariantGroupStructureOperation {
	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		if (!source.structure) {
			return;
		}
		target.GetOrCreateStructure().Merge(*source.structure, aggr_input_data.allocator);
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &) {
		delete state.structure;
		state.structure = nullptr;
	}
};

void VariantGroupStructureUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                                 Vector &state_vector, idx_t count) {
	D_ASSERT(input_count == 1);
	const VectorIterator<VectorVariantType> variants(inputs[0]);
	const auto states = state_vector.Values<VariantGroupStructureState *>();
	for (idx_t i = 0; i < count; i++) {
		// a SQL NULL is merged as a "NULL" structure, so the result is only NULL without rows
		states[i].GetValue()->GetOrCreateStructure().Merge(variants[i], aggr_input_data.allocator);
	}
}

class VariantGroupStructureSource {
public:
	explicit VariantGroupStructureSource(const VectorIterator<VariantGroupStructureState *> &states) : states(states) {
	}

	//! Returns whether the row is a SQL NULL
	bool Emit(idx_t row, VariantBuilder &builder) {
		auto &state = *states[row].GetValue();
		if (!state.structure) {
			return true;
		}
		state.structure->Emit(builder);
		return false;
	}

private:
	const VectorIterator<VariantGroupStructureState *> &states;
};

void VariantGroupStructureFinalize(Vector &state_vector, AggregateFinalizeInputData &, Vector &result, idx_t count,
                                   idx_t offset) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::VARIANT);
	const auto states = state_vector.Values<VariantGroupStructureState *>();
	Vector structures(LogicalType::VARIANT(), count);
	VariantGroupStructureSource source(states);
	BuildVariant(source, count, structures);

	VectorOperations::Copy(structures, result, count, 0, offset);
	FlatVector::SetSize(result, offset + count);
}

} // namespace

AggregateFunction VariantGroupStructureFun::GetFunction() {
	using STATE = VariantGroupStructureState;
	using OP = VariantGroupStructureOperation;
	AggregateFunction function({LogicalType::VARIANT()}, LogicalType::VARIANT(), AggregateFunction::StateSize<STATE>,
	                           AggregateFunction::StateInitialize<STATE, OP>, VariantGroupStructureUpdate,
	                           AggregateFunction::StateCombine<STATE, OP>, VariantGroupStructureFinalize,
	                           FunctionNullHandling::SPECIAL_HANDLING, nullptr, nullptr,
	                           AggregateFunction::StateDestroy<STATE, OP>);
	function.GetSignature().GetParameter(0).SetName("value");
	function.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
	function.SetDistinctDependent(AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT);
	return function;
}

} // namespace duckdb
