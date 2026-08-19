#include "core_functions/aggregate/variant_functions.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/owning_string_map.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/list_segment.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/variant/variant_builder.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/aggregate_state_layout.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// State
//===--------------------------------------------------------------------===//

// TODO: can we reference the input vector so we don't need to copy over any data?
struct AggregateState {
	static constexpr const char *STATE_NAMES[] = {"map"};
	using STATE_TYPE = StructStateType<>;

	// TODO: verify performance implications for OrderedOwningStringMap
	OwningStringMap<idx_t> *map;
};

//===--------------------------------------------------------------------===//
// Operation
//===--------------------------------------------------------------------===//

struct AggregateOperation {
	static bool IgnoreNull() {
		return true;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &input) {
		if (!source.map) {
			return;
		}
		if (!target.map) {
			target.map = new OwningStringMap<idx_t>(input.allocator);
		}

		for (auto &[key, idx] : *source.map) {
			target.map->emplace(make_pair(key, idx));
		}
	}
};

void BinaryScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &states,
                         idx_t count) {
	D_ASSERT(input_count == 2);
	const auto &keys = inputs[0];
	const auto &values = inputs[1];

	UnifiedVectorFormat key_data, value_data, state_data;
	keys.ToUnifiedFormat(key_data);
	values.ToUnifiedFormat(value_data);
	states.ToUnifiedFormat(state_data);

	const auto key_data_ptr = UnifiedVectorFormat::GetData<string_t>(key_data);

	for (idx_t i = 0; i < count; i++) {
		const auto key_idx = key_data.sel->get_index(i);
		const auto state_idx = state_data.sel->get_index(i);

		const auto lkey = key_data_ptr[key_idx];
		auto lstate = ((AggregateState **)state_data.data)[state_idx];
		if (!lstate->map) {
			lstate->map = new OwningStringMap<idx_t>(aggr_input_data.allocator);
		}

		auto idx = lstate->map->size();
		lstate->map->insert(make_pair(lkey, idx));
	}

	// AggregateBinaryInput input(aggr_input_data, key_data.validity, bvalidity);
}

class AggregateSource {
public:
	AggregateSource(idx_t count, AggregateState &state) : count(count), state(state) {
	}

public:
	bool Emit(idx_t row_idx, VariantBuilder &builder) const {
		vector<string_t> children;
		children.reserve(state.map->size());

		for (const auto &child : *state.map) {
			children.push_back(child.first);
		}

		builder.EmitObject(
		    state.map->size(), [&](idx_t i) { return children[i]; },
		    [&](idx_t i) {
			    //auto byte_offset = NumericCast<uint32_t>(builder.blob.size());
			    //builder.EmitPrimitive(Value::BOOLEAN(true), byte_offset);
			    builder.EmitNull();
		    });

		return false;
	}

private:
	idx_t count;
	AggregateState &state;
};

void StateFinalize(Vector &states, AggregateFinalizeInputData &finalize_input_data, Vector &result, idx_t count,
                   idx_t offset) {
	// TODO: How can states be something different?
	if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		const auto state_data = ConstantVector::GetData<AggregateState *>(states);

		AggregateSource source(count, **state_data);
		BuildVariant(source, count, result);

		return;
	}
	if (states.GetVectorType() == VectorType::FLAT_VECTOR) {
		auto state_data = FlatVector::GetData<AggregateState *>(states);

		AggregateSource source(count, *state_data[0]);
		BuildVariant(source, count, result);

		return;
	}

	throw InternalException("Not compatible!");
}

} // namespace

AggregateFunction VariantGroupObjectFun::GetFunction() {
	auto func =
	    AggregateFunction({LogicalType::VARCHAR, LogicalType::VARIANT()}, LogicalType::VARIANT(),
	                      AggregateFunction::StateSize<AggregateState>,
	                      AggregateFunction::StateInitialize<AggregateState, AggregateOperation>, BinaryScatterUpdate,
	                      AggregateFunction::StateCombine<AggregateState, AggregateOperation>, StateFinalize,
	                      FunctionNullHandling::DEFAULT_NULL_HANDLING);
	return func;
}

} // namespace duckdb
