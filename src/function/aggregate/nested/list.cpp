#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/function/aggregate/nested_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

struct ListAggState {
	Vector *list_vector;
};

struct ListFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->list_vector = nullptr;
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->list_vector) {
			delete state->list_vector;
		}
	}
	static bool IgnoreNull() {
		return false;
	}
};

static void ListUpdateFunction(Vector inputs[], AggregateInputData &, idx_t input_count, Vector &state_vector,
                               idx_t count) {
	D_ASSERT(input_count == 1);

	auto &input = inputs[0];
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);

	auto list_vector_type = LogicalType::LIST(input.GetType());

	auto states = (ListAggState **)sdata.data;
	if (input.GetVectorType() == VectorType::SEQUENCE_VECTOR) {
		input.Flatten(count);
	}
	for (idx_t i = 0; i < count; i++) {
		auto state = states[sdata.sel->get_index(i)];
		if (!state->list_vector) {
			// NOTE: any number bigger than 1 can cause DuckDB to run out of memory for specific queries
			// consisting of millions of groups in the group by and complex (nested) vectors
			state->list_vector = new Vector(list_vector_type, 1);
		}
		ListVector::Append(*state->list_vector, input, i + 1, i);
	}
}

static void ListCombineFunction(Vector &state, Vector &combined, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state.ToUnifiedFormat(count, sdata);
	auto states_ptr = (ListAggState **)sdata.data;

	auto combined_ptr = FlatVector::GetData<ListAggState *>(combined);
	for (idx_t i = 0; i < count; i++) {
		auto state = states_ptr[sdata.sel->get_index(i)];
		if (!state->list_vector) {
			// NULL, no need to append.
			continue;
		}
		if (!combined_ptr[i]->list_vector) {
			// NOTE: initializing this with a capacity of ListVector::GetListSize(*state->list_vector) causes
			// DuckDB to run out of memory for multiple threads with millions of groups in the group by and complex
			// (nested) vectors
			combined_ptr[i]->list_vector = new Vector(state->list_vector->GetType(), 1);
		}
		ListVector::Append(*combined_ptr[i]->list_vector, ListVector::GetEntry(*state->list_vector),
		                   ListVector::GetListSize(*state->list_vector));
	}
}

static void ListFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (ListAggState **)sdata.data;

	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

	auto &mask = FlatVector::Validity(result);
	auto list_struct_data = FlatVector::GetData<list_entry_t>(result);
	size_t total_len = ListVector::GetListSize(result);

	for (idx_t i = 0; i < count; i++) {
		auto state = states[sdata.sel->get_index(i)];
		const auto rid = i + offset;
		if (!state->list_vector) {
			mask.SetInvalid(rid);
			continue;
		}

		auto &state_lv = *state->list_vector;
		auto state_lv_count = ListVector::GetListSize(state_lv);
		list_struct_data[rid].length = state_lv_count;
		list_struct_data[rid].offset = total_len;
		total_len += state_lv_count;

		auto &list_vec_to_append = ListVector::GetEntry(state_lv);
		ListVector::Append(result, list_vec_to_append, state_lv_count);
	}
}

unique_ptr<FunctionData> ListBindFunction(ClientContext &context, AggregateFunction &function,
                                          vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 1);
	function.return_type = LogicalType::LIST(arguments[0]->return_type);
	return nullptr;
}

void ListFun::RegisterFunction(BuiltinFunctions &set) {
	auto agg =
	    AggregateFunction("list", {LogicalType::ANY}, LogicalTypeId::LIST, AggregateFunction::StateSize<ListAggState>,
	                      AggregateFunction::StateInitialize<ListAggState, ListFunction>, ListUpdateFunction,
	                      ListCombineFunction, ListFinalize, nullptr, ListBindFunction,
	                      AggregateFunction::StateDestroy<ListAggState, ListFunction>, nullptr, nullptr);
	set.AddFunction(agg);
	agg.name = "array_agg";
	set.AddFunction(agg);
}

} // namespace duckdb
