#include "duckdb/function/aggregate/nested_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

struct ListAggState {
	ChunkCollection *cc;
};

struct ListFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->cc = nullptr;
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->cc) {
			delete state->cc;
		}
	}
	static bool IgnoreNull() {
		return false;
	}
};

static void ListUpdateFunction(Vector inputs[], FunctionData *, idx_t input_count, Vector &state_vector, idx_t count) {
	D_ASSERT(input_count == 1);

	auto &input = inputs[0];
	VectorData sdata;
	state_vector.Orrify(count, sdata);

	DataChunk insert_chunk;

	vector<LogicalType> chunk_types;
	chunk_types.push_back(input.type);
	insert_chunk.Initialize(chunk_types);
	insert_chunk.SetCardinality(1);

	auto states = (ListAggState **)sdata.data;
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < count; i++) {
		auto state = states[sdata.sel->get_index(i)];
		if (!state->cc) {
			state->cc = new ChunkCollection();
		}
		sel.set_index(0, i);
		insert_chunk.data[0].Slice(input, sel, 1);
		state->cc->Append(insert_chunk);
	}
}

static void ListCombineFunction(Vector &state, Vector &combined, idx_t count) {
	VectorData sdata;
	state.Orrify(count, sdata);
	auto states_ptr = (ListAggState **)sdata.data;

	auto combined_ptr = FlatVector::GetData<ListAggState *>(combined);

	for (idx_t i = 0; i < count; i++) {
		auto state = states_ptr[sdata.sel->get_index(i)];
		D_ASSERT(state->cc);
		if (!combined_ptr[i]->cc) {
			combined_ptr[i]->cc = new ChunkCollection();
		}
		combined_ptr[i]->cc->Append(*state->cc);
	}
}

static void ListFinalize(Vector &state_vector, FunctionData *, Vector &result, idx_t count) {
	VectorData sdata;
	state_vector.Orrify(count, sdata);
	auto states = (ListAggState **)sdata.data;

	D_ASSERT(result.type.id() == LogicalTypeId::LIST);
	result.Initialize(result.type); // deals with constants
	auto list_struct_data = FlatVector::GetData<list_entry_t>(result);
	auto &nullmask = FlatVector::Nullmask(result);

	size_t total_len = 0;
	for (idx_t i = 0; i < count; i++) {
		auto state = states[sdata.sel->get_index(i)];
		if (!state->cc) {
			nullmask[i] = true;
			continue;
		}
		D_ASSERT(state->cc);
		auto &state_cc = *state->cc;
		D_ASSERT(state_cc.Types().size() == 1);
		list_struct_data[i].length = state_cc.Count();
		list_struct_data[i].offset = total_len;
		total_len += state_cc.Count();
	}

	auto list_child = make_unique<ChunkCollection>();
	for (idx_t i = 0; i < count; i++) {
		auto state = states[sdata.sel->get_index(i)];
		if (!state->cc) {
			continue;
		}
		auto &state_cc = *state->cc;
		D_ASSERT(state_cc.GetChunk(0).ColumnCount() == 1);
		list_child->Append(state_cc);
	}
	D_ASSERT(list_child->Count() == total_len);
	ListVector::SetEntry(result, move(list_child));
}

unique_ptr<FunctionData> ListBindFunction(ClientContext &context, AggregateFunction &function,
                                   vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 1);
	child_list_t<LogicalType> children;
	children.push_back(make_pair("", arguments[0]->return_type));

	function.return_type = LogicalType(LogicalTypeId::LIST, move(children));
	return make_unique<ListBindData>(); // TODO atm this is not used anywhere but it might not be required after all
	                                    // except for sanity checking
}

void ListFun::RegisterFunction(BuiltinFunctions &set) {
	auto agg = AggregateFunction(
	    "list", {LogicalType::ANY}, LogicalType::LIST, AggregateFunction::StateSize<ListAggState>,
	    AggregateFunction::StateInitialize<ListAggState, ListFunction>, ListUpdateFunction, ListCombineFunction, ListFinalize,
	    nullptr, ListBindFunction, AggregateFunction::StateDestroy<ListAggState, ListFunction>);
	set.AddFunction(agg);
	agg.name = "array_agg";
	set.AddFunction(agg);
}

} // namespace duckdb
