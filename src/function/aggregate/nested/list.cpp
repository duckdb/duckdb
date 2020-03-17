#include "duckdb/function/aggregate/nested_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

using namespace std;

namespace duckdb {

struct list_agg_state_t {
	ChunkCollection *cc;
};

struct ListFunction {
	template <class STATE> static void Initialize(STATE *state) {
		state->cc = nullptr;
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		throw NotImplementedException("COMBINE not implemented for LIST");
	}

	template <class STATE> static void Destroy(STATE *state) {
		if (state->cc) {
			delete state->cc;
		}
	}
	static bool IgnoreNull() {
		return true;
	}
};

static void list_update(Vector inputs[], idx_t input_count, Vector &state) {
	// assert(input_count == 1);
	// inputs[0].Normalify();
	// auto states = (list_agg_state_t **)state.GetData();

	// DataChunk insert_chunk;

	// vector<TypeId> chunk_types;
	// chunk_types.push_back(inputs[0].type);
	// insert_chunk.Initialize(chunk_types);
	// insert_chunk.data[0].Reference(inputs[0]);
	// insert_chunk.SetCardinality(1, insert_chunk.owned_sel_vector);

	// VectorOperations::Exec(state, [&](idx_t i, idx_t k) {
	// 	if (!states[i]->cc) {
	// 		states[i]->cc = new ChunkCollection();
	// 	}
	// 	assert(states[i]->cc);
	// 	insert_chunk.sel_vector[0] = i;
	// 	insert_chunk.Verify();
	// 	states[i]->cc->Append(insert_chunk);
	// 	states[i]->cc->Verify();
	// });
	throw NotImplementedException("FIXME list update");
}

static void list_finalize(Vector &state, Vector &result) {
	// auto states = (list_agg_state_t **)state.GetData();

	// result.Initialize(TypeId::LIST);
	// auto list_struct_data = (list_entry_t *)result.GetData();

	// size_t total_len = 0;
	// VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
	// 	assert(states[i]->cc);
	// 	auto &state_cc = *(states[i]->cc);
	// 	assert(state_cc.types.size() == 1);
	// 	list_struct_data[i].length = state_cc.count;
	// 	list_struct_data[i].offset = total_len;
	// 	total_len += state_cc.count;
	// });

	// auto list_child = make_unique<ChunkCollection>();
	// VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
	// 	auto &state_cc = *(states[i]->cc);
	// 	assert(state_cc.chunks[0]->column_count() == 1);
	// 	list_child->Append(state_cc);
	// });
	// assert(list_child->count == total_len);
	// result.SetListEntry(move(list_child));
	throw NotImplementedException("FIXME list finalize");
}

unique_ptr<FunctionData> list_bind(BoundAggregateExpression &expr, ClientContext &context, SQLType &return_type) {
	assert(expr.children.size() == 1);
	return_type = SQLType::LIST;
	return_type.child_type.push_back(make_pair("", expr.arguments[0]));
	return make_unique<ListBindData>(); // TODO atm this is not used anywhere but it might not be required after all
	                                    // except for sanity checking
}

void ListFun::RegisterFunction(BuiltinFunctions &set) {
	auto agg = AggregateFunction("list", {SQLType::ANY}, SQLType::LIST,
			AggregateFunction::StateSize<list_agg_state_t>,
			AggregateFunction::StateInitialize<list_agg_state_t, ListFunction>,
			list_update,
			AggregateFunction::StateCombine<list_agg_state_t, ListFunction>,
			list_finalize,
			nullptr,
			list_bind,
			AggregateFunction::StateDestroy<list_agg_state_t, ListFunction>);
	set.AddFunction(agg);
}

} // namespace duckdb
