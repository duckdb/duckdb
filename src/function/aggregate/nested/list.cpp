#include "duckdb/function/aggregate/nested_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

using namespace std;

namespace duckdb {

struct list_agg_state_t {
	FlatVector* vec;
	ChunkCollection* cc;
};

static index_t list_payload_size(TypeId return_type) {
	return sizeof(list_agg_state_t);
}

// NB: the result of this is copied around merrily, so just zero it for now
static void list_initialize(data_ptr_t payload, TypeId return_type) {
	memset(payload, 0, sizeof(list_agg_state_t));
}

static void list_update(Vector inputs[], index_t input_count, Vector &state) {
	assert(input_count == 1);
	inputs[0].Normalify();
	auto states = (list_agg_state_t **)state.GetData();

	DataChunk insert_chunk;

	vector<TypeId> chunk_types;
	chunk_types.push_back(inputs[0].type);
	insert_chunk.Initialize(chunk_types);
	insert_chunk.data[0].Reference(inputs[0]);
	insert_chunk.SetCardinality(1, insert_chunk.owned_sel_vector);

	VectorOperations::Exec(state, [&](index_t i, index_t k) {
		if(!states[i]->vec) {
			states[i]->cc = new ChunkCollection();
			states[i]->vec = new FlatVector();
			states[i]->vec->Initialize(inputs[0].type, true, 150);
			states[i]->vec->SetCount(0);
		}
		assert(states[i]->vec);
		auto& state_vec = *(states[i]->vec);
		// FIXME
		assert(state_vec.size() <= 150);
		state_vec.SetCount(state_vec.size()+1);
		state_vec.SetValue(state_vec.size() - 1, inputs[0].GetValue(i)); // FIXME this is evil and slow.
		// We could alternatively collect all values for the same vector in this input chunk and assign with selection
		// vectors map<ptr, sel_vec>! worst case, one entry per input value, but meh todo: could abort?
		insert_chunk.sel_vector[0] = i;
		insert_chunk.Verify();
		//states[i]->cc->Append(insert_chunk);
	});
}

static void list_combine(Vector &state, Vector &combined) {
	throw Exception("eek");
	// TODO should be rather straightforward, copy vectors together.
}

static void list_destroy(Vector &state) {
	auto states = (list_agg_state_t **)state.GetData();
	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		if (states[i]->vec) {
			delete states[i]->vec;
			delete states[i]->cc;

		}
	});
}

static void list_finalize(Vector &state, Vector &result) {
	auto states = (list_agg_state_t **)state.GetData();

	result.Initialize(TypeId::LIST, false, state.size());
	auto list_struct_data = (list_entry_t *)result.GetData();

	size_t total_len = 0;
	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		assert(states[i]->vec);
		auto& state_vec = *(states[i]->vec);
		list_struct_data[i].length = state_vec.size();
		list_struct_data[i].offset = total_len;
		total_len += state_vec.size();
	});

	auto list_child = make_unique<FlatVector>();
	list_child->Initialize(states[0]->vec->type, false, total_len);
	list_child->SetCount(0);
	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		auto& state_vec = *(states[i]->vec);
		VectorOperations::Append(state_vec, *list_child);
		list_child->SetCount(list_child->size() + state_vec.size());
	});
	assert(list_child->size() == total_len);
	result.SetListEntry(move(list_child));
}


unique_ptr<FunctionData> list_bind(BoundAggregateExpression &expr, ClientContext &context, SQLType& return_type) {
	assert(expr.children.size() == 1);
	return_type = SQLType::LIST;
	return_type.child_type.push_back(make_pair("", expr.arguments[0]));
	return make_unique<ListBindData>(); // TODO atm this is not used anywhere but it might not be required after all except for sanity checking
}

void ListFun::RegisterFunction(BuiltinFunctions &set) {
	auto agg = AggregateFunction("list", {SQLType::ANY}, SQLType::LIST, list_payload_size, list_initialize, list_update,
	                             list_combine, list_finalize, nullptr, list_bind, list_destroy);
	set.AddFunction(agg);
}

} // namespace duckdb
