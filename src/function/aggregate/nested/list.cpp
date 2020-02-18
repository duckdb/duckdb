#include "duckdb/function/aggregate/nested_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

using namespace std;

namespace duckdb {


static index_t list_payload_size(TypeId return_type) {
	return sizeof(Vector);
}

// NB: the result of this is copied around
static void list_initialize(data_ptr_t payload, TypeId return_type) {
	memset(payload, 0, sizeof(Vector));
	auto v = (Vector *)payload;
	v->type = TypeId::INVALID;
}

static void list_update(Vector inputs[], index_t input_count, Vector &state) {
	assert(input_count == 1);
	inputs[0].Normalify();
//
//	auto states = (Vector **)state.GetData();
//
//	VectorOperations::Exec(state, [&](index_t i, index_t k) {
//		auto state = states[i];
//		if (state->type == TypeId::INVALID) {
//			state->Initialize(inputs[0].type, true, 150); // FIXME size? needs to grow this!
//			state->count = 0;
//			// TODO need to init child vectors, too
//			// TODO need sqltype for this
//		}
//		state->count++;
//		for (auto &child : state->GetChildren()) {
//			child.second->count++;
//		}
//		state->SetValue(state->count - 1, inputs[0].GetValue(i)); // FIXME this is evil and slow.
//		// We could alternatively collect all values for the same vector in this input chunk and assign with selection
//		// vectors map<ptr, sel_vec>! worst case, one entry per input value, but meh todo: could abort?
//	});
}

static void list_combine(Vector &state, Vector &combined) {
	throw Exception("eek");
	// TODO should be rather straightforward, copy vectors together
}

static void list_finalize(Vector &state, Vector &result) {
	auto states = (Vector **)state.GetData();

//	result.Initialize(TypeId::LIST, false, state.count);
//	auto list_struct_data = (list_entry_t *)result.GetData();
//
//	// first get total len of child vec
//	size_t total_len = 0;
//	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
//		auto state_ptr = states[i];
//		list_struct_data[i].length = state_ptr->count;
//		list_struct_data[i].offset = total_len;
//		total_len += state_ptr->count;
//	});
//
//	auto list_child = make_unique<Vector>();
//	list_child->Initialize(states[0]->type, false, total_len);
//	list_child->count = 0;
//	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
//		auto state_ptr = states[i];
//		list_child->Append(*state_ptr);
//	});
//	assert(list_child->count == total_len);
//	result.AddChild(move(list_child));
}


unique_ptr<FunctionData> list_bind(BoundAggregateExpression &expr, ClientContext &context) {
	assert(expr.children.size() == 1);
	expr.sql_return_type = SQLType::LIST;
	expr.sql_return_type.child_type.push_back(make_pair("", expr.arguments[0]));
	return make_unique<ListBindData>(expr.sql_return_type);
}

void ListFun::RegisterFunction(BuiltinFunctions &set) {
	auto agg = AggregateFunction("list", {SQLType::ANY}, SQLType::LIST, list_payload_size, list_initialize, list_update,
	                             list_combine, list_finalize, nullptr, list_bind);
	set.AddFunction(agg);
}

} // namespace duckdb
