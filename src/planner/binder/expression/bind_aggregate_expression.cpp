#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

using namespace duckdb;
using namespace std;


static index_t list_payload_size(TypeId return_type) {
	return sizeof(Vector);
}

// NB: the result of this is copied around
static void list_initialize(data_ptr_t payload, TypeId return_type) {
	memset(payload, 0, sizeof(Vector));
	auto v = (Vector*) payload;
	v->type = TypeId::INVALID;
}

static void list_update(Vector inputs[], index_t input_count, Vector &state) {
	assert(input_count == 1);
	inputs[0].Normalify();

	auto states = (Vector**)state.GetData();

	VectorOperations::Exec(state, [&](index_t i, index_t k) {
		auto state = states[i];
		if (state->type == TypeId::INVALID) {
			state->Initialize(inputs[0].type, true, 100);  // FIXME size? needs to grow this!
			state->count = 0;
			// TODO need to init child vectors, too
			// TODO need sqltype for this
		}
		state->count++;
		for (auto& child : state->children) {
			child.second->count++;
		}
		state->SetValue(state->count-1, inputs[0].GetValue(i)); // FIXME this is evil and slow.
		// We could alternatively collect all values for the same vector in this input chunk and assign with selection vectors
		// map<ptr, sel_vec>!
		// worst case, one entry per input value, but meh
		// todo: could abort?
	});
}

static void list_combine(Vector &state, Vector &combined) {
	throw Exception("eek");
	// TODO should be rather straightforward, copy vectors together
}

static void list_finalize(Vector &state, Vector &result) {
	auto states = (Vector**)state.GetData();

	result.Initialize(TypeId::LIST, false, state.count);
	auto list_struct_data = (list_entry_t *)result.GetData();

	// first get total len of child vec
	size_t total_len = 0;
	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		auto state_ptr = states[i];
		list_struct_data[i].length = state_ptr->count;
		list_struct_data[i].offset = total_len;
		total_len += state_ptr->count;
	});

	auto list_child = make_unique<Vector>();
	list_child->Initialize(states[0]->type, false, total_len);
	list_child->count = 0;
	VectorOperations::Exec(state, [&](uint64_t i, uint64_t k) {
		auto state_ptr = states[i];
		list_child->Append(*state_ptr);
	});
	assert(list_child->count == total_len);
	result.children.push_back(pair<string, unique_ptr<Vector>>("", move(list_child)));
}



BindResult SelectBinder::BindAggregate(FunctionExpression &aggr, AggregateFunctionCatalogEntry *func, index_t depth) {
	// first bind the child of the aggregate expression (if any)
	AggregateBinder aggregate_binder(binder, context);
	string error;
	for (index_t i = 0; i < aggr.children.size(); i++) {
		aggregate_binder.BindChild(aggr.children[i], 0, error);
	}
	if (!error.empty()) {
		// failed to bind child
		if (aggregate_binder.BoundColumns()) {
			for (index_t i = 0; i < aggr.children.size(); i++) {
				// however, we bound columns!
				// that means this aggregation belongs to this node
				// check if we have to resolve any errors by binding with parent binders
				bool success = aggregate_binder.BindCorrelatedColumns(aggr.children[i]);
				// if there is still an error after this, we could not successfully bind the aggregate
				if (!success) {
					throw BinderException(error);
				}
				auto &bound_expr = (BoundExpression &)*aggr.children[i];
				ExtractCorrelatedExpressions(binder, *bound_expr.expr);
			}
		} else {
			// we didn't bind columns, try again in children
			return BindResult(error);
		}
	}
	// all children bound successfully
	// extract the children and types
	vector<SQLType> types;
	vector<unique_ptr<Expression>> children;
	for (index_t i = 0; i < aggr.children.size(); i++) {
		auto &child = (BoundExpression &)*aggr.children[i];
		types.push_back(child.sql_type);
		children.push_back(move(child.expr));
	}

	// FIXME eeevil hack alert
	if (func->name == "list") {

		// can only have one child
		// return type is a list of child type

		// TODO exception
		assert(children.size() == 1 && types.size() == 1);

		auto return_type = SQLType::LIST;
		return_type.child_type.push_back(pair<string, SQLType>("", types[0]));


		auto bound_function = AggregateFunction("list", {types[0]}, return_type, list_payload_size, list_initialize,
			                                  list_update, list_combine, list_finalize);
		// bind the aggregate

		// TODO wtf happens if it is?
		assert(!aggr.distinct);

		// create the aggregate
		auto aggregate = make_unique<BoundAggregateExpression>(GetInternalType(return_type), bound_function, aggr.distinct);
		aggregate->children = move(children);


		index_t aggr_index;
		aggr_index = node.aggregates.size();
		node.aggregate_map.insert(make_pair(aggregate.get(), aggr_index));
		node.aggregates.push_back(move(aggregate));

		// TODO: check for all the aggregates if this aggregate already exists

		// now create a column reference referring to the aggregate
		auto colref = make_unique<BoundColumnRefExpression>(
		    aggr.alias.empty() ? node.aggregates[aggr_index]->ToString() : aggr.alias,
		    node.aggregates[aggr_index]->return_type, ColumnBinding(node.aggregate_index, aggr_index), depth);
		// move the aggregate expression into the set of bound aggregates
		return BindResult(move(colref), return_type);
	}

	// bind the aggregate
	index_t best_function = Function::BindFunction(func->name, func->functions, types);
	// found a matching function!
	auto &bound_function = func->functions[best_function];
	// check if we need to add casts to the children
	bound_function.CastToFunctionArguments(children, types);

	auto return_type = bound_function.return_type;
	// create the aggregate
	auto aggregate = make_unique<BoundAggregateExpression>(GetInternalType(return_type), bound_function, aggr.distinct);
	aggregate->children = move(children);

	// check for all the aggregates if this aggregate already exists
	index_t aggr_index;
	auto entry = node.aggregate_map.find(aggregate.get());
	if (entry == node.aggregate_map.end()) {
		// new aggregate: insert into aggregate list
		aggr_index = node.aggregates.size();
		node.aggregate_map.insert(make_pair(aggregate.get(), aggr_index));
		node.aggregates.push_back(move(aggregate));
	} else {
		// duplicate aggregate: simplify refer to this aggregate
		aggr_index = entry->second;
	}

	// now create a column reference referring to the aggregate
	auto colref = make_unique<BoundColumnRefExpression>(
	    aggr.alias.empty() ? node.aggregates[aggr_index]->ToString() : aggr.alias,
	    node.aggregates[aggr_index]->return_type, ColumnBinding(node.aggregate_index, aggr_index), depth);
	// move the aggregate expression into the set of bound aggregates
	return BindResult(move(colref), return_type);
}
