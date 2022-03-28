#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

// FIXME: use a local state for each thread to increase performance?
// FIXME: benchmark the use of simple_update against using update (if applicable)

struct ListAggregatesBindData : public FunctionData {
	ListAggregatesBindData(const LogicalType &stype_p, unique_ptr<Expression> aggr_expr_p);
	~ListAggregatesBindData() override;

	LogicalType stype;
	unique_ptr<Expression> aggr_expr;

	unique_ptr<FunctionData> Copy() override;
};

ListAggregatesBindData::ListAggregatesBindData(const LogicalType &stype_p, unique_ptr<Expression> aggr_expr_p)
    : stype(stype_p), aggr_expr(move(aggr_expr_p)) {
}

unique_ptr<FunctionData> ListAggregatesBindData::Copy() {
	return make_unique<ListAggregatesBindData>(stype, aggr_expr->Copy());
}

ListAggregatesBindData::~ListAggregatesBindData() {
}

struct StateVector {
	StateVector(idx_t count_p, unique_ptr<Expression> aggr_expr_p)
	    : count(count_p), aggr_expr(move(aggr_expr_p)), state_vector(Vector(LogicalType::POINTER, count_p)) {
	}

	~StateVector() {
		// destroy objects within the aggregate states
		auto &aggr = (BoundAggregateExpression &)*aggr_expr;
		if (aggr.function.destructor) {
			aggr.function.destructor(state_vector, count);
		}
	}

	idx_t count;
	unique_ptr<Expression> aggr_expr;
	Vector state_vector;
};

static void ListAggregateFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	D_ASSERT(args.ColumnCount() == 2);
	auto count = args.size();
	Vector &lists = args.data[0];

	// set the result vector
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_validity = FlatVector::Validity(result);

	if (lists.GetType().id() == LogicalTypeId::SQLNULL) {
		result_validity.SetInvalid(0);
		return;
	}

	// get the aggregate function
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (ListAggregatesBindData &)*func_expr.bind_info;
	auto &aggr = (BoundAggregateExpression &)*info.aggr_expr;

	D_ASSERT(aggr.function.update);

	auto lists_size = ListVector::GetListSize(lists);
	auto &child_vector = ListVector::GetEntry(lists);

	VectorData child_data;
	child_vector.Orrify(lists_size, child_data);

	VectorData lists_data;
	lists.Orrify(count, lists_data);
	auto list_entries = (list_entry_t *)lists_data.data;

	// state_buffer holds the state for each list of this chunk
	idx_t size = aggr.function.state_size();
	auto state_buffer = unique_ptr<data_t[]>(new data_t[size * count]);

	// state vector for initialize and finalize
	StateVector state_vector(count, info.aggr_expr->Copy());
	auto states = FlatVector::GetData<data_ptr_t>(state_vector.state_vector);

	// state vector of STANDARD_VECTOR_SIZE holds the pointers to the states
	Vector state_vector_update = Vector(LogicalType::POINTER);
	auto states_update = FlatVector::GetData<data_ptr_t>(state_vector_update);

	// selection vector pointing to the data
	SelectionVector sel_vector(STANDARD_VECTOR_SIZE);
	idx_t states_idx = 0;

	for (idx_t i = 0; i < count; i++) {

		// initialize the state for this list
		auto state_ptr = state_buffer.get() + size * i;
		states[i] = state_ptr;
		aggr.function.initialize(states[i]);

		auto lists_index = lists_data.sel->get_index(i);
		const auto &list_entry = list_entries[lists_index];

		// nothing to do for this list
		if (!lists_data.validity.RowIsValid(lists_index)) {
			result_validity.SetInvalid(i);
			continue;
		}

		// skip empty list
		if (list_entry.length == 0) {
			continue;
		}

		auto source_idx = child_data.sel->get_index(list_entry.offset);
		idx_t child_idx = 0;

		while (child_idx < list_entry.length) {

			// states vector is full, update
			if (states_idx == STANDARD_VECTOR_SIZE) {

				// update the aggregate state(s)
				Vector slice = Vector(child_vector, sel_vector, states_idx);
				aggr.function.update(&slice, aggr.bind_info.get(), 1, state_vector_update, states_idx);

				// reset values
				states_idx = 0;
			}

			sel_vector.set_index(states_idx, source_idx + child_idx);
			states_update[states_idx] = state_ptr;
			states_idx++;
			child_idx++;
		}
	}

	// update the remaining elements of the last list(s)
	if (states_idx != 0) {
		Vector slice = Vector(child_vector, sel_vector, states_idx);
		aggr.function.update(&slice, aggr.bind_info.get(), 1, state_vector_update, states_idx);
	}

	// finalize all the aggregate states
	aggr.function.finalize(state_vector.state_vector, aggr.bind_info.get(), result, count, 0);
}

static unique_ptr<FunctionData> ListAggregateBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {

	// the list column and the name of the aggregate function
	D_ASSERT(bound_function.arguments.size() == 2);
	D_ASSERT(arguments.size() == 2);

	if (arguments[0]->return_type.id() == LogicalTypeId::SQLNULL) {
		bound_function.arguments[0] = LogicalType::SQLNULL;
		bound_function.return_type = LogicalType::SQLNULL;
		return make_unique<VariableReturnBindData>(bound_function.return_type);
	}

	D_ASSERT(LogicalTypeId::LIST == arguments[0]->return_type.id());
	auto list_child_type = ListType::GetChildType(arguments[0]->return_type);
	bound_function.return_type = list_child_type;

	if (!arguments[1]->IsFoldable()) {
		throw InvalidInputException("Aggregate function name must be a constant");
	}

	// get the function name
	Value function_value = ExpressionExecutor::EvaluateScalar(*arguments[1]);
	auto function_name = StringValue::Get(function_value);

	vector<LogicalType> types;
	types.push_back(list_child_type);

	// create the child expression and its type
	vector<unique_ptr<Expression>> children;
	auto expr = make_unique<BoundConstantExpression>(Value(LogicalType::SQLNULL));
	expr->return_type = list_child_type;
	children.push_back(move(expr));

	// look up the aggregate function in the catalog
	QueryErrorContext error_context(nullptr, 0);
	auto func = (AggregateFunctionCatalogEntry *)Catalog::GetCatalog(context).GetEntry<AggregateFunctionCatalogEntry>(
	    context, DEFAULT_SCHEMA, function_name, false, error_context);
	D_ASSERT(func->type == CatalogType::AGGREGATE_FUNCTION_ENTRY);

	// find a matching aggregate function
	string error;
	auto best_function_idx = Function::BindFunction(func->name, func->functions, types, error);
	if (best_function_idx == DConstants::INVALID_INDEX) {
		throw BinderException("No matching aggregate function");
	}

	// found a matching function, bind it as an aggregate
	auto &best_function = func->functions[best_function_idx];
	auto bound_aggr_function = AggregateFunction::BindAggregateFunction(context, best_function, move(children));

	bound_function.arguments[0] =
	    LogicalType::LIST(bound_aggr_function->function.arguments[0]); // for proper casting of the vectors
	bound_function.return_type = bound_aggr_function->function.return_type;
	return make_unique<ListAggregatesBindData>(bound_function.return_type, move(bound_aggr_function));
}

ScalarFunction ListAggregateFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::VARCHAR}, LogicalType::ANY,
	                      ListAggregateFunction, false, ListAggregateBind, nullptr, nullptr, nullptr);
}

void ListAggregateFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_aggregate", "array_aggregate", "list_aggr", "array_aggr"}, GetFunction());
}

} // namespace duckdb
