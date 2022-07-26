#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate/nested_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

// FIXME: use a local state for each thread to increase performance?
// FIXME: benchmark the use of simple_update against using update (if applicable)

struct ListAggregatesBindData : public FunctionData {
	ListAggregatesBindData(const LogicalType &stype_p, unique_ptr<Expression> aggr_expr_p);
	~ListAggregatesBindData() override;

	LogicalType stype;
	unique_ptr<Expression> aggr_expr;

	unique_ptr<FunctionData> Copy() const override {
		return make_unique<ListAggregatesBindData>(stype, aggr_expr->Copy());
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = (const ListAggregatesBindData &)other_p;
		return stype == other.stype && aggr_expr->Equals(other.aggr_expr.get());
	}
};

ListAggregatesBindData::ListAggregatesBindData(const LogicalType &stype_p, unique_ptr<Expression> aggr_expr_p)
    : stype(stype_p), aggr_expr(move(aggr_expr_p)) {
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

struct FinalizeValueFunctor {
	template <class T>
	static Value FinalizeValue(T first) {
		return Value::CreateValue(first);
	}
};

struct FinalizeStringValueFunctor {
	template <class T>
	static Value FinalizeValue(T first) {
		string_t value = first;
		return Value::CreateValue(value);
	}
};

struct AggregateFunctor {
	template <class OP, class T, class MAP_TYPE = unordered_map<T, idx_t>>
	static void ListExecuteFunction(Vector &result, Vector &state_vector, idx_t count) {
	}
};

struct DistinctFunctor {
	template <class OP, class T, class MAP_TYPE = unordered_map<T, idx_t>>
	static void ListExecuteFunction(Vector &result, Vector &state_vector, idx_t count) {

		UnifiedVectorFormat sdata;
		state_vector.ToUnifiedFormat(count, sdata);
		auto states = (HistogramAggState<T, MAP_TYPE> **)sdata.data;

		auto result_data = FlatVector::GetData<list_entry_t>(result);

		idx_t offset = 0;
		for (idx_t i = 0; i < count; i++) {

			auto state = states[sdata.sel->get_index(i)];
			result_data[i].offset = offset;

			if (!state->hist) {
				result_data[i].length = 0;
				continue;
			}

			result_data[i].length = state->hist->size();
			offset += state->hist->size();

			for (auto &entry : *state->hist) {
				Value bucket_value = OP::template FinalizeValue<T>(entry.first);
				ListVector::PushBack(result, bucket_value);
			}
		}
		result.Verify(count);
	}
};

struct UniqueFunctor {
	template <class OP, class T, class MAP_TYPE = unordered_map<T, idx_t>>
	static void ListExecuteFunction(Vector &result, Vector &state_vector, idx_t count) {

		UnifiedVectorFormat sdata;
		state_vector.ToUnifiedFormat(count, sdata);
		auto states = (HistogramAggState<T, MAP_TYPE> **)sdata.data;

		auto result_data = FlatVector::GetData<uint64_t>(result);

		for (idx_t i = 0; i < count; i++) {

			auto state = states[sdata.sel->get_index(i)];

			if (!state->hist) {
				result_data[i] = 0;
				continue;
			}

			result_data[i] = state->hist->size();
		}
		result.Verify(count);
	}
};

template <class FUNCTION_FUNCTOR, bool IS_AGGR = false>
static void ListAggregatesFunction(DataChunk &args, ExpressionState &state, Vector &result) {

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
	AggregateInputData aggr_input_data(aggr.bind_info.get());

	D_ASSERT(aggr.function.update);

	auto lists_size = ListVector::GetListSize(lists);
	auto &child_vector = ListVector::GetEntry(lists);

	UnifiedVectorFormat child_data;
	child_vector.ToUnifiedFormat(lists_size, child_data);

	UnifiedVectorFormat lists_data;
	lists.ToUnifiedFormat(count, lists_data);
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

		for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {

			// states vector is full, update
			if (states_idx == STANDARD_VECTOR_SIZE) {

				// update the aggregate state(s)
				Vector slice = Vector(child_vector, sel_vector, states_idx);
				aggr.function.update(&slice, aggr_input_data, 1, state_vector_update, states_idx);

				// reset values
				states_idx = 0;
			}

			auto source_idx = child_data.sel->get_index(list_entry.offset + child_idx);
			sel_vector.set_index(states_idx, source_idx);
			states_update[states_idx] = state_ptr;
			states_idx++;
		}
	}

	// update the remaining elements of the last list(s)
	if (states_idx != 0) {
		Vector slice = Vector(child_vector, sel_vector, states_idx);
		aggr.function.update(&slice, aggr_input_data, 1, state_vector_update, states_idx);
	}

	if (IS_AGGR) {
		// finalize all the aggregate states
		aggr.function.finalize(state_vector.state_vector, aggr_input_data, result, count, 0);

	} else {
		// finalize manually to use the map
		D_ASSERT(aggr.function.arguments.size() == 1);
		auto key_type = aggr.function.arguments[0];

		switch (key_type.InternalType()) {
		case PhysicalType::BOOL:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, bool>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::UINT8:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, uint8_t>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::UINT16:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, uint16_t>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::UINT32:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, uint32_t>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::UINT64:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, uint64_t>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::INT8:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, int8_t>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::INT16:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, int16_t>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::INT32:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, int32_t>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::INT64:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, int64_t>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::FLOAT:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, float>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::DOUBLE:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, double>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::DATE32:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, int32_t>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::DATE64:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, int64_t>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::TIMESTAMP:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, int64_t>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::TIME32:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, int32_t>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::TIME64:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, int64_t>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::STRING:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeStringValueFunctor, string>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::LARGE_STRING:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeStringValueFunctor, string>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::VARCHAR:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeStringValueFunctor, string>(
			    result, state_vector.state_vector, count);
			break;
		default:
			throw InternalException("Unimplemented histogram aggregate");
		}
	}
}

static void ListAggregateFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	D_ASSERT(args.ColumnCount() == 2);
	ListAggregatesFunction<AggregateFunctor, true>(args, state, result);
}

static void ListDistinctFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	D_ASSERT(args.ColumnCount() == 1);
	ListAggregatesFunction<DistinctFunctor>(args, state, result);
}

static void ListUniqueFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	D_ASSERT(args.ColumnCount() == 1);
	ListAggregatesFunction<UniqueFunctor>(args, state, result);
}

template <bool IS_AGGR = false>
static unique_ptr<FunctionData> ListAggregatesBindFunction(ClientContext &context, ScalarFunction &bound_function,
                                                           const LogicalType &list_child_type,
                                                           AggregateFunction &aggr_function) {

	// create the child expression and its type
	vector<unique_ptr<Expression>> children;
	auto expr = make_unique<BoundConstantExpression>(Value(list_child_type));
	children.push_back(move(expr));

	auto bound_aggr_function = AggregateFunction::BindAggregateFunction(context, aggr_function, move(children));
	bound_function.arguments[0] = LogicalType::LIST(bound_aggr_function->function.arguments[0]);

	if (IS_AGGR) {
		bound_function.return_type = bound_aggr_function->function.return_type;
	}

	return make_unique<ListAggregatesBindData>(bound_function.return_type, move(bound_aggr_function));
}

template <bool IS_AGGR = false>
static unique_ptr<FunctionData> ListAggregatesBind(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	if (arguments[0]->return_type.id() == LogicalTypeId::SQLNULL) {
		bound_function.arguments[0] = LogicalType::SQLNULL;
		bound_function.return_type = LogicalType::SQLNULL;
		return make_unique<VariableReturnBindData>(bound_function.return_type);
	}

	bool is_parameter = arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN;
	auto list_child_type = is_parameter ? LogicalTypeId::UNKNOWN : ListType::GetChildType(arguments[0]->return_type);

	string function_name = "histogram";
	if (IS_AGGR) { // get the name of the aggregate function

		if (!arguments[1]->IsFoldable()) {
			throw InvalidInputException("Aggregate function name must be a constant");
		}
		// get the function name
		Value function_value = ExpressionExecutor::EvaluateScalar(*arguments[1]);
		function_name = function_value.ToString();
	}

	// look up the aggregate function in the catalog
	QueryErrorContext error_context(nullptr, 0);
	auto func = (AggregateFunctionCatalogEntry *)Catalog::GetCatalog(context).GetEntry<AggregateFunctionCatalogEntry>(
	    context, DEFAULT_SCHEMA, function_name, false, error_context);
	D_ASSERT(func->type == CatalogType::AGGREGATE_FUNCTION_ENTRY);

	if (is_parameter) {
		bound_function.arguments[0] = LogicalTypeId::UNKNOWN;
		bound_function.return_type = LogicalType::SQLNULL;
		return nullptr;
	}

	// find a matching aggregate function
	string error;
	vector<LogicalType> types;
	types.push_back(list_child_type);
	auto best_function_idx = Function::BindFunction(func->name, func->functions, types, error);
	if (best_function_idx == DConstants::INVALID_INDEX) {
		throw BinderException("No matching aggregate function");
	}

	// found a matching function, bind it as an aggregate
	auto &best_function = func->functions[best_function_idx];

	if (IS_AGGR) {
		return ListAggregatesBindFunction<IS_AGGR>(context, bound_function, list_child_type, best_function);
	}

	// create the unordered map histogram function
	D_ASSERT(best_function.arguments.size() == 1);
	auto key_type = best_function.arguments[0];
	auto aggr_function = HistogramFun::GetHistogramUnorderedMap(key_type);
	return ListAggregatesBindFunction<IS_AGGR>(context, bound_function, list_child_type, aggr_function);
}

static unique_ptr<FunctionData> ListAggregateBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {

	// the list column and the name of the aggregate function
	D_ASSERT(bound_function.arguments.size() == 2);
	D_ASSERT(arguments.size() == 2);

	return ListAggregatesBind<true>(context, bound_function, arguments);
}

static unique_ptr<FunctionData> ListDistinctBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {

	D_ASSERT(bound_function.arguments.size() == 1);
	D_ASSERT(arguments.size() == 1);
	bound_function.return_type = arguments[0]->return_type;

	return ListAggregatesBind<>(context, bound_function, arguments);
}

static unique_ptr<FunctionData> ListUniqueBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {

	D_ASSERT(bound_function.arguments.size() == 1);
	D_ASSERT(arguments.size() == 1);
	bound_function.return_type = LogicalType::UBIGINT;

	return ListAggregatesBind<>(context, bound_function, arguments);
}

ScalarFunction ListAggregateFun::GetFunction() {
	auto result = ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::VARCHAR}, LogicalType::ANY,
	                             ListAggregateFunction, ListAggregateBind);
	result.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return result;
}

ScalarFunction ListDistinctFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY)}, LogicalType::LIST(LogicalType::ANY),
	                      ListDistinctFunction, ListDistinctBind);
}

ScalarFunction ListUniqueFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY)}, LogicalType::UBIGINT, ListUniqueFunction,
	                      ListUniqueBind);
}

void ListAggregateFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_aggregate", "array_aggregate", "list_aggr", "array_aggr"}, GetFunction());
}

void ListDistinctFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_distinct", "array_distinct"}, GetFunction());
}

void ListUniqueFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_unique", "array_unique"}, GetFunction());
}

} // namespace duckdb
