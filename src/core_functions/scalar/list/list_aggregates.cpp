#include "duckdb/core_functions/scalar/list_functions.hpp"
#include "duckdb/core_functions/aggregate/nested_functions.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/function/function_binder.hpp"

namespace duckdb {

// FIXME: use a local state for each thread to increase performance?
// FIXME: benchmark the use of simple_update against using update (if applicable)

static unique_ptr<FunctionData> ListAggregatesBindFailure(ScalarFunction &bound_function) {
	bound_function.arguments[0] = LogicalType::SQLNULL;
	bound_function.return_type = LogicalType::SQLNULL;
	return make_uniq<VariableReturnBindData>(LogicalType::SQLNULL);
}

struct ListAggregatesBindData : public FunctionData {
	ListAggregatesBindData(const LogicalType &stype_p, unique_ptr<Expression> aggr_expr_p);
	~ListAggregatesBindData() override;

	LogicalType stype;
	unique_ptr<Expression> aggr_expr;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ListAggregatesBindData>(stype, aggr_expr->Copy());
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ListAggregatesBindData>();
		return stype == other.stype && aggr_expr->Equals(*other.aggr_expr);
	}
	void Serialize(Serializer &serializer) const {
		serializer.WriteProperty(1, "stype", stype);
		serializer.WriteProperty(2, "aggr_expr", aggr_expr);
	}
	static unique_ptr<ListAggregatesBindData> Deserialize(Deserializer &deserializer) {
		auto stype = deserializer.ReadProperty<LogicalType>(1, "stype");
		auto aggr_expr = deserializer.ReadProperty<unique_ptr<Expression>>(2, "aggr_expr");
		auto result = make_uniq<ListAggregatesBindData>(std::move(stype), std::move(aggr_expr));
		return result;
	}

	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                      const ScalarFunction &function) {
		auto bind_data = dynamic_cast<const ListAggregatesBindData *>(bind_data_p.get());
		serializer.WritePropertyWithDefault(100, "bind_data", bind_data, (const ListAggregatesBindData *)nullptr);
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, ScalarFunction &bound_function) {
		auto result = deserializer.ReadPropertyWithDefault<unique_ptr<ListAggregatesBindData>>(
		    100, "bind_data", unique_ptr<ListAggregatesBindData>(nullptr));
		if (!result) {
			return ListAggregatesBindFailure(bound_function);
		}
		return std::move(result);
	}
};

ListAggregatesBindData::ListAggregatesBindData(const LogicalType &stype_p, unique_ptr<Expression> aggr_expr_p)
    : stype(stype_p), aggr_expr(std::move(aggr_expr_p)) {
}

ListAggregatesBindData::~ListAggregatesBindData() {
}

struct StateVector {
	StateVector(idx_t count_p, unique_ptr<Expression> aggr_expr_p)
	    : count(count_p), aggr_expr(std::move(aggr_expr_p)), state_vector(Vector(LogicalType::POINTER, count_p)) {
	}

	~StateVector() { // NOLINT
		// destroy objects within the aggregate states
		auto &aggr = aggr_expr->Cast<BoundAggregateExpression>();
		if (aggr.function.destructor) {
			ArenaAllocator allocator(Allocator::DefaultAllocator());
			AggregateInputData aggr_input_data(aggr.bind_info.get(), allocator);
			aggr.function.destructor(state_vector, aggr_input_data, count);
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
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<ListAggregatesBindData>();
	auto &aggr = info.aggr_expr->Cast<BoundAggregateExpression>();
	ArenaAllocator allocator(Allocator::DefaultAllocator());
	AggregateInputData aggr_input_data(aggr.bind_info.get(), allocator);

	D_ASSERT(aggr.function.update);

	auto lists_size = ListVector::GetListSize(lists);
	auto &child_vector = ListVector::GetEntry(lists);
	child_vector.Flatten(lists_size);

	UnifiedVectorFormat child_data;
	child_vector.ToUnifiedFormat(lists_size, child_data);

	UnifiedVectorFormat lists_data;
	lists.ToUnifiedFormat(count, lists_data);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(lists_data);

	// state_buffer holds the state for each list of this chunk
	idx_t size = aggr.function.state_size();
	auto state_buffer = make_unsafe_uniq_array<data_t>(size * count);

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
				Vector slice(child_vector, sel_vector, states_idx);
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
		Vector slice(child_vector, sel_vector, states_idx);
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
			if (key_type.id() == LogicalTypeId::DATE) {
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, date_t>(
				    result, state_vector.state_vector, count);
			} else {
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, int32_t>(
				    result, state_vector.state_vector, count);
			}
			break;
		case PhysicalType::INT64:
			switch (key_type.id()) {
			case LogicalTypeId::TIME:
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, dtime_t>(
				    result, state_vector.state_vector, count);
				break;
			case LogicalTypeId::TIME_TZ:
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, dtime_tz_t>(
				    result, state_vector.state_vector, count);
				break;
			case LogicalTypeId::TIMESTAMP:
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, timestamp_t>(
				    result, state_vector.state_vector, count);
				break;
			case LogicalTypeId::TIMESTAMP_MS:
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, timestamp_ms_t>(
				    result, state_vector.state_vector, count);
				break;
			case LogicalTypeId::TIMESTAMP_NS:
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, timestamp_ns_t>(
				    result, state_vector.state_vector, count);
				break;
			case LogicalTypeId::TIMESTAMP_SEC:
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, timestamp_sec_t>(
				    result, state_vector.state_vector, count);
				break;
			case LogicalTypeId::TIMESTAMP_TZ:
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, timestamp_tz_t>(
				    result, state_vector.state_vector, count);
				break;
			default:
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, int64_t>(
				    result, state_vector.state_vector, count);
				break;
			}
			break;
		case PhysicalType::FLOAT:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, float>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::DOUBLE:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, double>(
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

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void ListAggregateFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() >= 2);
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
static unique_ptr<FunctionData>
ListAggregatesBindFunction(ClientContext &context, ScalarFunction &bound_function, const LogicalType &list_child_type,
                           AggregateFunction &aggr_function, vector<unique_ptr<Expression>> &arguments) {

	// create the child expression and its type
	vector<unique_ptr<Expression>> children;
	auto expr = make_uniq<BoundConstantExpression>(Value(list_child_type));
	children.push_back(std::move(expr));
	// push any extra arguments into the list aggregate bind
	if (arguments.size() > 2) {
		for (idx_t i = 2; i < arguments.size(); i++) {
			children.push_back(std::move(arguments[i]));
		}
		arguments.resize(2);
	}

	FunctionBinder function_binder(context);
	auto bound_aggr_function = function_binder.BindAggregateFunction(aggr_function, std::move(children));
	bound_function.arguments[0] = LogicalType::LIST(bound_aggr_function->function.arguments[0]);

	if (IS_AGGR) {
		bound_function.return_type = bound_aggr_function->function.return_type;
	}
	// check if the aggregate function consumed all the extra input arguments
	if (bound_aggr_function->children.size() > 1) {
		throw InvalidInputException(
		    "Aggregate function %s is not supported for list_aggr: extra arguments were not removed during bind",
		    bound_aggr_function->ToString());
	}

	return make_uniq<ListAggregatesBindData>(bound_function.return_type, std::move(bound_aggr_function));
}

template <bool IS_AGGR = false>
static unique_ptr<FunctionData> ListAggregatesBind(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	if (arguments[0]->return_type.id() == LogicalTypeId::SQLNULL) {
		return ListAggregatesBindFailure(bound_function);
	}

	bool is_parameter = arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN;
	auto list_child_type = is_parameter ? LogicalTypeId::UNKNOWN : ListType::GetChildType(arguments[0]->return_type);

	string function_name = "histogram";
	if (IS_AGGR) { // get the name of the aggregate function
		if (!arguments[1]->IsFoldable()) {
			throw InvalidInputException("Aggregate function name must be a constant");
		}
		// get the function name
		Value function_value = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
		function_name = function_value.ToString();
	}

	// look up the aggregate function in the catalog
	QueryErrorContext error_context(nullptr, 0);
	auto &func = Catalog::GetSystemCatalog(context).GetEntry<AggregateFunctionCatalogEntry>(
	    context, DEFAULT_SCHEMA, function_name, error_context);
	D_ASSERT(func.type == CatalogType::AGGREGATE_FUNCTION_ENTRY);

	if (is_parameter) {
		bound_function.arguments[0] = LogicalTypeId::UNKNOWN;
		bound_function.return_type = LogicalType::SQLNULL;
		return nullptr;
	}

	// find a matching aggregate function
	string error;
	vector<LogicalType> types;
	types.push_back(list_child_type);
	// push any extra arguments into the type list
	for (idx_t i = 2; i < arguments.size(); i++) {
		types.push_back(arguments[i]->return_type);
	}

	FunctionBinder function_binder(context);
	auto best_function_idx = function_binder.BindFunction(func.name, func.functions, types, error);
	if (best_function_idx == DConstants::INVALID_INDEX) {
		throw BinderException("No matching aggregate function\n%s", error);
	}

	// found a matching function, bind it as an aggregate
	auto best_function = func.functions.GetFunctionByOffset(best_function_idx);
	if (IS_AGGR) {
		return ListAggregatesBindFunction<IS_AGGR>(context, bound_function, list_child_type, best_function, arguments);
	}

	// create the unordered map histogram function
	D_ASSERT(best_function.arguments.size() == 1);
	auto key_type = best_function.arguments[0];
	auto aggr_function = HistogramFun::GetHistogramUnorderedMap(key_type);
	return ListAggregatesBindFunction<IS_AGGR>(context, bound_function, list_child_type, aggr_function, arguments);
}

static unique_ptr<FunctionData> ListAggregateBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {

	// the list column and the name of the aggregate function
	D_ASSERT(bound_function.arguments.size() >= 2);
	D_ASSERT(arguments.size() >= 2);

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
	result.varargs = LogicalType::ANY;
	result.serialize = ListAggregatesBindData::Serialize;
	result.deserialize = ListAggregatesBindData::Deserialize;
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

} // namespace duckdb
