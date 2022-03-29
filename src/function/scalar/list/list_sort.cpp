#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

struct ListSortBindData : public FunctionData {
	ListSortBindData(OrderType order_type_p, OrderByNullType null_order_p, LogicalType &return_type_p,
	                 LogicalType &child_type_p);
	~ListSortBindData() override;

	OrderType order_type;
	OrderByNullType null_order;
	LogicalType return_type;
	LogicalType child_type;

	unique_ptr<FunctionData> Copy() override;
};

ListSortBindData::ListSortBindData(OrderType order_type_p, OrderByNullType null_order_p, LogicalType &return_type_p,
                                   LogicalType &child_type_p)
    : order_type(order_type_p), null_order(null_order_p), return_type(return_type_p), child_type(child_type_p) {
}

unique_ptr<FunctionData> ListSortBindData::Copy() {
	return make_unique<ListSortBindData>(order_type, null_order, return_type, child_type);
}

ListSortBindData::~ListSortBindData() {
}

static void ListSortFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	D_ASSERT(args.ColumnCount() == 1 || args.ColumnCount() == 3);
	auto count = args.size();
	Vector &lists = args.data[0];

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_validity = FlatVector::Validity(result);
	auto result_entries = FlatVector::GetData<list_entry_t>(result);

	if (lists.GetType().id() == LogicalTypeId::SQLNULL) {
		result_validity.SetInvalid(0);
		return;
	}

	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (ListSortBindData &)*func_expr.bind_info;

	// get the order, null order and data type
	vector<OrderType> order_types;
	order_types.emplace_back(info.order_type);
	vector<OrderByNullType> null_orders;
	null_orders.emplace_back(info.null_order);
	vector<LogicalType> child_types;
	child_types.emplace_back(info.child_type);

	D_ASSERT(child_types.size() == 1);

	// get the child vector
	auto lists_size = ListVector::GetListSize(lists);
	auto &child_vector = ListVector::GetEntry(lists);
	VectorData child_data;
	child_vector.Orrify(lists_size, child_data);

	// get the lists data
	VectorData lists_data;
	lists.Orrify(count, lists_data);
	auto list_entries = (list_entry_t *)lists_data.data;

	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {

		auto lists_index = lists_data.sel->get_index(i);
		const auto &list_entry = list_entries[lists_index];
		result_entries[i].offset = offset;
		result_entries[i].length = 0;

		// nothing to do for this list
		if (!lists_data.validity.RowIsValid(lists_index)) {
			result_validity.SetInvalid(i);
			continue;
		}

		// empty list, no sorting required
		if (list_entry.length == 0) {
			continue;
		}

		// slice exactly the current list
		SelectionVector sel_vector(list_entry.length);
		auto source_idx = child_data.sel->get_index(list_entry.offset);
		for (idx_t i = 0; i < list_entry.length; i++) {
			sel_vector.set_index(i, source_idx + i);
		}
		Vector slice = Vector(child_vector, sel_vector, list_entry.length);

		// append the slice to an empty data chunk
		DataChunk data_chunk;
		data_chunk.InitializeEmpty(child_types);
		data_chunk.data[0].Reference(slice);
		data_chunk.SetCardinality(list_entry.length);

		// append the data chunk to a chunk collection
		ChunkCollection chunk_collection;
		chunk_collection.Append(data_chunk);

		// sort the data
		vector<idx_t> reordering; // sorting buffer
		reordering.resize(chunk_collection.Count());
		chunk_collection.Sort(order_types, null_orders, reordering.data());
		chunk_collection.Reorder(reordering.data());

		// append the sorted data to the result vector
		result_entries[i].length += list_entry.length;
		offset += result_entries[i].length;
		auto sorted_vector = chunk_collection.GetChunk(0).data.data();
		ListVector::Append(result, *sorted_vector, list_entry.length);
	}

	D_ASSERT(ListVector::GetListSize(result) == offset);
}

static unique_ptr<FunctionData> ListSortBind(ClientContext &context, ScalarFunction &bound_function,
                                             vector<unique_ptr<Expression>> &arguments) {

	D_ASSERT(bound_function.arguments.size() == 1 || bound_function.arguments.size() == 3);
	D_ASSERT(arguments.size() == 1 || arguments.size() == 3);

	if (arguments[0]->return_type.id() == LogicalTypeId::SQLNULL) {
		bound_function.arguments[0] = LogicalType::SQLNULL;
		bound_function.return_type = LogicalType::SQLNULL;
		return make_unique<VariableReturnBindData>(bound_function.return_type);
	}

	bound_function.arguments[0] = arguments[0]->return_type;
	bound_function.return_type = arguments[0]->return_type;
	auto child_type = ListType::GetChildType(arguments[0]->return_type);

	// mimic SQLite default behavior
	auto order = OrderType::ASCENDING;
	auto null_order = OrderByNullType::NULLS_FIRST;

	// get custom order and null order
	if (arguments.size() == 3) {
		if (!arguments[1]->IsFoldable()) {
			throw InvalidInputException("Sorting order must be a constant");
		}
		if (!arguments[2]->IsFoldable()) {
			throw InvalidInputException("Null sorting order must be a constant");
		}

		Value order_value = ExpressionExecutor::EvaluateScalar(*arguments[1]);
		auto order_name = order_value.ToString();
		std::transform(order_name.begin(), order_name.end(), order_name.begin(), ::toupper);
		if (order_name != "DESC" && order_name != "ASC") {
			throw InvalidInputException("Sorting order must be either ASC or DESC");
		}

		Value null_order_value = ExpressionExecutor::EvaluateScalar(*arguments[2]);
		auto null_order_name = null_order_value.ToString();
		std::transform(null_order_name.begin(), null_order_name.end(), null_order_name.begin(), ::toupper);
		if (null_order_name != "NULLS FIRST" && null_order_name != "NULLS LAST") {
			throw InvalidInputException("Null sorting order must be either NULLS FIRST or NULLS LAST");
		}

		if (null_order_name == "NULLS LAST") {
			null_order = OrderByNullType::NULLS_LAST;
		}
		// FIXME: according to sorted_aggregate_function.cpp, line 8 the collection sorting does
		// not handle OrderByNullType correctly, so this is a hack
		if (order_name == "DESC") {
			order = OrderType::DESCENDING;
			if (null_order_name == "NULLS LAST") {
				null_order = OrderByNullType::NULLS_FIRST;
			} else {
				null_order = OrderByNullType::NULLS_LAST;
			}
		}
	}

	return make_unique<ListSortBindData>(order, null_order, bound_function.return_type, child_type);
}

void ListSortFun::RegisterFunction(BuiltinFunctions &set) {

	ScalarFunction list({LogicalType::LIST(LogicalType::ANY)}, LogicalType::LIST(LogicalType::ANY), ListSortFunction,
	                    false, ListSortBind, nullptr, nullptr, nullptr);

	ScalarFunction list_custom_order({LogicalType::LIST(LogicalType::ANY), LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                 LogicalType::LIST(LogicalType::ANY), ListSortFunction, false, ListSortBind,
	                                 nullptr, nullptr, nullptr);

	ScalarFunctionSet list_sort("list_sort");
	list_sort.AddFunction(list);
	list_sort.AddFunction(list_custom_order);
	set.AddFunction(list_sort);

	ScalarFunctionSet array_sort("array_sort");
	array_sort.AddFunction(list);
	array_sort.AddFunction(list_custom_order);
	set.AddFunction(array_sort);
}

} // namespace duckdb