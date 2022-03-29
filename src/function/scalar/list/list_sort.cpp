#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

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

ListSortBindData::ListSortBindData(OrderType order_type_p, OrderByNullType null_order_p, 
	LogicalType &return_type_p, LogicalType &child_type_p) 
	: order_type(order_type_p), null_order(null_order_p), return_type(return_type_p),
	child_type(child_type_p) {
}

unique_ptr<FunctionData> ListSortBindData::Copy() {
	return make_unique<ListSortBindData>(order_type, null_order, return_type, child_type);
}

ListSortBindData::~ListSortBindData() {
}

static void ListSortFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	D_ASSERT(args.ColumnCount() == 1);
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
	
	D_ASSERT(bound_function.arguments.size() == 1);
	D_ASSERT(arguments.size() == 1);

	if (arguments[0]->return_type.id() == LogicalTypeId::SQLNULL) {
		bound_function.arguments[0] = LogicalType::SQLNULL;
		bound_function.return_type = LogicalType::SQLNULL;
		return make_unique<VariableReturnBindData>(bound_function.return_type);
	}

	bound_function.arguments[0] = arguments[0]->return_type;
	bound_function.return_type = arguments[0]->return_type;

	auto child_type = ListType::GetChildType(arguments[0]->return_type);

	// default values for order and null order
	return make_unique<ListSortBindData>(OrderType::ORDER_DEFAULT, OrderByNullType::ORDER_DEFAULT, 
		bound_function.return_type, child_type);
}

ScalarFunction ListSortFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY)}, LogicalType::LIST(LogicalType::ANY),
	                      ListSortFunction, false, ListSortBind, nullptr, nullptr, nullptr);
}

void ListSortFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_sort", "array_sort"}, GetFunction());
}

} // namespace duckdb