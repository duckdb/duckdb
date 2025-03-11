#include "core_functions/scalar/list_functions.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/sort/sort.hpp"

namespace duckdb {

struct ListSortBindData : public FunctionData {
	ListSortBindData(OrderType order_type_p, OrderByNullType null_order_p, bool is_grade_up,
	                 const LogicalType &return_type_p, const LogicalType &child_type_p, ClientContext &context_p);
	~ListSortBindData() override;

	OrderType order_type;
	OrderByNullType null_order;
	LogicalType return_type;
	LogicalType child_type;
	bool is_grade_up;

	vector<LogicalType> types;
	vector<LogicalType> payload_types;

	ClientContext &context;
	RowLayout payload_layout;
	vector<BoundOrderByNode> orders;

public:
	bool Equals(const FunctionData &other_p) const override;
	unique_ptr<FunctionData> Copy() const override;
};

ListSortBindData::ListSortBindData(OrderType order_type_p, OrderByNullType null_order_p, bool is_grade_up_p,
                                   const LogicalType &return_type_p, const LogicalType &child_type_p,
                                   ClientContext &context_p)
    : order_type(order_type_p), null_order(null_order_p), return_type(return_type_p), child_type(child_type_p),
      is_grade_up(is_grade_up_p), context(context_p) {

	// get the vector types
	types.emplace_back(LogicalType::USMALLINT);
	types.emplace_back(child_type);
	D_ASSERT(types.size() == 2);

	// get the payload types
	payload_types.emplace_back(LogicalType::UINTEGER);
	D_ASSERT(payload_types.size() == 1);

	// initialize the payload layout
	payload_layout.Initialize(payload_types);

	// get the BoundOrderByNode
	auto idx_col_expr = make_uniq_base<Expression, BoundReferenceExpression>(LogicalType::USMALLINT, 0U);
	auto lists_col_expr = make_uniq_base<Expression, BoundReferenceExpression>(child_type, 1U);
	orders.emplace_back(OrderType::ASCENDING, OrderByNullType::ORDER_DEFAULT, std::move(idx_col_expr));
	orders.emplace_back(order_type, null_order, std::move(lists_col_expr));
}

unique_ptr<FunctionData> ListSortBindData::Copy() const {
	return make_uniq<ListSortBindData>(order_type, null_order, is_grade_up, return_type, child_type, context);
}

bool ListSortBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<ListSortBindData>();
	return order_type == other.order_type && null_order == other.null_order && is_grade_up == other.is_grade_up;
}

ListSortBindData::~ListSortBindData() {
}

// create the key_chunk and the payload_chunk and sink them into the local_sort_state
void SinkDataChunk(Vector *child_vector, SelectionVector &sel, idx_t offset_lists_indices, vector<LogicalType> &types,
                   vector<LogicalType> &payload_types, Vector &payload_vector, LocalSortState &local_sort_state,
                   bool &data_to_sort, Vector &lists_indices) {

	// slice the child vector
	Vector slice(*child_vector, sel, offset_lists_indices);

	// initialize and fill key_chunk
	DataChunk key_chunk;
	key_chunk.InitializeEmpty(types);
	key_chunk.data[0].Reference(lists_indices);
	key_chunk.data[1].Reference(slice);
	key_chunk.SetCardinality(offset_lists_indices);

	// initialize and fill key_chunk and payload_chunk
	DataChunk payload_chunk;
	payload_chunk.InitializeEmpty(payload_types);
	payload_chunk.data[0].Reference(payload_vector);
	payload_chunk.SetCardinality(offset_lists_indices);

	key_chunk.Verify();
	payload_chunk.Verify();

	// sink
	key_chunk.Flatten();
	local_sort_state.SinkChunk(key_chunk, payload_chunk);
	data_to_sort = true;
}

static void ListSortFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() >= 1 && args.ColumnCount() <= 3);
	auto count = args.size();
	Vector &input_lists = args.data[0];

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_validity = FlatVector::Validity(result);

	if (input_lists.GetType().id() == LogicalTypeId::SQLNULL) {
		result_validity.SetInvalid(0);
		return;
	}

	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<ListSortBindData>();

	// initialize the global and local sorting state
	auto &buffer_manager = BufferManager::GetBufferManager(info.context);
	GlobalSortState global_sort_state(info.context, info.orders, info.payload_layout);
	LocalSortState local_sort_state;
	local_sort_state.Initialize(global_sort_state, buffer_manager);

	Vector sort_result_vec = info.is_grade_up ? Vector(input_lists.GetType()) : result;

	// this ensures that we do not change the order of the entries in the input chunk
	VectorOperations::Copy(input_lists, sort_result_vec, count, 0, 0);

	// get the child vector
	auto lists_size = ListVector::GetListSize(sort_result_vec);
	auto &child_vector = ListVector::GetEntry(sort_result_vec);

	// get the lists data
	UnifiedVectorFormat lists_data;
	sort_result_vec.ToUnifiedFormat(count, lists_data);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(lists_data);

	// create the lists_indices vector, this contains an element for each list's entry,
	// the element corresponds to the list's index, e.g. for [1, 2, 4], [5, 4]
	// lists_indices contains [0, 0, 0, 1, 1]
	Vector lists_indices(LogicalType::USMALLINT);
	auto lists_indices_data = FlatVector::GetData<uint16_t>(lists_indices);

	// create the payload_vector, this is just a vector containing incrementing integers
	// this will later be used as the 'new' selection vector of the child_vector, after
	// rearranging the payload according to the sorting order
	Vector payload_vector(LogicalType::UINTEGER);
	auto payload_vector_data = FlatVector::GetData<uint32_t>(payload_vector);

	// selection vector pointing to the data of the child vector,
	// used for slicing the child_vector correctly
	SelectionVector sel(STANDARD_VECTOR_SIZE);

	idx_t offset_lists_indices = 0;
	uint32_t incr_payload_count = 0;
	bool data_to_sort = false;

	for (idx_t i = 0; i < count; i++) {
		auto lists_index = lists_data.sel->get_index(i);
		const auto &list_entry = list_entries[lists_index];

		// nothing to do for this list
		if (!lists_data.validity.RowIsValid(lists_index)) {
			result_validity.SetInvalid(i);
			continue;
		}

		// empty list, no sorting required
		if (list_entry.length == 0) {
			continue;
		}

		for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
			// lists_indices vector is full, sink
			if (offset_lists_indices == STANDARD_VECTOR_SIZE) {
				SinkDataChunk(&child_vector, sel, offset_lists_indices, info.types, info.payload_types, payload_vector,
				              local_sort_state, data_to_sort, lists_indices);
				offset_lists_indices = 0;
			}

			auto source_idx = list_entry.offset + child_idx;
			sel.set_index(offset_lists_indices, source_idx);
			lists_indices_data[offset_lists_indices] = UnsafeNumericCast<uint16_t>(i);
			payload_vector_data[offset_lists_indices] = NumericCast<uint32_t>(source_idx);
			offset_lists_indices++;
			incr_payload_count++;
		}
	}

	if (offset_lists_indices != 0) {
		SinkDataChunk(&child_vector, sel, offset_lists_indices, info.types, info.payload_types, payload_vector,
		              local_sort_state, data_to_sort, lists_indices);
	}

	if (info.is_grade_up) {
		ListVector::Reserve(result, lists_size);
		ListVector::SetListSize(result, lists_size);
		auto result_data = ListVector::GetData(result);
		memcpy(result_data, list_entries, count * sizeof(list_entry_t));
	}

	if (data_to_sort) {
		// add local state to global state, which sorts the data
		global_sort_state.AddLocalState(local_sort_state);
		global_sort_state.PrepareMergePhase();

		// selection vector that is to be filled with the 'sorted' payload
		SelectionVector sel_sorted(incr_payload_count);
		idx_t sel_sorted_idx = 0;

		// scan the sorted row data
		PayloadScanner scanner(*global_sort_state.sorted_blocks[0]->payload_data, global_sort_state);
		for (;;) {
			DataChunk result_chunk;
			result_chunk.Initialize(Allocator::DefaultAllocator(), info.payload_types);
			result_chunk.SetCardinality(0);
			scanner.Scan(result_chunk);
			if (result_chunk.size() == 0) {
				break;
			}

			// construct the selection vector with the new order from the result vectors
			Vector result_vector(result_chunk.data[0]);
			auto result_data = FlatVector::GetData<uint32_t>(result_vector);
			auto row_count = result_chunk.size();

			for (idx_t i = 0; i < row_count; i++) {
				sel_sorted.set_index(sel_sorted_idx, result_data[i]);
				D_ASSERT(result_data[i] < lists_size);
				sel_sorted_idx++;
			}
		}

		D_ASSERT(sel_sorted_idx == incr_payload_count);
		if (info.is_grade_up) {
			auto &result_entry = ListVector::GetEntry(result);
			auto result_data = ListVector::GetData(result);
			for (idx_t i = 0; i < count; i++) {
				if (!result_validity.RowIsValid(i)) {
					continue;
				}
				for (idx_t j = result_data[i].offset; j < result_data[i].offset + result_data[i].length; j++) {
					auto b = sel_sorted.get_index(j) - result_data[i].offset;
					result_entry.SetValue(j, Value::BIGINT(UnsafeNumericCast<int64_t>(b + 1)));
				}
			}
		} else {
			child_vector.Slice(sel_sorted, sel_sorted_idx);
			child_vector.Flatten(sel_sorted_idx);
		}
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> ListSortBind(ClientContext &context, ScalarFunction &bound_function,
                                             vector<unique_ptr<Expression>> &arguments, OrderType &order,
                                             OrderByNullType &null_order) {

	LogicalType child_type;
	if (arguments[0]->return_type == LogicalTypeId::UNKNOWN) {
		bound_function.arguments[0] = LogicalTypeId::UNKNOWN;
		bound_function.return_type = LogicalType::SQLNULL;
		child_type = bound_function.return_type;
		return make_uniq<ListSortBindData>(order, null_order, false, bound_function.return_type, child_type, context);
	}

	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));
	child_type = ListType::GetChildType(arguments[0]->return_type);

	bound_function.arguments[0] = arguments[0]->return_type;
	bound_function.return_type = arguments[0]->return_type;

	return make_uniq<ListSortBindData>(order, null_order, false, bound_function.return_type, child_type, context);
}

template <class T>
static T GetOrder(ClientContext &context, Expression &expr) {
	if (!expr.IsFoldable()) {
		throw InvalidInputException("Sorting order must be a constant");
	}
	Value order_value = ExpressionExecutor::EvaluateScalar(context, expr);
	auto order_name = StringUtil::Upper(order_value.ToString());
	return EnumUtil::FromString<T>(order_name.c_str());
}

static unique_ptr<FunctionData> ListGradeUpBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {

	D_ASSERT(!arguments.empty() && arguments.size() <= 3);
	auto order = OrderType::ORDER_DEFAULT;
	auto null_order = OrderByNullType::ORDER_DEFAULT;

	// get the sorting order
	if (arguments.size() >= 2) {
		order = GetOrder<OrderType>(context, *arguments[1]);
	}
	// get the null sorting order
	if (arguments.size() == 3) {
		null_order = GetOrder<OrderByNullType>(context, *arguments[2]);
	}
	auto &config = DBConfig::GetConfig(context);
	order = config.ResolveOrder(order);
	null_order = config.ResolveNullOrder(order, null_order);

	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));

	bound_function.arguments[0] = arguments[0]->return_type;
	bound_function.return_type = LogicalType::LIST(LogicalTypeId::BIGINT);
	auto child_type = ListType::GetChildType(arguments[0]->return_type);
	return make_uniq<ListSortBindData>(order, null_order, true, bound_function.return_type, child_type, context);
}

static unique_ptr<FunctionData> ListNormalSortBind(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(!arguments.empty() && arguments.size() <= 3);
	auto order = OrderType::ORDER_DEFAULT;
	auto null_order = OrderByNullType::ORDER_DEFAULT;

	// get the sorting order
	if (arguments.size() >= 2) {
		order = GetOrder<OrderType>(context, *arguments[1]);
	}
	// get the null sorting order
	if (arguments.size() == 3) {
		null_order = GetOrder<OrderByNullType>(context, *arguments[2]);
	}
	auto &config = DBConfig::GetConfig(context);
	order = config.ResolveOrder(order);
	null_order = config.ResolveNullOrder(order, null_order);
	return ListSortBind(context, bound_function, arguments, order, null_order);
}

static unique_ptr<FunctionData> ListReverseSortBind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {
	auto order = OrderType::ORDER_DEFAULT;
	auto null_order = OrderByNullType::ORDER_DEFAULT;

	if (arguments.size() == 2) {
		null_order = GetOrder<OrderByNullType>(context, *arguments[1]);
	}
	auto &config = DBConfig::GetConfig(context);
	order = config.ResolveOrder(order);
	switch (order) {
	case OrderType::ASCENDING:
		order = OrderType::DESCENDING;
		break;
	case OrderType::DESCENDING:
		order = OrderType::ASCENDING;
		break;
	default:
		throw InternalException("Unexpected order type in list reverse sort");
	}
	null_order = config.ResolveNullOrder(order, null_order);
	return ListSortBind(context, bound_function, arguments, order, null_order);
}

ScalarFunctionSet ListSortFun::GetFunctions() {
	// one parameter: list
	ScalarFunction sort({LogicalType::LIST(LogicalType::ANY)}, LogicalType::LIST(LogicalType::ANY), ListSortFunction,
	                    ListNormalSortBind);

	// two parameters: list, order
	ScalarFunction sort_order({LogicalType::LIST(LogicalType::ANY), LogicalType::VARCHAR},
	                          LogicalType::LIST(LogicalType::ANY), ListSortFunction, ListNormalSortBind);

	// three parameters: list, order, null order
	ScalarFunction sort_orders({LogicalType::LIST(LogicalType::ANY), LogicalType::VARCHAR, LogicalType::VARCHAR},
	                           LogicalType::LIST(LogicalType::ANY), ListSortFunction, ListNormalSortBind);

	ScalarFunctionSet list_sort;
	list_sort.AddFunction(sort);
	list_sort.AddFunction(sort_order);
	list_sort.AddFunction(sort_orders);
	return list_sort;
}

ScalarFunctionSet ListGradeUpFun::GetFunctions() {
	// one parameter: list
	ScalarFunction sort({LogicalType::LIST(LogicalType::ANY)}, LogicalType::LIST(LogicalType::ANY), ListSortFunction,
	                    ListGradeUpBind);

	// two parameters: list, order
	ScalarFunction sort_order({LogicalType::LIST(LogicalType::ANY), LogicalType::VARCHAR},
	                          LogicalType::LIST(LogicalType::ANY), ListSortFunction, ListGradeUpBind);

	// three parameters: list, order, null order
	ScalarFunction sort_orders({LogicalType::LIST(LogicalType::ANY), LogicalType::VARCHAR, LogicalType::VARCHAR},
	                           LogicalType::LIST(LogicalType::ANY), ListSortFunction, ListGradeUpBind);

	ScalarFunctionSet list_grade_up;
	list_grade_up.AddFunction(sort);
	list_grade_up.AddFunction(sort_order);
	list_grade_up.AddFunction(sort_orders);
	return list_grade_up;
}

ScalarFunctionSet ListReverseSortFun::GetFunctions() {
	// one parameter: list
	ScalarFunction sort_reverse({LogicalType::LIST(LogicalType::ANY)}, LogicalType::LIST(LogicalType::ANY),
	                            ListSortFunction, ListReverseSortBind);

	// two parameters: list, null order
	ScalarFunction sort_reverse_null_order({LogicalType::LIST(LogicalType::ANY), LogicalType::VARCHAR},
	                                       LogicalType::LIST(LogicalType::ANY), ListSortFunction, ListReverseSortBind);

	ScalarFunctionSet list_reverse_sort;
	list_reverse_sort.AddFunction(sort_reverse);
	list_reverse_sort.AddFunction(sort_reverse_null_order);
	return list_reverse_sort;
}

} // namespace duckdb
