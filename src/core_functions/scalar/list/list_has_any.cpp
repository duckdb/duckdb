#include "duckdb/core_functions/lambda_functions.hpp"
#include "duckdb/core_functions/scalar/list_functions.hpp"
#include "duckdb/core_functions/create_sort_key.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/common/string_map_set.hpp"

namespace duckdb {

static unique_ptr<FunctionData> ListHasAnyBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {

	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));
	arguments[1] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[1]));

	const auto left_type = ListType::GetChildType(arguments[0]->return_type);
	const auto right_type = ListType::GetChildType(arguments[1]->return_type);

	if (left_type != LogicalType::SQLNULL && right_type != LogicalType::SQLNULL && left_type != right_type) {
		LogicalType common_type;
		if (LogicalType::TryGetMaxLogicalType(context, arguments[0]->return_type, arguments[1]->return_type,
		                                      common_type)) {
			arguments[0] = BoundCastExpression::AddCastToType(context, std::move(arguments[0]), common_type);
			arguments[1] = BoundCastExpression::AddCastToType(context, std::move(arguments[1]), common_type);
		} else {
			throw BinderException("ListHasAny: cannot compare lists of different types: '%s' and '%s'",
			                      left_type.ToString(), right_type.ToString());
		}
	}

	return nullptr;
}

static void ListHasAnyFunction(DataChunk &args, ExpressionState &, Vector &result) {

	auto &l_vec = args.data[0];
	auto &r_vec = args.data[1];

	if (ListType::GetChildType(l_vec.GetType()) == LogicalType::SQLNULL ||
	    ListType::GetChildType(r_vec.GetType()) == LogicalType::SQLNULL) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::GetData<bool>(result)[0] = false;
		return;
	}

	const auto l_size = ListVector::GetListSize(l_vec);
	const auto r_size = ListVector::GetListSize(r_vec);

	auto &l_child = ListVector::GetEntry(l_vec);
	auto &r_child = ListVector::GetEntry(r_vec);

	// Setup unified formats for the list elements
	UnifiedVectorFormat l_child_format;
	UnifiedVectorFormat r_child_format;

	l_child.ToUnifiedFormat(l_size, l_child_format);
	r_child.ToUnifiedFormat(r_size, r_child_format);

	// Create the sort keys for the list elements
	Vector l_sortkey_vec(LogicalType::BLOB, l_size);
	Vector r_sortkey_vec(LogicalType::BLOB, r_size);

	const OrderModifiers order_modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);

	CreateSortKeyHelpers::CreateSortKey(l_child, l_size, order_modifiers, l_sortkey_vec);
	CreateSortKeyHelpers::CreateSortKey(r_child, r_size, order_modifiers, r_sortkey_vec);

	const auto l_sortkey_ptr = FlatVector::GetData<string_t>(l_sortkey_vec);
	const auto r_sortkey_ptr = FlatVector::GetData<string_t>(r_sortkey_vec);

	string_set_t set;

	BinaryExecutor::Execute<list_entry_t, list_entry_t, bool>(
	    l_vec, r_vec, result, args.size(), [&](const list_entry_t &l_list, const list_entry_t &r_list) {
		    // Short circuit if either list is empty
		    if (l_list.length == 0 || r_list.length == 0) {
			    return false;
		    }

		    auto build_list = l_list;
		    auto probe_list = r_list;

		    auto build_data = l_sortkey_ptr;
		    auto probe_data = r_sortkey_ptr;

		    auto build_format = &l_child_format;
		    auto probe_format = &r_child_format;

		    // Use the smaller list to build the set
		    if (r_list.length < l_list.length) {

			    build_list = r_list;
			    probe_list = l_list;

			    build_data = r_sortkey_ptr;
			    probe_data = l_sortkey_ptr;

			    build_format = &r_child_format;
			    probe_format = &l_child_format;
		    }

		    // Reset the set
		    set.clear();

		    // Build the set
		    for (auto idx = build_list.offset; idx < build_list.offset + build_list.length; idx++) {
			    const auto entry_idx = build_format->sel->get_index(idx);
			    if (build_format->validity.RowIsValid(entry_idx)) {
				    set.insert(build_data[entry_idx]);
			    }
		    }
		    // Probe the set
		    for (auto idx = probe_list.offset; idx < probe_list.offset + probe_list.length; idx++) {
			    const auto entry_idx = probe_format->sel->get_index(idx);
			    if (probe_format->validity.RowIsValid(entry_idx) && set.find(probe_data[entry_idx]) != set.end()) {
				    return true;
			    }
		    }
		    return false;
	    });
}

ScalarFunction ListHasAnyFun::GetFunction() {
	ScalarFunction fun({LogicalType::LIST(LogicalType::ANY), LogicalType::LIST(LogicalType::ANY)}, LogicalType::BOOLEAN,
	                   ListHasAnyFunction, ListHasAnyBind);
	return fun;
}

} // namespace duckdb
