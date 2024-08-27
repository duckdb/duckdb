#include "duckdb/core_functions/lambda_functions.hpp"
#include "duckdb/core_functions/scalar/list_functions.hpp"
#include "duckdb/core_functions/create_sort_key.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/common/string_map_set.hpp"

namespace duckdb {

static unique_ptr<FunctionData> ListHasAnyOrAllBind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {

	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));
	arguments[1] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[1]));

	const auto lhs_is_param = arguments[0]->HasParameter();
	const auto rhs_is_param = arguments[1]->HasParameter();

	if (lhs_is_param && rhs_is_param) {
		throw ParameterNotResolvedException();
	}

	const auto &lhs_list = arguments[0]->return_type;
	const auto &rhs_list = arguments[1]->return_type;

	if (lhs_is_param) {
		bound_function.arguments[0] = rhs_list;
		bound_function.arguments[1] = rhs_list;
		return nullptr;
	}
	if (rhs_is_param) {
		bound_function.arguments[0] = lhs_list;
		bound_function.arguments[1] = lhs_list;
		return nullptr;
	}

	bound_function.arguments[0] = lhs_list;
	bound_function.arguments[1] = rhs_list;

	const auto &lhs_child = ListType::GetChildType(bound_function.arguments[0]);
	const auto &rhs_child = ListType::GetChildType(bound_function.arguments[1]);

	if (lhs_child != LogicalType::SQLNULL && rhs_child != LogicalType::SQLNULL && lhs_child != rhs_child) {
		LogicalType common_child;
		if (!LogicalType::TryGetMaxLogicalType(context, lhs_child, rhs_child, common_child)) {
			throw BinderException("'%s' cannot compare lists of different types: '%s' and '%s'", bound_function.name,
			                      lhs_child.ToString(), rhs_child.ToString());
		}
		bound_function.arguments[0] = LogicalType::LIST(common_child);
		bound_function.arguments[1] = LogicalType::LIST(common_child);
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

static void ListHasAllFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	const auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto swap = func_expr.function.name == "<@";

	auto &l_vec = args.data[swap ? 1 : 0];
	auto &r_vec = args.data[swap ? 0 : 1];

	if (ListType::GetChildType(l_vec.GetType()) == LogicalType::SQLNULL &&
	    ListType::GetChildType(r_vec.GetType()) == LogicalType::SQLNULL) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::GetData<bool>(result)[0] = true;
		return;
	}

	const auto l_size = ListVector::GetListSize(l_vec);
	const auto r_size = ListVector::GetListSize(r_vec);

	auto &l_child = ListVector::GetEntry(l_vec);
	auto &r_child = ListVector::GetEntry(r_vec);

	// Setup unified formats for the list elements
	UnifiedVectorFormat build_format;
	UnifiedVectorFormat probe_format;

	l_child.ToUnifiedFormat(l_size, build_format);
	r_child.ToUnifiedFormat(r_size, probe_format);

	// Create the sort keys for the list elements
	Vector l_sortkey_vec(LogicalType::BLOB, l_size);
	Vector r_sortkey_vec(LogicalType::BLOB, r_size);

	const OrderModifiers order_modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);

	CreateSortKeyHelpers::CreateSortKey(l_child, l_size, order_modifiers, l_sortkey_vec);
	CreateSortKeyHelpers::CreateSortKey(r_child, r_size, order_modifiers, r_sortkey_vec);

	const auto build_data = FlatVector::GetData<string_t>(l_sortkey_vec);
	const auto probe_data = FlatVector::GetData<string_t>(r_sortkey_vec);

	string_set_t set;

	BinaryExecutor::Execute<list_entry_t, list_entry_t, bool>(
	    l_vec, r_vec, result, args.size(), [&](const list_entry_t &build_list, const list_entry_t &probe_list) {
		    // Short circuit if the probe list is empty
		    if (probe_list.length == 0) {
			    return true;
		    }

		    // Reset the set
		    set.clear();

		    // Build the set
		    for (auto idx = build_list.offset; idx < build_list.offset + build_list.length; idx++) {
			    const auto entry_idx = build_format.sel->get_index(idx);
			    if (build_format.validity.RowIsValid(entry_idx)) {
				    set.insert(build_data[entry_idx]);
			    }
		    }

		    // Probe the set
		    for (auto idx = probe_list.offset; idx < probe_list.offset + probe_list.length; idx++) {
			    const auto entry_idx = probe_format.sel->get_index(idx);
			    if (probe_format.validity.RowIsValid(entry_idx) && set.find(probe_data[entry_idx]) == set.end()) {
				    return false;
			    }
		    }
		    return true;
	    });
}

ScalarFunction ListHasAnyFun::GetFunction() {
	ScalarFunction fun({LogicalType::LIST(LogicalType::ANY), LogicalType::LIST(LogicalType::ANY)}, LogicalType::BOOLEAN,
	                   ListHasAnyFunction, ListHasAnyOrAllBind);
	return fun;
}

ScalarFunction ListHasAllFun::GetFunction() {
	ScalarFunction fun({LogicalType::LIST(LogicalType::ANY), LogicalType::LIST(LogicalType::ANY)}, LogicalType::BOOLEAN,
	                   ListHasAllFunction, ListHasAnyOrAllBind);
	return fun;
}

} // namespace duckdb
