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

	Vector l_sortkey_vec(LogicalType::BLOB, l_size);
	Vector r_sortkey_vec(LogicalType::BLOB, r_size);

	const OrderModifiers order_modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);
	CreateSortKeyHelpers::CreateSortKey(ListVector::GetEntry(l_vec), l_size, order_modifiers, l_sortkey_vec);
	CreateSortKeyHelpers::CreateSortKey(ListVector::GetEntry(r_vec), r_size, order_modifiers, r_sortkey_vec);

	const auto l_validity_ptr = &FlatVector::Validity(ListVector::GetEntry(l_vec));
	const auto r_validity_ptr = &FlatVector::Validity(ListVector::GetEntry(r_vec));

	const auto l_ptr = FlatVector::GetData<string_t>(l_sortkey_vec);
	const auto r_ptr = FlatVector::GetData<string_t>(r_sortkey_vec);

	string_set_t set;

	BinaryExecutor::Execute<list_entry_t, list_entry_t, bool>(
	    l_vec, r_vec, result, args.size(), [&](const list_entry_t &l_val, const list_entry_t &r_val) {
		    // Short circuit if either list is empty
		    if (l_val.length == 0 || r_val.length == 0) {
			    return false;
		    }

		    auto build_ptr = l_ptr;
		    auto probe_ptr = r_ptr;
		    auto build_val = l_val;
		    auto probe_val = r_val;

		    auto build_validity = l_validity_ptr;
		    auto probe_validity = r_validity_ptr;

		    // Use the smaller list to build the set
		    if (probe_val.length < build_val.length) {
			    std::swap(build_ptr, probe_ptr);
			    std::swap(build_val, probe_val);
			    std::swap(build_validity, probe_validity);
		    }

		    set.clear();
		    for (auto i = build_val.offset; i < build_val.offset + build_val.length; i++) {
			    if (build_validity->RowIsValid(i)) {
				    set.insert(build_ptr[i]);
			    }
		    }
		    for (auto i = probe_val.offset; i < probe_val.offset + probe_val.length; i++) {
			    if (probe_validity->RowIsValid(i) && set.find(probe_ptr[i]) != set.end()) {
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
