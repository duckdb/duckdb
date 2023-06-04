#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

static void ListReverseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto count = args.size();

	Vector &input_list = args.data[0];
	if (input_list.GetType().id() == LogicalTypeId::SQLNULL) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}

	// create a copy (result) of the input list
	// this ensures that we do not change the order of the entries in the input chunk
	VectorOperations::Copy(input_list, result, count, 0, 0);

	// get the input list data from the copy.
	UnifiedVectorFormat input_list_data;
	result.ToUnifiedFormat(count, input_list_data);
	auto input_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(input_list_data);

	// get the validity mask of the result to avoid complications later
	auto &result_validity = FlatVector::Validity(result);

	// create a selection vector for slicing the child vector
	SelectionVector rev_sel(ListVector::GetListSize(result));

	for (idx_t i = 0; i < count; i++) {
		auto input_list_list_index = input_list_data.sel->get_index(i);

		// check if the input list is valid
		if (!input_list_data.validity.RowIsValid(input_list_list_index)) {
			result_validity.SetInvalid(i);
			continue;
		};

		D_ASSERT(input_list_data.validity.RowIsValid(input_list_list_index));
		const auto &input_list_entry = input_list_entries[input_list_list_index];

		// populate the selection vector
		// set index of selection vector in a way, that only the entries of the current row are reversed at once
		for (idx_t j = 0; j < input_list_entry.length; j++) {
			rev_sel.set_index(j + input_list_entry.offset, input_list_entry.length - j - 1 + input_list_entry.offset);
		}
	}

	// slice the result vector
	ListVector::GetEntry(result).Slice(rev_sel, ListVector::GetListSize(result));

	if (input_list.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> ListReverseBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 1);

	auto &input_list = arguments[0]->return_type;
	if (input_list.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}
	// if the input is a NULL, we should just return a NULL
	else if (input_list.id() == LogicalTypeId::SQLNULL) {
		bound_function.arguments[0] = input_list;
		bound_function.return_type = input_list;
	} else {
		D_ASSERT(input_list.id() == LogicalTypeId::LIST);

		LogicalType child_type = LogicalType::SQLNULL;
		for (const auto &argument : arguments) {
			child_type = LogicalType::MaxLogicalType(child_type, ListType::GetChildType(argument->return_type));
		}
		auto list_type = LogicalType::LIST(child_type);

		bound_function.arguments[0] = list_type;
		bound_function.return_type = list_type;
	}
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

static unique_ptr<BaseStatistics> ListReverseStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	D_ASSERT(child_stats.size() == 1);

	auto &left_stats = child_stats[0];

	auto stats = left_stats.ToUnique();

	return stats;
}

ScalarFunction ListReverseFun::GetFunction() {
	// the arguments and return types are actually set in the binder function
	auto fun = ScalarFunction({LogicalType::LIST(LogicalType::ANY)}, LogicalType::LIST(LogicalType::ANY),
	                          ListReverseFunction, ListReverseBind, nullptr, ListReverseStats);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

void ListReverseFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_reverse", "array_reverse"}, GetFunction());
}

} // namespace duckdb
