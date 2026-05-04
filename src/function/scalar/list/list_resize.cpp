#include "duckdb/common/ubigint.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/list_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

static void ListResizeFunction(DataChunk &args, ExpressionState &, Vector &result) {
	// Early-out, if the return value is a constant NULL.
	if (result.GetType().id() == LogicalTypeId::SQLNULL) {
		ConstantVector::SetNull(result, count_t(args.size()));
		return;
	}

	auto &lists = args.data[0];
	auto &new_sizes = args.data[1];
	auto row_count = args.size();
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(new_sizes.GetType().id() == LogicalTypeId::UBIGINT);

	auto list_entries = lists.Values<list_entry_t>(row_count);
	auto new_size_entries = new_sizes.Values<ubigint_t>(row_count);
	auto &child_vector = ListVector::GetChild(lists);

	// Sum up the total child capacity
	ubigint_t total_child_size(0);
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		auto list_entry = list_entries[row_idx];
		auto new_size_entry = new_size_entries[row_idx];
		if (list_entry.IsValid() && new_size_entry.IsValid()) {
			total_child_size += new_size_entry.GetValue();
		}
	}

	ListVector::Reserve(result, total_child_size.value);
	auto result_entries = FlatVector::Writer<list_entry_t>(result, row_count);

	optional<VectorValidityIterator> default_validity;
	if (args.ColumnCount() == 3) {
		auto &default_vector = args.data[2];
		default_validity = default_vector.Validity(row_count);
	}

	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		auto list_entry = list_entries[row_idx];
		// Set to NULL, if the list is NULL.
		if (!list_entry.IsValid()) {
			result_entries.WriteNull();
			continue;
		}

		auto new_size_entry = new_size_entries[row_idx];
		ubigint_t new_size = new_size_entry.IsValid() ? new_size_entry.GetValue() : ubigint_t(0);

		// If new_size >= length, then we copy [0, length) values.
		// If new_size < length, then we copy [0, new_size) values.
		const auto &source_list = list_entry.GetValue();
		auto copy_count = MinValue<ubigint_t>(source_list.length, new_size);

		auto list = result_entries.WriteDynamicList();
		ubigint_t source_offset = source_list.offset;
		ubigint_t source_count = source_offset + copy_count;
		list.Append(child_vector, *FlatVector::IncrementalSelectionVector(), source_count.value, source_offset.value,
		            copy_count.value);

		if (copy_count >= new_size) {
			continue;
		}
		ubigint_t remaining_count = new_size - copy_count;

		if (default_validity) {
			// if a default value is provided fill the list with the default value
			if (default_validity.value().IsValid(row_idx)) {
				SelectionVector sel(remaining_count.value);
				for (idx_t j = 0; j < remaining_count.value; j++) {
					sel.set_index(j, row_idx);
				}
				auto &default_vector = args.data[2];
				list.Append(default_vector, sel, remaining_count.value, 0, remaining_count.value);
				continue;
			}
		}

		// Fill the remaining space with NULL.
		list.AppendNulls(remaining_count.value);
	}
}

static unique_ptr<FunctionData> ListResizeBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	D_ASSERT(bound_function.GetArguments().size() == 2 || arguments.size() == 3);
	bound_function.GetArguments()[1] = LogicalType::UBIGINT;

	// If the first argument is an array, cast it to a list.
	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));

	// Early-out, if the first argument is a constant NULL.
	if (arguments[0]->GetReturnType() == LogicalType::SQLNULL) {
		bound_function.GetArguments()[0] = LogicalType::SQLNULL;
		bound_function.SetReturnType(LogicalType::SQLNULL);
		return make_uniq<VariableReturnBindData>(bound_function.GetReturnType());
	}

	// Early-out, if the first argument is a prepared statement.
	if (arguments[0]->GetReturnType() == LogicalType::UNKNOWN) {
		bound_function.SetReturnType(arguments[0]->GetReturnType());
		return make_uniq<VariableReturnBindData>(bound_function.GetReturnType());
	}

	// Attempt implicit casting, if the default type does not match list the list child type.
	if (bound_function.GetArguments().size() == 3 &&
	    ListType::GetChildType(arguments[0]->GetReturnType()) != arguments[2]->GetReturnType() &&
	    arguments[2]->GetReturnType() != LogicalTypeId::SQLNULL) {
		bound_function.GetArguments()[2] = ListType::GetChildType(arguments[0]->GetReturnType());
	}

	bound_function.SetReturnType(arguments[0]->GetReturnType());
	return make_uniq<VariableReturnBindData>(bound_function.GetReturnType());
}

ScalarFunctionSet ListResizeFun::GetFunctions() {
	ScalarFunction simple_fun({LogicalType::LIST(LogicalTypeId::ANY), LogicalTypeId::ANY},
	                          LogicalType::LIST(LogicalTypeId::ANY), ListResizeFunction, ListResizeBind);
	simple_fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	simple_fun.SetFallible();
	ScalarFunction default_value_fun({LogicalType::LIST(LogicalTypeId::ANY), LogicalTypeId::ANY, LogicalTypeId::ANY},
	                                 LogicalType::LIST(LogicalTypeId::ANY), ListResizeFunction, ListResizeBind);
	default_value_fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	default_value_fun.SetFallible();
	ScalarFunctionSet list_resize_set("list_resize");
	list_resize_set.AddFunction(simple_fun);
	list_resize_set.AddFunction(default_value_fun);
	return list_resize_set;
}

} // namespace duckdb
