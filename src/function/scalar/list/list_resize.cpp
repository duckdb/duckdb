#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

void ListResizeFunction(DataChunk &args, ExpressionState &, Vector &result) {

	// Early-out, if the return value is a constant NULL.
	if (result.GetType().id() == LogicalTypeId::SQLNULL) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}

	auto &lists = args.data[0];
	auto &new_sizes = args.data[1];
	auto row_count = args.size();

	UnifiedVectorFormat lists_data;
	lists.ToUnifiedFormat(row_count, lists_data);
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(lists_data);

	auto &child_vector = ListVector::GetEntry(lists);
	UnifiedVectorFormat child_data;
	child_vector.ToUnifiedFormat(row_count, child_data);

	UnifiedVectorFormat new_sizes_data;
	new_sizes.ToUnifiedFormat(row_count, new_sizes_data);
	D_ASSERT(new_sizes.GetType().id() == LogicalTypeId::UBIGINT);
	auto new_sizes_entries = UnifiedVectorFormat::GetData<uint64_t>(new_sizes_data);

	// Get the new size of the result child vector.
	idx_t new_child_vector_size = 0;
	for (idx_t i = 0; i < row_count; i++) {
		auto idx = new_sizes_data.sel->get_index(i);
		if (new_sizes_data.validity.RowIsValid(idx)) {
			new_child_vector_size += new_sizes_entries[idx];
		}
	}
	ListVector::Reserve(result, new_child_vector_size);
	ListVector::SetListSize(result, new_child_vector_size);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);
	auto &result_child_vector = ListVector::GetEntry(result);

	// Get the default values vector, if it exists.
	UnifiedVectorFormat default_data;
	optional_ptr<Vector> default_vector;
	if (args.ColumnCount() == 3) {
		default_vector = &args.data[2];
		default_vector->ToUnifiedFormat(row_count, default_data);
	}

	idx_t offset = 0;
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {

		auto list_idx = lists_data.sel->get_index(row_idx);
		auto new_size_idx = new_sizes_data.sel->get_index(row_idx);

		// Set to NULL, if the list is NULL.
		if (!lists_data.validity.RowIsValid(list_idx)) {
			result_validity.SetInvalid(row_idx);
			continue;
		}

		idx_t new_size = 0;
		if (new_sizes_data.validity.RowIsValid(new_size_idx)) {
			new_size = new_sizes_entries[new_size_idx];
		}

		// If new_size >= length, then we copy [0, length) values.
		// If new_size < length, then we copy [0, new_size) values.
		auto copy_count = MinValue<idx_t>(list_entries[list_idx].length, new_size);

		// Set the result entry.
		result_entries[row_idx].offset = offset;
		result_entries[row_idx].length = new_size;

		// Copy the child vector's values.
		// The number of elements to copy is later determined like so: source_count - source_offset.
		idx_t source_offset = list_entries[list_idx].offset;
		idx_t source_count = source_offset + copy_count;
		VectorOperations::Copy(child_vector, result_child_vector, source_count, source_offset, offset);
		offset += copy_count;

		// Fill the remaining space with the default values.
		if (copy_count < new_size) {
			idx_t remaining_count = new_size - copy_count;

			if (default_vector) {
				auto default_idx = default_data.sel->get_index(row_idx);
				if (default_data.validity.RowIsValid(default_idx)) {
					SelectionVector sel(remaining_count);
					for (idx_t j = 0; j < remaining_count; j++) {
						sel.set_index(j, default_idx);
					}
					VectorOperations::Copy(*default_vector, result_child_vector, sel, remaining_count, 0, offset);
					offset += remaining_count;
					continue;
				}
			}

			// Fill the remaining space with NULL.
			for (idx_t j = copy_count; j < new_size; j++) {
				FlatVector::SetNull(result_child_vector, offset, true);
				offset++;
			}
		}
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> ListResizeBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2 || arguments.size() == 3);
	bound_function.arguments[1] = LogicalType::UBIGINT;

	// If the first argument is an array, cast it to a list.
	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));

	// Early-out, if the first argument is a constant NULL.
	if (arguments[0]->return_type == LogicalType::SQLNULL) {
		bound_function.arguments[0] = LogicalType::SQLNULL;
		bound_function.return_type = LogicalType::SQLNULL;
		return make_uniq<VariableReturnBindData>(bound_function.return_type);
	}

	// Early-out, if the first argument is a prepared statement.
	if (arguments[0]->return_type == LogicalType::UNKNOWN) {
		bound_function.return_type = arguments[0]->return_type;
		return make_uniq<VariableReturnBindData>(bound_function.return_type);
	}

	// Attempt implicit casting, if the default type does not match list the list child type.
	if (bound_function.arguments.size() == 3 &&
	    ListType::GetChildType(arguments[0]->return_type) != arguments[2]->return_type &&
	    arguments[2]->return_type != LogicalTypeId::SQLNULL) {
		bound_function.arguments[2] = ListType::GetChildType(arguments[0]->return_type);
	}

	bound_function.return_type = arguments[0]->return_type;
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

void ListResizeFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction simple_fun({LogicalType::LIST(LogicalTypeId::ANY), LogicalTypeId::ANY},
	                          LogicalType::LIST(LogicalTypeId::ANY), ListResizeFunction, ListResizeBind);
	simple_fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;

	ScalarFunction default_value_fun({LogicalType::LIST(LogicalTypeId::ANY), LogicalTypeId::ANY, LogicalTypeId::ANY},
	                                 LogicalType::LIST(LogicalTypeId::ANY), ListResizeFunction, ListResizeBind);
	default_value_fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;

	ScalarFunctionSet list_resize("list_resize");
	list_resize.AddFunction(simple_fun);
	list_resize.AddFunction(default_value_fun);
	set.AddFunction(list_resize);

	ScalarFunctionSet array_resize("array_resize");
	array_resize.AddFunction(simple_fun);
	array_resize.AddFunction(default_value_fun);
	set.AddFunction(array_resize);
}

} // namespace duckdb
