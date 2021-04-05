#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

template <typename T>
static bool ClampOffset(T &offset, const list_entry_t &list_entry) {
	if (offset < 0) {
		if ((idx_t)-offset > list_entry.length) {
			return false;
		}
		offset = list_entry.offset + list_entry.length + offset;
	}
	return true;
}

template <typename T>
static bool SliceList(list_entry_t &list_entry, T begin, T end, bool list_valid, bool begin_valid, bool end_valid) {
	// Clamp offsets
	begin = begin_valid ? begin : 0;
	end = end_valid ? end : list_entry.length;
	if (!ClampOffset(begin, list_entry) || !ClampOffset(end, list_entry) || !list_valid) {
		return false;
	}
	end = MaxValue<T>(begin, end);

	// Trim down the range
	list_entry.offset += begin;
	list_entry.length = end - begin;

	return true;
}

static void ArraySliceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(result.GetType().child_types().size() == 1);
	D_ASSERT(args.ColumnCount() == 3);
	D_ASSERT(args.data.size() == 3);
	auto count = args.size();

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}

	Vector &list = args.data[0];
	Vector &begins = args.data[1];
	Vector &ends = args.data[2];

	// Share the value dictionary as we are just going to slice it
	ListVector::ReferenceEntry(result, list);

	if (result.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		auto result_data = ConstantVector::GetData<list_entry_t>(result);
		auto list_data = ConstantVector::GetData<list_entry_t>(list);
		auto begin_data = ConstantVector::GetData<int64_t>(begins);
		auto end_data = ConstantVector::GetData<int64_t>(ends);

		auto list_entry = list_data[0];
		auto begin = begin_data[0];
		auto end = end_data[0];

		auto list_valid = !ConstantVector::IsNull(list);
		auto begin_valid = !ConstantVector::IsNull(begins);
		auto end_valid = !ConstantVector::IsNull(ends);

		// Clamp offsets
		if (!SliceList(list_entry, begin, end, list_valid, begin_valid, end_valid)) {
			ConstantVector::SetNull(result, true);
		} else {
			result_data[0] = list_entry;
		}
	} else {
		VectorData list_data, begins_data, ends_data;

		list.Orrify(count, list_data);
		begins.Orrify(count, begins_data);
		ends.Orrify(count, ends_data);

		auto result_data = FlatVector::GetData<list_entry_t>(result);
		auto result_mask = FlatVector::Validity(result);

		for (idx_t i = 0; i < count; ++i) {
			auto list_index = list_data.sel->get_index(i);
			auto begins_index = begins_data.sel->get_index(i);
			auto ends_index = ends_data.sel->get_index(i);

			auto list_entry = ((list_entry_t *)list_data.data)[list_index];
			auto begin = ((int64_t *)begins_data.data)[begins_index];
			auto end = ((int64_t *)ends_data.data)[ends_index];

			auto list_valid = list_data.validity.RowIsValid(list_index);
			auto begin_valid = begins_data.validity.RowIsValid(begins_index);
			auto end_valid = ends_data.validity.RowIsValid(ends_index);

			// Clamp offsets
			if (!SliceList(list_entry, begin, end, list_valid, begin_valid, end_valid)) {
				result_mask.SetInvalid(i);
				continue;
			}

			result_data[i] = list_entry;
		}
	}

	result.Verify(count);
}

static unique_ptr<FunctionData> ArraySliceBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	if (arguments[0]->return_type.id() != LogicalTypeId::LIST) {
		throw BinderException("ARRAY_SLICE can only operate on LISTs");
	}

	// The result is the same type
	bound_function.return_type = arguments[0]->return_type;
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

void ArraySliceFun::RegisterFunction(BuiltinFunctions &set) {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun("array_slice", {LogicalType::ANY, LogicalType::BIGINT, LogicalType::BIGINT}, LogicalType::LIST,
	                   ArraySliceFunction, false, ArraySliceBind);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb
