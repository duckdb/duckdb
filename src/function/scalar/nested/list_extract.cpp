#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

template <class T, bool HEAP_REF = false>
void ListExtractTemplate(idx_t count, Vector &list, Vector &offsets, Vector &result) {
	VectorData list_data, offsets_data, child_data;

	list.Orrify(count, list_data);
	offsets.Orrify(count, offsets_data);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<T>(result);
	auto &result_mask = FlatVector::Validity(result);

	auto &vec = ListVector::GetEntry(list);
	// heap-ref once
	if (HEAP_REF) {
		StringVector::AddHeapReference(result, vec);
	}

	// this is lifted from ExecuteGenericLoop because we can't push the list child data into this otherwise
	// should have gone with GetValue perhaps
	for (idx_t i = 0; i < count; i++) {
		auto list_index = list_data.sel->get_index(i);
		auto offsets_index = offsets_data.sel->get_index(i);
		if (list_data.validity.RowIsValid(list_index) && offsets_data.validity.RowIsValid(offsets_index)) {
			auto list_entry = ((list_entry_t *)list_data.data)[list_index];
			auto offsets_entry = ((int64_t *)offsets_data.data)[offsets_index];
			idx_t child_offset;
			if (offsets_entry < 0) {
				if ((idx_t)-offsets_entry > list_entry.length) {
					result_mask.SetInvalid(i);
					continue;
				}
				child_offset = list_entry.offset + list_entry.length + offsets_entry;
			} else {
				if ((idx_t)offsets_entry >= list_entry.length) {
					result_mask.SetInvalid(i);
					continue;
				}
				child_offset = list_entry.offset + offsets_entry;
			}
			auto child_index = child_offset % STANDARD_VECTOR_SIZE;
			vec.Orrify(ListVector::GetListSize(list), child_data);
			auto child_index_sel = child_data.sel->get_index(child_index);
			if (child_data.validity.RowIsValid(child_index_sel)) {
				result_data[i] = ((T *)child_data.data)[child_index_sel];
			} else {
				result_mask.SetInvalid(i);
			}
		} else {
			result_mask.SetInvalid(i);
		}
	}
	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void ListExtractFunFun(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	D_ASSERT(args.data[0].GetType().id() == LogicalTypeId::LIST);

	auto &list = args.data[0];
	auto &offsets = args.data[1];
	auto count = args.size();

	switch (result.GetType().id()) {
	case LogicalTypeId::UTINYINT:
		ListExtractTemplate<uint8_t>(count, list, offsets, result);
		break;
	case LogicalTypeId::TINYINT:
		ListExtractTemplate<int8_t>(count, list, offsets, result);
		break;
	case LogicalTypeId::USMALLINT:
		ListExtractTemplate<uint16_t>(count, list, offsets, result);
		break;
	case LogicalTypeId::SMALLINT:
		ListExtractTemplate<int16_t>(count, list, offsets, result);
		break;
	case LogicalTypeId::UINTEGER:
		ListExtractTemplate<uint32_t>(count, list, offsets, result);
		break;
	case LogicalTypeId::INTEGER:
		ListExtractTemplate<int32_t>(count, list, offsets, result);
		break;
	case LogicalTypeId::UBIGINT:
		ListExtractTemplate<uint64_t>(count, list, offsets, result);
		break;
	case LogicalTypeId::BIGINT:
		ListExtractTemplate<int64_t>(count, list, offsets, result);
		break;
	case LogicalTypeId::HUGEINT:
		ListExtractTemplate<hugeint_t>(count, list, offsets, result);
		break;
	case LogicalTypeId::FLOAT:
		ListExtractTemplate<float>(count, list, offsets, result);
		break;
	case LogicalTypeId::DOUBLE:
		ListExtractTemplate<double>(count, list, offsets, result);
		break;
	case LogicalTypeId::DATE:
		ListExtractTemplate<date_t>(count, list, offsets, result);
		break;
	case LogicalTypeId::TIME:
		ListExtractTemplate<dtime_t>(count, list, offsets, result);
		break;
	case LogicalTypeId::TIMESTAMP:
		ListExtractTemplate<timestamp_t>(count, list, offsets, result);
		break;
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		ListExtractTemplate<string_t, true>(count, list, offsets, result);
		break;
	case LogicalTypeId::SQLNULL:
		result.Reference(Value());
		break;
	default:
		throw NotImplementedException("Unimplemented type for LIST_EXTRACT");
	}

	result.Verify(args.size());
}

static unique_ptr<FunctionData> ListExtractBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	if (arguments[0]->return_type.id() != LogicalTypeId::LIST) {
		throw BinderException("LIST_EXTRACT can only operate on LISTs");
	}
	// list extract returns the child type of the list as return type
	bound_function.return_type = arguments[0]->return_type.child_types()[0].second;
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

void ListExtractFun::RegisterFunction(BuiltinFunctions &set) {
	// return type is set in bind, unknown at this point
	ScalarFunction fun("list_extract", {LogicalType::ANY, LogicalType::BIGINT}, LogicalType::ANY, ListExtractFunFun,
	                   false, ListExtractBind);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
	fun.name = "list_element";
	set.AddFunction(fun);
	fun.name = "array_extract";
	set.AddFunction(fun);
}

} // namespace duckdb
