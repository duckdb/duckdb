#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/storage/statistics/list_statistics.hpp"
#include "duckdb/storage/statistics/validity_statistics.hpp"

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
			vec.Orrify(ListVector::GetListSize(list), child_data);
			if (child_data.validity.RowIsValid(child_offset)) {
				result_data[i] = ((T *)child_data.data)[child_offset];
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

static void ExecuteListExtract(Vector &result, Vector &list, Vector &offsets, const idx_t count) {
	D_ASSERT(list.GetType().id() == LogicalTypeId::LIST);

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
	case LogicalTypeId::LIST: {
		// nested list: we have to reference the child
		auto &child_list = ListVector::GetEntry(list);
		auto &child_child_list = ListVector::GetEntry(child_list);

		ListVector::GetEntry(result).Reference(child_child_list);
		ListVector::SetListSize(result, ListVector::GetListSize(child_list));
		ListExtractTemplate<list_entry_t>(count, list, offsets, result);
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type for LIST_EXTRACT");
	}

	result.Verify(count);
}

static void ExecuteStringExtract(Vector &result, Vector &input_vector, Vector &subscript_vector, const idx_t count) {
	BinaryExecutor::Execute<string_t, int32_t, string_t>(
	    input_vector, subscript_vector, result, count, [&](string_t input_string, int32_t subscript) {
		    return SubstringFun::SubstringScalarFunction(result, input_string, subscript + int32_t(subscript >= 0), 1);
	    });
}

static void ListExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	auto count = args.size();

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}

	Vector &base = args.data[0];
	Vector &subscript = args.data[1];

	switch (base.GetType().id()) {
	case LogicalTypeId::LIST:
		ExecuteListExtract(result, base, subscript, count);
		break;
	case LogicalTypeId::VARCHAR:
		ExecuteStringExtract(result, base, subscript, count);
		break;
	default:
		throw NotImplementedException("Specifier type not implemented");
	}
}

static unique_ptr<FunctionData> ListExtractBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	D_ASSERT(LogicalTypeId::LIST == arguments[0]->return_type.id());
	// list extract returns the child type of the list as return type
	bound_function.return_type = ListType::GetChildType(arguments[0]->return_type);

	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

static unique_ptr<BaseStatistics> ListExtractStats(ClientContext &context, BoundFunctionExpression &expr,
                                                   FunctionData *bind_data,
                                                   vector<unique_ptr<BaseStatistics>> &child_stats) {
	if (!child_stats[0]) {
		return nullptr;
	}
	auto &list_stats = (ListStatistics &)*child_stats[0];
	if (!list_stats.child_stats) {
		return nullptr;
	}
	auto child_copy = list_stats.child_stats->Copy();
	// list_extract always pushes a NULL, since if the offset is out of range for a list it inserts a null
	child_copy->validity_stats = make_unique<ValidityStatistics>(true);
	return child_copy;
}

void ListExtractFun::RegisterFunction(BuiltinFunctions &set) {
	// the arguments and return types are actually set in the binder function
	ScalarFunction lfun({LogicalType::LIST(LogicalType::ANY), LogicalType::BIGINT}, LogicalType::ANY,
	                    ListExtractFunction, false, ListExtractBind, nullptr, ListExtractStats);

	ScalarFunction sfun({LogicalType::VARCHAR, LogicalType::INTEGER}, LogicalType::VARCHAR, ListExtractFunction, false,
	                    nullptr);

	ScalarFunctionSet list_extract("list_extract");
	list_extract.AddFunction(lfun);
	list_extract.AddFunction(sfun);
	set.AddFunction(list_extract);

	ScalarFunctionSet list_element("list_element");
	list_element.AddFunction(lfun);
	list_element.AddFunction(sfun);
	set.AddFunction(list_element);

	ScalarFunctionSet array_extract("array_extract");
	array_extract.AddFunction(lfun);
	array_extract.AddFunction(sfun);
	array_extract.AddFunction(StructExtractFun::GetFunction());
	set.AddFunction(array_extract);
}

} // namespace duckdb
