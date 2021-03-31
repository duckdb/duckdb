//#include "duckdb/planner/expression/bound_function_expression.hpp"
//#include "duckdb/common/string_util.hpp"
//#include "duckdb/parser/expression/bound_expression.hpp"
//#include "duckdb/function/scalar/nested_functions.hpp"
//#include "duckdb/common/types/data_chunk.hpp"
//
//namespace duckdb {
//
//template <class T, bool HEAP_REF = false>
//void MapExtractTemplate(idx_t count, Vector &list, Vector &offsets, Vector &result) {
//	int a = 0;
////	VectorData list_data, offsets_data, child_data;
////
////	list.Orrify(count, list_data);
////	offsets.Orrify(count, offsets_data);
////
////	result.SetVectorType(VectorType::FLAT_VECTOR);
////	auto result_data = FlatVector::GetData<T>(result);
////	auto &result_mask = FlatVector::Validity(result);
////
////	auto &vec = ListVector::GetEntry(list);
////	// heap-ref once
////	if (HEAP_REF) {
////		StringVector::AddHeapReference(result, vec);
////	}
////
////	// this is lifted from ExecuteGenericLoop because we can't push the list child data into this otherwise
////	// should have gone with GetValue perhaps
////	for (idx_t i = 0; i < count; i++) {
////		auto list_index = list_data.sel->get_index(i);
////		auto offsets_index = offsets_data.sel->get_index(i);
////		if (list_data.validity.RowIsValid(list_index) && offsets_data.validity.RowIsValid(offsets_index)) {
////			auto list_entry = ((list_entry_t *)list_data.data)[list_index];
////			auto offsets_entry = ((int64_t *)offsets_data.data)[offsets_index];
////			idx_t child_offset;
////			if (offsets_entry < 0) {
////				if ((idx_t)-offsets_entry > list_entry.length) {
////					result_mask.SetInvalid(i);
////					continue;
////				}
////				child_offset = list_entry.offset + list_entry.length + offsets_entry;
////			} else {
////				if ((idx_t)offsets_entry >= list_entry.length) {
////					result_mask.SetInvalid(i);
////					continue;
////				}
////				child_offset = list_entry.offset + offsets_entry;
////			}
////			vec.Orrify(ListVector::GetListSize(list), child_data);
////			if (child_data.validity.RowIsValid(child_offset)) {
////				result_data[i] = ((T *)child_data.data)[child_offset];
////			} else {
////				result_mask.SetInvalid(i);
////			}
////		} else {
////			result_mask.SetInvalid(i);
////		}
////	}
////	if (count == 1) {
////		result.SetVectorType(VectorType::CONSTANT_VECTOR);
////	}
//}
//
//static void MapExtractFunFun(DataChunk &args, ExpressionState &state, Vector &result) {
//	D_ASSERT(args.data.size() == 2);
//	D_ASSERT(args.data[0].GetType().id() == LogicalTypeId::MAP);
//
//	auto &map = args.data[0];
//	auto &key = args.data[1];
//	auto count = args.size();
//
//	switch (key.GetType().id()) {
//	case LogicalTypeId::UTINYINT:
//		MapExtractTemplate<uint8_t>(count, map, key, result);
//		break;
//	case LogicalTypeId::TINYINT:
//		MapExtractTemplate<int8_t>(count, map, key, result);
//		break;
//	case LogicalTypeId::USMALLINT:
//		MapExtractTemplate<uint16_t>(count, map, key, result);
//		break;
//	case LogicalTypeId::SMALLINT:
//		MapExtractTemplate<int16_t>(count, map, key, result);
//		break;
//	case LogicalTypeId::UINTEGER:
//		MapExtractTemplate<uint32_t>(count, map, key, result);
//		break;
//	case LogicalTypeId::INTEGER:
//		MapExtractTemplate<int32_t>(count, map, key, result);
//		break;
//	case LogicalTypeId::UBIGINT:
//		MapExtractTemplate<uint64_t>(count, map, key, result);
//		break;
//	case LogicalTypeId::BIGINT:
//		MapExtractTemplate<int64_t>(count, map, key, result);
//		break;
//	case LogicalTypeId::HUGEINT:
//		MapExtractTemplate<hugeint_t>(count, map, key, result);
//		break;
//	case LogicalTypeId::FLOAT:
//		MapExtractTemplate<float>(count, map, key, result);
//		break;
//	case LogicalTypeId::DOUBLE:
//		MapExtractTemplate<double>(count, map, key, result);
//		break;
//	case LogicalTypeId::DATE:
//		MapExtractTemplate<date_t>(count, map, key, result);
//		break;
//	case LogicalTypeId::TIME:
//		MapExtractTemplate<dtime_t>(count, map, key, result);
//		break;
//	case LogicalTypeId::TIMESTAMP:
//		MapExtractTemplate<timestamp_t>(count, map, key, result);
//		break;
//	case LogicalTypeId::BLOB:
//	case LogicalTypeId::VARCHAR:
//		MapExtractTemplate<string_t, true>(count, map, key, result);
//		break;
//	case LogicalTypeId::SQLNULL:
//		result.Reference(Value());
//		break;
//	default:
//		throw NotImplementedException("Unimplemented type for LIST_EXTRACT");
//	}
//
//	result.Verify(args.size());
//}
//
//static unique_ptr<FunctionData> MapExtractBind(ClientContext &context, ScalarFunction &bound_function,
//                                                vector<unique_ptr<Expression>> &arguments) {
//	if (arguments[0]->return_type.id() != LogicalTypeId::MAP) {
//		throw BinderException("MAP_EXTRACT can only operate on LISTs");
//	}
//	return make_unique<VariableReturnBindData>(bound_function.return_type);
//}
//
//void MapExtractFun::RegisterFunction(BuiltinFunctions &set) {
//	ScalarFunction fun("map_extract", {LogicalType::ANY, LogicalType::ANY}, LogicalType::UBIGINT, MapExtractFunFun,
//	                   false, MapExtractBind);
//	fun.varargs = LogicalType::ANY;
//	set.AddFunction(fun);
//}
//
//} // namespace duckdb
