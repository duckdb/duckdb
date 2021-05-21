#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {
void MapExtract(Vector &map, Value &values, Vector &result) {
	auto &result_mask = FlatVector::Validity(result);
	switch (values.type().id()) {
	case LogicalTypeId::UTINYINT: {
		auto result_data = FlatVector::GetData<uint8_t>(result);
		for (idx_t i = 0; i < values.list_value.size(); i++) {
			if (values.list_value[i].is_null) {
				result_mask.Set(i, false);
			} else {
				result_data[i] = values.list_value[i].value_.utinyint;
			}
		}
		break;
	}
	case LogicalTypeId::USMALLINT: {
		auto result_data = FlatVector::GetData<uint16_t>(result);
		for (idx_t i = 0; i < values.list_value.size(); i++) {
			if (values.list_value[i].is_null) {
				result_mask.Set(i, false);
			} else {
				result_data[i] = values.list_value[i].value_.usmallint;
			}
		}
		break;
	}
	case LogicalTypeId::UINTEGER: {
		auto result_data = FlatVector::GetData<uint32_t>(result);
		for (idx_t i = 0; i < values.list_value.size(); i++) {
			if (values.list_value[i].is_null) {
				result_mask.Set(i, false);
			} else {
				result_data[i] = values.list_value[i].value_.uinteger;
			}
		}
		break;
	}
	case LogicalTypeId::UBIGINT: {
		auto result_data = FlatVector::GetData<uint64_t>(result);
		for (idx_t i = 0; i < values.list_value.size(); i++) {
			if (values.list_value[i].is_null) {
				result_mask.Set(i, false);
			} else {
				result_data[i] = values.list_value[i].value_.ubigint;
			}
		}
		break;
	}
	case LogicalTypeId::TINYINT: {
		auto result_data = FlatVector::GetData<int8_t>(result);
		for (idx_t i = 0; i < values.list_value.size(); i++) {
			if (values.list_value[i].is_null) {
				result_mask.Set(i, false);
			} else {
				result_data[i] = values.list_value[i].value_.tinyint;
			}
		}
		break;
	}
	case LogicalTypeId::SMALLINT: {
		auto result_data = FlatVector::GetData<int16_t>(result);
		for (idx_t i = 0; i < values.list_value.size(); i++) {
			if (values.list_value[i].is_null) {
				result_mask.Set(i, false);
			} else {
				result_data[i] = values.list_value[i].value_.smallint;
			}
		}
		break;
	}
	case LogicalTypeId::INTEGER: {
		auto result_data = FlatVector::GetData<int32_t>(result);
		for (idx_t i = 0; i < values.list_value.size(); i++) {
			if (values.list_value[i].is_null) {
				result_mask.Set(i, false);
			} else {
				result_data[i] = values.list_value[i].value_.integer;
			}
		}
		break;
	}
	case LogicalTypeId::BIGINT: {
		auto result_data = FlatVector::GetData<int64_t>(result);
		for (idx_t i = 0; i < values.list_value.size(); i++) {
			if (values.list_value[i].is_null) {
				result_mask.Set(i, false);
			} else {
				result_data[i] = values.list_value[i].value_.bigint;
			}
		}
		break;
	}
	case LogicalTypeId::HUGEINT: {
		auto result_data = FlatVector::GetData<hugeint_t>(result);
		for (idx_t i = 0; i < values.list_value.size(); i++) {
			if (values.list_value[i].is_null) {
				result_mask.Set(i, false);
			} else {
				result_data[i] = values.list_value[i].value_.hugeint;
			}
		}
		break;
	}
	case LogicalTypeId::FLOAT: {
		auto result_data = FlatVector::GetData<float>(result);
		for (idx_t i = 0; i < values.list_value.size(); i++) {
			if (values.list_value[i].is_null) {
				result_mask.Set(i, false);
			} else {
				result_data[i] = values.list_value[i].value_.float_;
			}
		}
		break;
	}
	case LogicalTypeId::DOUBLE: {
		auto result_data = FlatVector::GetData<double>(result);
		for (idx_t i = 0; i < values.list_value.size(); i++) {
			if (values.list_value[i].is_null) {
				result_mask.Set(i, false);
			} else {
				result_data[i] = values.list_value[i].value_.double_;
			}
		}
		break;
	}
	case LogicalTypeId::DATE: {
		auto result_data = FlatVector::GetData<date_t>(result);
		for (idx_t i = 0; i < values.list_value.size(); i++) {
			if (values.list_value[i].is_null) {
				result_mask.Set(i, false);
			} else {
				result_data[i] = values.list_value[i].value_.date;
			}
		}
		break;
	}
	case LogicalTypeId::TIME: {
		auto result_data = FlatVector::GetData<dtime_t>(result);
		for (idx_t i = 0; i < values.list_value.size(); i++) {
			if (values.list_value[i].is_null) {
				result_mask.Set(i, false);
			} else {
				result_data[i] = values.list_value[i].value_.time;
			}
		}
		break;
	}
	case LogicalTypeId::TIMESTAMP: {
		auto result_data = FlatVector::GetData<timestamp_t>(result);
		for (idx_t i = 0; i < values.list_value.size(); i++) {
			if (values.list_value[i].is_null) {
				result_mask.Set(i, false);
			} else {
				result_data[i] = values.list_value[i].value_.timestamp;
			}
		}
		break;
	}
		//            case LogicalTypeId::BLOB:
		//            case LogicalTypeId::VARCHAR:
		//                MapExtractTemplate<string_t, true>(count, map, key, result);
		//                break;
		//            case LogicalTypeId::SQLNULL:
		//                result.Reference(Value());
		//                break;
	default:
		throw NotImplementedException("Unimplemented type for MAP_EXTRACT");
	}

	auto result_data = FlatVector::GetData<uint8_t>(result);
	for (idx_t i = 0; i < values.list_value.size(); i++) {
		result_data[i] = values.list_value[i].value_.utinyint;
	}
	//
	//	auto &vec = ListVector::GetEntry(list);
	//	// heap-ref once
	//	if (HEAP_REF) {
	//		StringVector::AddHeapReference(result, vec);
	//	}
	//
	//	// this is lifted from ExecuteGenericLoop because we can't push the list child data into this otherwise
	//	// should have gone with GetValue perhaps
	//	for (idx_t i = 0; i < count; i++) {
	//		auto list_index = list_data.sel->get_index(i);
	//		auto offsets_index = offsets_data.sel->get_index(i);
	//		if (list_data.validity.RowIsValid(list_index) && offsets_data.validity.RowIsValid(offsets_index)) {
	//			auto list_entry = ((list_entry_t *)list_data.data)[list_index];
	//			auto offsets_entry = ((int64_t *)offsets_data.data)[offsets_index];
	//			idx_t child_offset;
	//			if (offsets_entry < 0) {
	//				if ((idx_t)-offsets_entry > list_entry.length) {
	//					result_mask.SetInvalid(i);
	//					continue;
	//				}
	//				child_offset = list_entry.offset + list_entry.length + offsets_entry;
	//			} else {
	//				if ((idx_t)offsets_entry >= list_entry.length) {
	//					result_mask.SetInvalid(i);
	//					continue;
	//				}
	//				child_offset = list_entry.offset + offsets_entry;
	//			}
	//			vec.Orrify(ListVector::GetListSize(list), child_data);
	//			if (child_data.validity.RowIsValid(child_offset)) {
	//				result_data[i] = ((T *)child_data.data)[child_offset];
	//			} else {
	//				result_mask.SetInvalid(i);
	//			}
	//		} else {
	//			result_mask.SetInvalid(i);
	//		}
	//	}
	//	if (count == 1) {
	//		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	//	}
}

static void MapExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	D_ASSERT(args.data[0].GetType().id() == LogicalTypeId::MAP);

	auto &map = args.data[0];
	auto &key = args.data[1];

	auto &children = StructVector::GetEntries(map);
	auto key_value = key.GetValue(0);
	auto offsets = ListVector::Search(*children[0].second, key_value);

	for (auto &offset : offsets) {
		children[1].second->GetValue(offset);
	}
	auto values = ListVector::GetValuesFromOffsets(*children[1].second, offsets);
	MapExtract(map, values, result);
	result.Verify(args.size());
}

static unique_ptr<FunctionData> MapExtractBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 2) {
		throw BinderException("MAP_EXTRACT must have exactly two arguments");
	}
	if (arguments[0]->return_type.id() != LogicalTypeId::MAP) {
		throw BinderException("MAP_EXTRACT can only operate on MAPs");
	}
	auto key_type = arguments[0]->return_type.child_types()[0].second.child_types()[0].second;
	auto value_type = arguments[0]->return_type.child_types()[1].second.child_types()[0].second;
	//! TODO: prolly want to try to cast this first

	if (key_type != arguments[1]->return_type) {
		throw BinderException("MAP_EXTRACT second argument has a different type from the MAP type");
	}
	bound_function.return_type = value_type;
	return make_unique<VariableReturnBindData>(value_type);
}

void MapExtractFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction fun("map_extract", {LogicalType::ANY, LogicalType::ANY}, LogicalType::ANY, MapExtractFunction, false,
	                   MapExtractBind);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb
