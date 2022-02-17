#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

//! Forward declaration for recursion
static LogicalType StructureToType(yyjson_val *val);

static LogicalType StructureToTypeArray(yyjson_val *arr) {
	if (yyjson_arr_size(arr) != 1) {
		throw InvalidInputException("Too many values in array of JSON structure");
	}
	return LogicalType::LIST(StructureToType(yyjson_arr_get_first(arr)));
}

static LogicalType StructureToTypeObject(yyjson_val *obj) {
	unordered_set<string> names;
	child_list_t<LogicalType> child_types;
	yyjson_val *key, *val;
	yyjson_obj_iter iter;
	yyjson_obj_iter_init(obj, &iter);
	while ((key = yyjson_obj_iter_next(&iter))) {
		val = yyjson_obj_iter_get_val(key);
		auto key_str = yyjson_get_str(key);
		if (names.find(key_str) != names.end()) {
			auto obj_string = JSONCommon::WriteVal(obj);
			throw InvalidInputException("Duplicate keys in object in JSON structure: %s", obj_string.GetString());
		}
		names.insert(key_str);
		child_types.emplace_back(key_str, StructureToType(val));
	}
	return LogicalType::STRUCT(child_types);
}

static LogicalType StructureToTypeVal(yyjson_val *val) {
	auto type_string = StringUtil::Lower(yyjson_get_str(val));
	LogicalType result;
	if ((StringUtil::StartsWith(type_string, "decimal(") || StringUtil::StartsWith(type_string, "dec(") ||
	     StringUtil::StartsWith(type_string, "numeric(")) &&
	    StringUtil::EndsWith(type_string, ")")) {
		// Parse the DECIMAL width and scale
		auto start = type_string.find('(') + 1;
		auto end = type_string.size() - 1;
		auto ws_string = string(type_string.c_str() + start, end - start);
		auto split = StringUtil::Split(ws_string, ',');
		if (split.size() == 1 || split.size() == 2) {
			vector<idx_t> ws;
			for (auto &s : split) {
				char *p_end = nullptr;
				idx_t digit = strtoull(s.c_str(), &p_end, 10);
				if (!p_end) {
					throw InvalidInputException("unable to parse decimal type \"%s\"", type_string);
				}
				ws.push_back(digit);
			}
			if (ws[0] < 1 || ws[0] > 38) {
				throw InvalidInputException("decimal width must be between 1 and 38");
			}
			if (ws.size() == 2 && ws[1] > ws[0]) {
				throw InvalidInputException("decimal scale cannot be greater than width");
			}
			auto scale = ws.size() == 2 ? ws[1] : 0;
			result = LogicalType::DECIMAL(ws[0], scale);
		} else {
			throw InvalidInputException("unable to parse decimal type \"%s\"", type_string);
		}
	} else {
		result = TransformStringToLogicalType(type_string);
		switch (result.id()) {
		case LogicalTypeId::DECIMAL:
			// This is our default
			result = LogicalType::DECIMAL(18, 3);
			break;
		case LogicalTypeId::ENUM:
		case LogicalTypeId::USER:
		case LogicalTypeId::STRUCT:
		case LogicalTypeId::MAP:
		case LogicalTypeId::LIST:
			throw InvalidInputException("unsupported type in JSON structure: \"%s\"", type_string);
		default:
			break;
		}
	}
	D_ASSERT(result != LogicalType::INVALID);
	return result;
}

static LogicalType StructureToType(yyjson_val *val) {
	switch (yyjson_get_type(val)) {
	case YYJSON_TYPE_ARR:
		return StructureToTypeArray(val);
	case YYJSON_TYPE_OBJ:
		return StructureToTypeObject(val);
	case YYJSON_TYPE_STR:
		return StructureToTypeVal(val);
	default:
		throw InvalidInputException("invalid JSON structure");
	}
}

static unique_ptr<FunctionData> JSONTransformBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	if (arguments[1]->return_type == LogicalTypeId::SQLNULL) {
		bound_function.return_type = LogicalTypeId::SQLNULL;
	} else if (!arguments[1]->IsFoldable()) {
		throw InvalidInputException("JSON structure must be a constant!");
	} else {
		auto structure_val = ExpressionExecutor::EvaluateScalar(*arguments[1]);
		if (!structure_val.TryCastAs(LogicalType::JSON)) {
			throw InvalidInputException("cannot cast JSON structure to string");
		}
		auto structure_string = structure_val.GetValueUnsafe<string_t>();
		auto doc = JSONCommon::ReadDocumentUnsafe(structure_string);
		if (!doc) {
			throw InvalidInputException("malformed JSON structure");
		}
		bound_function.return_type = StructureToType(doc->root);
	}
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

//! Forward declaration for recursion
static void Transform(yyjson_val *vals[], Vector &result, const idx_t count, bool strict);

template <class T>
static void TransformNumerical(yyjson_val *vals[], Vector &result, const idx_t count, const bool strict) {
	auto data = (T *)FlatVector::GetData(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || yyjson_is_null(val) || !JSONCommon::GetValueNumerical<T>(val, data[i], strict)) {
			validity.SetInvalid(i);
		}
	}
}

template <class T>
static void TransformDecimal(yyjson_val *vals[], Vector &result, const idx_t count, uint8_t width, uint8_t scale,
                             const bool strict) {
	auto data = (T *)FlatVector::GetData(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || yyjson_is_null(val) || !JSONCommon::GetValueDecimal<T>(val, data[i], width, scale, strict)) {
			validity.SetInvalid(i);
		}
	}
}

static void TransformUUID(yyjson_val *vals[], Vector &result, const idx_t count, const bool strict) {
	auto data = (hugeint_t *)FlatVector::GetData(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || yyjson_is_null(val) || !JSONCommon::GetValueUUID(val, data[i], result, strict)) {
			validity.SetInvalid(i);
		}
	}
}

static void TransformString(yyjson_val *vals[], Vector &result, const idx_t count) {
	auto data = (string_t *)FlatVector::GetData(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || yyjson_is_null(val) || !JSONCommon::GetValueString(val, data[i], result)) {
			validity.SetInvalid(i);
		}
	}
}

template <class T, class OP>
static void TransformDateTime(yyjson_val *vals[], Vector &result, const idx_t count, const bool strict) {
	auto data = (T *)FlatVector::GetData(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || yyjson_is_null(val) || !JSONCommon::GetValueDateTime<T, OP>(val, data[i], strict)) {
			validity.SetInvalid(i);
		}
	}
}

template <class OP>
static void TransformTimestamp(yyjson_val *vals[], Vector &result, const idx_t count, const bool strict) {
	auto data = (timestamp_t *)FlatVector::GetData(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || yyjson_is_null(val) || !JSONCommon::GetValueTimestamp<OP>(val, data[i], strict)) {
			validity.SetInvalid(i);
		}
	}
}

static void TransformObject(yyjson_val *vals[], Vector &result, const idx_t count, const LogicalType &type,
                            bool strict) {
	// Initialize array for the nested values
	auto nested_vals_ptr = unique_ptr<yyjson_val *[]>(new yyjson_val *[count]);
	auto nested_vals = nested_vals_ptr.get();
	// Loop through child types
	auto &child_vs = StructVector::GetEntries(result);
	for (idx_t child_i = 0; child_i < child_vs.size(); child_i++) {
		const auto &name = StructType::GetChildName(type, child_i);
		auto name_ptr = name.c_str();
		auto name_len = name.size();
		for (idx_t i = 0; i < count; i++) {
			nested_vals[i] = yyjson_obj_getn(vals[i], name_ptr, name_len);
		}
		// Transform child values
		Transform(nested_vals, *child_vs[child_i], count, strict);
	}
}

static void TransformArray(yyjson_val *vals[], Vector &result, const idx_t count, bool strict) {
	// Initialize list vector
	auto list_entries = FlatVector::GetData<list_entry_t>(result);
	auto &list_validity = FlatVector::Validity(result);
	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		if (!vals[i] || yyjson_is_null(vals[i])) {
			list_validity.SetInvalid(i);
		}
		auto &entry = list_entries[i];
		entry.offset = offset;
		entry.length = yyjson_arr_size(vals[i]);
		offset += entry.length;
	}
	ListVector::SetListSize(result, offset);
	ListVector::Reserve(result, offset);
	// Initialize array for the nested values
	auto nested_vals_ptr = unique_ptr<yyjson_val *[]>(new yyjson_val *[offset]);
	auto nested_vals = nested_vals_ptr.get();
	// Get array values
	yyjson_val *val;
	yyjson_arr_iter iter;
	idx_t list_i = 0;
	for (idx_t i = 0; i < count; i++) {
		if (!list_validity.RowIsValid(i)) {
			// We already marked this as invalid
			continue;
		}
		yyjson_arr_iter_init(vals[i], &iter);
		while ((val = yyjson_arr_iter_next(&iter))) {
			nested_vals[list_i] = val;
			list_i++;
		}
	}
	D_ASSERT(list_i == offset);
	// Transform array values
	Transform(nested_vals, ListVector::GetEntry(result), offset, strict);
}

static void Transform(yyjson_val *vals[], Vector &result, const idx_t count, bool strict) {
	auto result_type = result.GetType();
	switch (result_type.id()) {
	case LogicalTypeId::BOOLEAN:
		return TransformNumerical<bool>(vals, result, count, strict);
	case LogicalTypeId::TINYINT:
		return TransformNumerical<int8_t>(vals, result, count, strict);
	case LogicalTypeId::SMALLINT:
		return TransformNumerical<int16_t>(vals, result, count, strict);
	case LogicalTypeId::INTEGER:
		return TransformNumerical<int32_t>(vals, result, count, strict);
	case LogicalTypeId::BIGINT:
		return TransformNumerical<int64_t>(vals, result, count, strict);
	case LogicalTypeId::UTINYINT:
		return TransformNumerical<uint8_t>(vals, result, count, strict);
	case LogicalTypeId::USMALLINT:
		return TransformNumerical<uint16_t>(vals, result, count, strict);
	case LogicalTypeId::UINTEGER:
		return TransformNumerical<uint32_t>(vals, result, count, strict);
	case LogicalTypeId::UBIGINT:
		return TransformNumerical<uint64_t>(vals, result, count, strict);
	case LogicalTypeId::HUGEINT:
		return TransformNumerical<hugeint_t>(vals, result, count, strict);
	case LogicalTypeId::UUID:
		return TransformUUID(vals, result, count, strict);
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(result_type);
		auto scale = DecimalType::GetScale(result_type);
		switch (result_type.InternalType()) {
		case PhysicalType::INT16:
			return TransformDecimal<int16_t>(vals, result, count, width, scale, strict);
		case PhysicalType::INT32:
			return TransformDecimal<int32_t>(vals, result, count, width, scale, strict);
		case PhysicalType::INT64:
			return TransformDecimal<int64_t>(vals, result, count, width, scale, strict);
		case PhysicalType::INT128:
			return TransformDecimal<hugeint_t>(vals, result, count, width, scale, strict);
		default:
			throw InternalException("Unimplemented internal type for decimal");
		}
	}
	case LogicalTypeId::FLOAT:
		return TransformNumerical<float>(vals, result, count, strict);
	case LogicalTypeId::DOUBLE:
		return TransformNumerical<double>(vals, result, count, strict);
	case LogicalTypeId::DATE:
		return TransformDateTime<date_t, TryCastErrorMessage>(vals, result, count, strict);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return TransformDateTime<dtime_t, TryCastErrorMessage>(vals, result, count, strict);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return TransformTimestamp<TryCast>(vals, result, count, strict);
	case LogicalTypeId::TIMESTAMP_NS:
		return TransformTimestamp<TryCastToTimestampNS>(vals, result, count, strict);
	case LogicalTypeId::TIMESTAMP_MS:
		return TransformTimestamp<TryCastToTimestampMS>(vals, result, count, strict);
	case LogicalTypeId::TIMESTAMP_SEC:
		return TransformTimestamp<TryCastToTimestampSec>(vals, result, count, strict);
	case LogicalTypeId::INTERVAL:
		return TransformDateTime<interval_t, TryCastErrorMessage>(vals, result, count, strict);
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		return TransformString(vals, result, count);
	case LogicalTypeId::SQLNULL:
		return;
	case LogicalTypeId::STRUCT:
		return TransformObject(vals, result, count, result_type, strict);
	case LogicalTypeId::LIST:
		return TransformArray(vals, result, count, strict);
	default:
		throw InternalException("Unexpected type arrived at Transform");
	}
}

template <bool strict>
static void TransformFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();
	auto &input = args.data[0];
	VectorData input_data;
	input.Orrify(count, input_data);
	auto inputs = (string_t *)input_data.data;
	// Read documents
	yyjson_doc *docs[STANDARD_VECTOR_SIZE];
	yyjson_val *vals[STANDARD_VECTOR_SIZE];
	auto &result_validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		auto idx = input_data.sel->get_index(i);
		if (!input_data.validity.RowIsValid(idx)) {
			docs[i] = nullptr;
			vals[i] = nullptr;
			result_validity.SetInvalid(i);
		} else {
			docs[i] = JSONCommon::ReadDocument(inputs[idx]);
			vals[i] = docs[i]->root;
		}
	}
	// Transform
	Transform(vals, result, count, strict);
	// Free documents again
	for (idx_t i = 0; i < count; i++) {
		yyjson_doc_free(docs[i]);
	}
}

CreateScalarFunctionInfo JSONFunctions::GetTransformFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("json_transform", {LogicalType::JSON, LogicalType::JSON},
	                                               LogicalType::ANY, TransformFunction<false>, false, JSONTransformBind,
	                                               nullptr, nullptr));
}

CreateScalarFunctionInfo JSONFunctions::GetTransformStrictFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("json_transform_strict", {LogicalType::JSON, LogicalType::JSON},
	                                               LogicalType::ANY, TransformFunction<true>, false, JSONTransformBind,
	                                               nullptr, nullptr));
}

} // namespace duckdb
