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
	child_list_t<LogicalType> child_types;
	yyjson_val *key, *val;
	yyjson_obj_iter iter;
	yyjson_obj_iter_init(obj, &iter);
	while ((key = yyjson_obj_iter_next(&iter))) {
		val = yyjson_obj_iter_get_val(key);
		child_types.emplace_back(yyjson_get_str(key), StructureToType(val));
	}
	return LogicalType::STRUCT(child_types);
}

static LogicalType StructureToTypeVal(yyjson_val *val) {
	auto type_string = StringUtil::Lower(yyjson_get_str(val));
	LogicalType result;
	if ((StringUtil::StartsWith(type_string, "decimal(") || StringUtil::StartsWith(type_string, "dec(") ||
	     StringUtil::StartsWith(type_string, "numeric(")) &&
	    StringUtil::EndsWith(type_string, ")")) {
		// cut out string
		auto start = type_string.find('(') + 1;
		auto end = type_string.size() - 1;
		auto ws_string = string(type_string.c_str() + start, end - start);
		auto split = StringUtil::Split(ws_string, ',');
		if (split.size() == 1 || split.size() == 2) {
			vector<idx_t> ws;
			for (auto &s : split) {
				idx_t digit;
				if (!JSONCommon::ReadIndex(s.c_str(), s.c_str() + s.size(), digit)) {
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
		case LogicalTypeId::USER:
		case LogicalTypeId::STRUCT:
		case LogicalTypeId::MAP:
		case LogicalTypeId::LIST:
			throw InvalidInputException("unsupported type in JSON structure: \"%s\"", type_string);
		default:
			break;
		}
	}
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
static void Transform(yyjson_val *vals[], Vector &result, const idx_t count);

template <class T>
static void TemplatedTransform(yyjson_val *vals[], Vector &result, const idx_t count) {
	auto data = (T *)FlatVector::GetData(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || yyjson_is_null(val) || !JSONCommon::TemplatedGetValue<T>(val, data[i])) {
			validity.SetInvalid(i);
		}
	}
}

static void TransformString(yyjson_val *vals[], Vector &result, const idx_t count) {
	auto data = (string_t *)FlatVector::GetData(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		string_t result_val {};
		if (!val || yyjson_is_null(val) || !JSONCommon::TemplatedGetValue<string_t>(val, result_val)) {
			validity.SetInvalid(i);
		} else {
			data[i] = StringVector::AddString(result, result_val);
		}
	}
}

static void TransformObject(yyjson_val *vals[], Vector &result, const idx_t count, const LogicalType &type) {
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
		Transform(nested_vals, *child_vs[child_i], count);
	}
}

static void TransformArray(yyjson_val *vals[], Vector &result, const idx_t count) {
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
	Transform(nested_vals, ListVector::GetEntry(result), offset);
}

static void Transform(yyjson_val *vals[], Vector &result, const idx_t count) {
	auto result_type = result.GetType().id();
	switch (result_type) {
	case LogicalTypeId::SQLNULL:
		return;
	case LogicalTypeId::BOOLEAN:
		return TemplatedTransform<bool>(vals, result, count);
	case LogicalTypeId::BIGINT:
		return TemplatedTransform<int64_t>(vals, result, count);
	case LogicalTypeId::UBIGINT:
		return TemplatedTransform<uint64_t>(vals, result, count);
	case LogicalTypeId::DOUBLE:
		return TemplatedTransform<double>(vals, result, count);
	case LogicalTypeId::VARCHAR:
		return TransformString(vals, result, count);
	case LogicalTypeId::STRUCT:
		return TransformObject(vals, result, count, result.GetType());
	case LogicalTypeId::LIST:
		return TransformArray(vals, result, count);
	default:
		throw InternalException("Unexpected type arrived at Transform");
	}
}

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
	Transform(vals, result, count);
	// Free documents again
	for (idx_t i = 0; i < count; i++) {
		yyjson_doc_free(docs[i]);
	}
	result.SetVectorType(input.GetVectorType());
}

CreateScalarFunctionInfo JSONFunctions::GetTransformFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("json_transform", {LogicalType::JSON, LogicalType::JSON},
	                                               LogicalType::ANY, TransformFunction, false, JSONTransformBind,
	                                               nullptr, nullptr));
}

} // namespace duckdb
