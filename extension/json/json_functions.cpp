#include "json_functions.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "yyjson.hpp"

namespace duckdb {

static inline bool ConvertToPath(const string_t &input, string &result, idx_t &len) {
	len = input.GetSize();
	if (len == 0) {
		return false;
	}
	const char *ptr = input.GetDataUnsafe();
	if (*ptr == '/') {
		// Already a path string
		result = input.GetString();
	} else if (*ptr == '$') {
		// Dollar/dot syntax
		len--;
		result = StringUtil::Replace(string(ptr + 1, len), ".", "/");
	} else {
		// Plain tag/array index, prepend slash
		len++;
		result = "/" + input.GetString();
	}
	return true;
}

struct JSONFunctionData : public FunctionData {
public:
	explicit JSONFunctionData(string path_p, idx_t len) : path(move(path_p)), len(len) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<JSONFunctionData>(path, len);
	}

public:
	const string path;
	const size_t len;
};

template <PhysicalType TYPE>
static unique_ptr<FunctionData> JSONBind(ClientContext &context, ScalarFunction &bound_function,
                                         vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	string path = "";
	idx_t len = 0;
	if (arguments[1]->return_type.id() != LogicalTypeId::SQLNULL && arguments[1]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(*arguments[1]).GetValueUnsafe<string_t>();
		if (!ConvertToPath(val, path, len)) {
			throw InvalidInputException("Invalid path string");
		}
	}
	return make_unique<JSONFunctionData>(path, len);
}

template <class T>
inline bool GetVal(yyjson_val *val, T &result) {
	throw NotImplementedException("Cannot extract JSON of this type");
}

template <>
inline bool GetVal(yyjson_val *val, bool &result) {
	auto valid = yyjson_is_bool(val);
	if (valid) {
		result = unsafe_yyjson_get_bool(val);
	}
	return valid;
}

template <>
inline bool GetVal(yyjson_val *val, int32_t &result) {
	auto valid = yyjson_is_int(val);
	if (valid) {
		result = unsafe_yyjson_get_int(val);
	}
	return valid;
}

template <>
inline bool GetVal(yyjson_val *val, int64_t &result) {
	auto valid = yyjson_is_sint(val);
	if (valid) {
		result = unsafe_yyjson_get_sint(val);
	}
	return valid;
}

template <>
inline bool GetVal(yyjson_val *val, uint64_t &result) {
	auto valid = yyjson_is_uint(val);
	if (valid) {
		result = unsafe_yyjson_get_uint(val);
	}
	return valid;
}

template <>
inline bool GetVal(yyjson_val *val, double &result) {
	auto valid = yyjson_is_real(val);
	if (valid) {
		result = unsafe_yyjson_get_real(val);
	}
	return valid;
}

template <>
inline bool GetVal(yyjson_val *val, string_t &result) {
	auto valid = yyjson_is_str(val);
	if (valid) {
		result = string_t(unsafe_yyjson_get_str(val), unsafe_yyjson_get_len(val));
	}
	return valid;
}

template <class T>
static inline bool TemplatedExtract(const string_t &input, const char *ptr, const idx_t &len, T &result) {
	// TODO: check if YYJSON_READ_ALLOW_INF_AND_NAN may be better?
	yyjson_doc *doc = yyjson_read(input.GetDataUnsafe(), input.GetSize(), YYJSON_READ_NOFLAG);
	yyjson_val *val = unsafe_yyjson_get_pointer(yyjson_doc_get_root(doc), ptr, len);
	return GetVal<T>(val, result);
}

template <class T>
static void TemplatedExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	const auto &info = (JSONFunctionData &)*func_expr.bind_info;

	auto &strings = args.data[0];
	if (info.len == 0) {
		// Column tag
		auto &queries = args.data[1];
		BinaryExecutor::ExecuteWithNulls<string_t, string_t, T>(
		    strings, queries, result, args.size(), [&](string_t input, string_t query, ValidityMask &mask, idx_t idx) {
			    string path;
			    idx_t len;
			    T result_val {};
			    if (!ConvertToPath(query, path, len) || !TemplatedExtract<T>(input, path.c_str(), len, result_val)) {
				    mask.SetInvalid(idx);
			    }
			    return result_val;
		    });
	} else {
		// Constant tag
		const char *ptr = info.path.c_str();
		const idx_t &len = info.len;
		UnaryExecutor::ExecuteWithNulls<string_t, T>(strings, result, args.size(),
		                                             [&](string_t input, ValidityMask &mask, idx_t idx) {
			                                             T result_val {};
			                                             if (!TemplatedExtract<T>(input, ptr, len, result_val)) {
				                                             mask.SetInvalid(idx);
			                                             }
			                                             return result_val;
		                                             });
	}
}

vector<ScalarFunction> JSONFunctions::GetExtractFunctions() {
	vector<ScalarFunction> extract_functions;
	extract_functions.push_back(ScalarFunction("json_extract_bool", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                           LogicalType::BIGINT, TemplatedExtractFunction<bool>, false,
	                                           JSONBind<PhysicalType::BOOL>, nullptr, nullptr));
	extract_functions.push_back(ScalarFunction("json_extract_int", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                           LogicalType::INTEGER, TemplatedExtractFunction<int32_t>, false,
	                                           JSONBind<PhysicalType::INT32>, nullptr, nullptr));
	extract_functions.push_back(ScalarFunction("json_extract_bigint", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                           LogicalType::BIGINT, TemplatedExtractFunction<int64_t>, false,
	                                           JSONBind<PhysicalType::INT64>, nullptr, nullptr));
	extract_functions.push_back(ScalarFunction("json_extract_ubigint", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                           LogicalType::UBIGINT, TemplatedExtractFunction<uint64_t>, false,
	                                           JSONBind<PhysicalType::UINT64>, nullptr, nullptr));
	extract_functions.push_back(ScalarFunction("json_extract_double", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                           LogicalType::DOUBLE, TemplatedExtractFunction<double>, false,
	                                           JSONBind<PhysicalType::DOUBLE>, nullptr, nullptr));
	extract_functions.push_back(ScalarFunction("json_extract_string", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                           LogicalType::VARCHAR, TemplatedExtractFunction<string_t>, false,
	                                           JSONBind<PhysicalType::VARCHAR>, nullptr, nullptr));
	return extract_functions;
}

} // namespace duckdb
