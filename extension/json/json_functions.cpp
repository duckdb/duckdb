#include "json_functions.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "yyjson.hpp"

namespace duckdb {

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

struct JSONFunctionData : public FunctionData {
public:
	explicit JSONFunctionData(string arg_p) : arg(move(arg_p)), path_ptr(arg.c_str()) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<JSONFunctionData>(arg);
	}

	template <class T>
	static inline bool TemplatedExtract(const string_t &input, const char *path_ptr_p, T &result) {
		// TODO: check if YYJSON_READ_ALLOW_INF_AND_NAN may be better?
		yyjson_doc *doc = yyjson_read(input.GetDataUnsafe(), input.GetSize(), YYJSON_READ_NOFLAG);
		yyjson_val *val = yyjson_doc_get_pointer(doc, path_ptr_p);
		return GetVal<T>(val, result);
	}

	template <class T>
	inline bool TemplatedExtract(const string_t &input, T &result) const {
		return TemplatedExtract<T>(input, path_ptr, result);
	}

public:
	const string arg;
	const char *path_ptr;
};

static inline string ConvertToPath(const string &str) {
	if (str.rfind('/', 0) == 0) {
		// Already a path string
		return str;
	} else if (str.rfind('$', 0) == 0) {
		// Dollar/dot syntax
		return StringUtil::Replace(str, ".", "/");
	} else {
		// Plain tag/array index
		return "/" + str;
	}
}

template <PhysicalType TYPE>
static unique_ptr<FunctionData> JSONBind(ClientContext &context, ScalarFunction &bound_function,
                                         vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	string path = "";
	if (arguments[1]->return_type.id() != LogicalTypeId::SQLNULL && arguments[1]->IsFoldable()) {
		auto str = StringValue::Get(ExpressionExecutor::EvaluateScalar(*arguments[1]));
		path = ConvertToPath(str);
	}
	return make_unique<JSONFunctionData>(path);
}

struct UnaryExtractWrapper {
	template <class FUNC, class T>
	static inline T Operation(string_t input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto fun = (FUNC *)dataptr;
		T result;
		if (!(*fun)(input, result)) {
			mask.SetInvalid(idx);
		}
		return result;
	}
};

struct BinaryExtractWrapper {
	template <class FUNC, class T>
	static inline T Operation(string_t input, string_t path, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto fun = (FUNC *)dataptr;
		T result;
		if (!(*fun)(input, result)) {
			mask.SetInvalid(idx);
		}
		return result;
	}
};

template <class T>
static void TemplatedExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	const auto &info = (JSONFunctionData &)*func_expr.bind_info;

	auto &strings = args.data[0];
	if (info.arg.empty()) {
		// Column tag
		auto &paths = args.data[1];
		BinaryExecutor::Execute<string_t, string_t, T, BinaryExtractWrapper>(strings, paths, result, args.size());
	} else {
		// Constant tag
		using lambda_fun = std::function<bool(string_t input, T &result)>;
//		const char *path_ptr = info.arg.c_str();
//		bool (*lambda)(string_t, T &) = [&](string_t input, T &result) {
//			return info.TemplatedExtract<T>(input, path_ptr, result);
//		};
		UnaryExecutor::GenericExecute<string_t, T, UnaryExtractWrapper>(strings, result, args.size(), (void *)lambda);
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
