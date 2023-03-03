#include "json_functions.hpp"
#include "json_serializer.hpp"
#include "json_deserializer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {


struct JsonSerializeBindData : public FunctionData {
	bool skip_if_null = false;
	bool skip_if_empty = false;

	JsonSerializeBindData(bool skip_if_null_p, bool skip_if_empty_p)
	    : skip_if_null(skip_if_null_p), skip_if_empty(skip_if_empty_p) { }
public:
	unique_ptr<FunctionData> Copy() const override {
		return make_unique<JsonSerializeBindData>(skip_if_null, skip_if_empty);
	}
	bool Equals(const FunctionData &other_p) const override {
		return true;
	}
};

static unique_ptr<FunctionData> JsonSerializeBind(ClientContext &context, ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments) {
	if (arguments.empty()) {
		throw BinderException("json_serialize_sql takes at least one argument");
	}

	if(arguments[0]->return_type != LogicalType::VARCHAR) {
		throw InvalidTypeException("json_serialize_sql first argument must be a VARCHAR");
	}

	// Optional arguments

	bool skip_if_null = false;
	bool skip_if_empty = false;

	if (arguments.size() > 1 && arguments.size() < 4) {
		for(idx_t i = 1; i < arguments.size(); i++) {
			auto &arg = arguments[i];
			if (arg->alias == "skip_if_null") {
				if (arg->HasParameter()) {
					throw ParameterNotResolvedException();
				}
				if (!arg->IsFoldable()) {
					throw InvalidInputException("skip_if_null argument must be constant");
				}
				if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
					throw InvalidTypeException("skip_if_null argument must be a boolean");
				}
				skip_if_null = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
			} else if(arg->alias == "skip_if_empty") {
				if (arg->HasParameter()) {
					throw ParameterNotResolvedException();
				}
				if (!arg->IsFoldable()) {
					throw InvalidInputException("skip_if_empty argument must be constant");
				}
				if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
					throw InvalidTypeException("skip_if_empty argument must be a boolean");
				}
				skip_if_empty = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
			}
		}
	}
	return make_unique<JsonSerializeBindData>(skip_if_null, skip_if_empty);
}


static void JsonSerializeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &local_state = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = local_state.json_allocator.GetYYJSONAllocator();
	auto &inputs = args.data[0];

	auto &func_expr = (BoundFunctionExpression &)state.expr;
	const auto &info = (JsonSerializeBindData &)*func_expr.bind_info;


	UnaryExecutor::Execute<string_t, string_t>(inputs, result, args.size(), [&](string_t input) {

		auto doc = JSONCommon::CreateDocument(alc);
		auto result_obj = yyjson_mut_obj(doc);
		yyjson_mut_doc_set_root(doc, result_obj);

		try {
			auto parser = Parser();
			parser.ParseQuery(input.GetString());

			auto statements_arr = yyjson_mut_arr(doc);

			for (auto &statement : parser.statements) {
				if (statement->type != StatementType::SELECT_STATEMENT) {
					throw NotImplementedException("Only SELECT statements can be serialized to json!");
				}
				auto &select = (SelectStatement &)*statement;
				auto serializer = JsonSerializer(doc, info.skip_if_null, info.skip_if_empty);
				select.FormatSerialize(serializer);
				auto json = serializer.GetRootObject();

				yyjson_mut_arr_append(statements_arr, json);
			}

			yyjson_mut_obj_add_false(doc, result_obj, "error");
			yyjson_mut_obj_add_val(doc, result_obj, "statements", statements_arr);
			return JSONCommon::WriteVal(result_obj, alc);

		} catch (Exception &exception) {
			yyjson_mut_obj_add_true(doc, result_obj, "error");
			yyjson_mut_obj_add_strcpy(doc, result_obj, "error_type", StringUtil::Lower(exception.ExceptionTypeToString(exception.type)).c_str());
			yyjson_mut_obj_add_strcpy(doc, result_obj, "error_message", exception.RawMessage().c_str());
			return JSONCommon::WriteVal(result_obj, alc);
		}
	});
}

static void JsonDeserializeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &local_state = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = local_state.json_allocator.GetYYJSONAllocator();
	auto &inputs = args.data[0];

	UnaryExecutor::Execute<string_t, string_t>(inputs, result, args.size(), [&](string_t input) {
		auto doc = JSONCommon::ReadDocument(input, JSONCommon::READ_FLAG, alc);
		if(!doc) {
			throw ParserException("Could not parse json");
		}
		auto root = doc->root;
		auto err = yyjson_obj_get(root, "error");
		if(err && yyjson_is_true(err)) {
			auto err_type = yyjson_obj_get(root, "error_type");
			auto err_msg = yyjson_obj_get(root, "error_message");
			if(err_type && err_msg) {
				throw ParserException("Error parsing json: %s: %s", yyjson_get_str(err_type), yyjson_get_str(err_msg));
			}
			throw ParserException("Error parsing json");
		}

		auto statements = yyjson_obj_get(root, "statements");
		if(!statements || !yyjson_is_arr(statements)) {
			throw ParserException("Error parsing json: no statements array");
		}
		auto size = yyjson_arr_size(statements);
		if(size == 0) {
			return string_t();
		}
		auto stmt_json = yyjson_arr_get(statements, 0);
		JsonDeserializer deserializer(stmt_json, doc);
		auto node = QueryNode::FormatDeserialize(deserializer);


		return StringVector::AddString(result, "stmt_str");
	});
}

CreateScalarFunctionInfo JSONFunctions::GetSerializeSqlFunction() {
	ScalarFunctionSet set("json_serialize_sql");
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, JSONCommon::JSONType(),
	                               JsonSerializeFunction, JsonSerializeBind, nullptr, nullptr, JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BOOLEAN}, JSONCommon::JSONType(),
	                                   JsonSerializeFunction, JsonSerializeBind, nullptr, nullptr, JSONFunctionLocalState::Init));

	set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN}, JSONCommon::JSONType(),
	                               JsonSerializeFunction, JsonSerializeBind, nullptr, nullptr, JSONFunctionLocalState::Init));

	return CreateScalarFunctionInfo(set);
}

CreateScalarFunctionInfo JSONFunctions::GetDeserializeSqlFunction() {
	ScalarFunctionSet set("json_deserialize_sql");
	set.AddFunction(ScalarFunction({JSONCommon::JSONType()}, LogicalType::VARCHAR,
	                               JsonDeserializeFunction, nullptr, nullptr, nullptr, JSONFunctionLocalState::Init));

	return CreateScalarFunctionInfo(set);
}

} // namespace duckdb
