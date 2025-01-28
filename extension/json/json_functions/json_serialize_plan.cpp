#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "json_common.hpp"
#include "json_deserializer.hpp"
#include "json_functions.hpp"
#include "json_serializer.hpp"

namespace duckdb {

//-----------------------------------------------------------------------------
// json_serialize_plan
//-----------------------------------------------------------------------------
struct JsonSerializePlanBindData : public FunctionData {
	bool skip_if_null = false;
	bool skip_if_empty = false;
	bool skip_if_default = false;
	bool format = false;
	bool optimize = false;

	JsonSerializePlanBindData(bool skip_if_null_p, bool skip_if_empty_p, bool skip_if_default_p, bool format_p,
	                          bool optimize_p)
	    : skip_if_null(skip_if_null_p), skip_if_empty(skip_if_empty_p), skip_if_default(skip_if_default_p),
	      format(format_p), optimize(optimize_p) {
	}

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<JsonSerializePlanBindData>(skip_if_null, skip_if_empty, skip_if_default, format, optimize);
	}
	bool Equals(const FunctionData &other_p) const override {
		return true;
	}
};

static unique_ptr<FunctionData> JsonSerializePlanBind(ClientContext &context, ScalarFunction &bound_function,
                                                      vector<unique_ptr<Expression>> &arguments) {
	if (arguments.empty()) {
		throw BinderException("json_serialize_plan takes at least one argument");
	}

	if (arguments[0]->return_type != LogicalType::VARCHAR) {
		throw InvalidTypeException("json_serialize_plan first argument must be a VARCHAR");
	}

	// Optional arguments
	bool skip_if_null = false;
	bool skip_if_empty = false;
	bool skip_if_default = false;
	bool format = false;
	bool optimize = false;

	for (idx_t i = 1; i < arguments.size(); i++) {
		auto &arg = arguments[i];
		if (arg->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!arg->IsFoldable()) {
			throw BinderException("json_serialize_plan: arguments must be constant");
		}
		auto &alias = arg->GetAlias();
		if (alias == "skip_null") {
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("json_serialize_plan: 'skip_null' argument must be a boolean");
			}
			skip_if_null = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else if (alias == "skip_empty") {
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("json_serialize_plan: 'skip_empty' argument must be a boolean");
			}
			skip_if_empty = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else if (alias == "skip_default") {
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("json_serialize_plan: 'skip_default' argument must be a boolean");
			}
			skip_if_default = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else if (alias == "format") {
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("json_serialize_plan: 'format' argument must be a boolean");
			}
			format = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else if (alias == "optimize") {
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("json_serialize_plan: 'optimize' argument must be a boolean");
			}
			optimize = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else {
			throw BinderException(StringUtil::Format("json_serialize_plan: Unknown argument '%s'", alias));
		}
	}
	return make_uniq<JsonSerializePlanBindData>(skip_if_null, skip_if_empty, skip_if_default, format, optimize);
}

static bool OperatorSupportsSerialization(LogicalOperator &op, string &operator_name) {
	for (auto &child : op.children) {
		if (!OperatorSupportsSerialization(*child, operator_name)) {
			return false;
		}
	}
	auto supported = op.SupportSerialization();
	if (!supported) {
		operator_name = EnumUtil::ToString(op.type);
	}
	return supported;
}

static void JsonSerializePlanFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &local_state = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = local_state.json_allocator.GetYYAlc();
	auto &inputs = args.data[0];

	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.bind_info->Cast<JsonSerializePlanBindData>();

	if (!state.HasContext()) {
		throw InvalidInputException("json_serialize_plan: No client context available");
	}
	auto &context = state.GetContext();

	UnaryExecutor::Execute<string_t, string_t>(inputs, result, args.size(), [&](string_t input) {
		auto doc = JSONCommon::CreateDocument(alc);
		auto result_obj = yyjson_mut_obj(doc);
		yyjson_mut_doc_set_root(doc, result_obj);

		try {
			Parser parser;
			parser.ParseQuery(input.GetString());
			auto plans_arr = yyjson_mut_arr(doc);

			for (auto &statement : parser.statements) {
				auto stmt = std::move(statement);

				Planner planner(context);
				planner.CreatePlan(std::move(stmt));
				auto plan = std::move(planner.plan);

				if (info.optimize && plan->RequireOptimizer()) {
					Optimizer optimizer(*planner.binder, context);
					plan = optimizer.Optimize(std::move(plan));
				}

				ColumnBindingResolver resolver;
				resolver.Verify(*plan);
				resolver.VisitOperator(*plan);
				plan->ResolveOperatorTypes();

				string operator_name;
				if (!OperatorSupportsSerialization(*plan, operator_name)) {
					throw InvalidInputException("Operator '%s' does not support serialization", operator_name);
				}

				auto plan_json =
				    JsonSerializer::Serialize(*plan, doc, info.skip_if_null, info.skip_if_empty, info.skip_if_default);
				yyjson_mut_arr_append(plans_arr, plan_json);
			}

			yyjson_mut_obj_add_false(doc, result_obj, "error");
			yyjson_mut_obj_add_val(doc, result_obj, "plans", plans_arr);

			idx_t len;
			auto data = yyjson_mut_val_write_opts(result_obj,
			                                      info.format ? JSONCommon::WRITE_PRETTY_FLAG : JSONCommon::WRITE_FLAG,
			                                      alc, reinterpret_cast<size_t *>(&len), nullptr);
			if (data == nullptr) {
				throw SerializationException(
				    "Failed to serialize json, perhaps the query contains invalid utf8 characters?");
			}

			return StringVector::AddString(result, data, len);

		} catch (std::exception &ex) {
			ErrorData error(ex);
			yyjson_mut_obj_add_true(doc, result_obj, "error");
			// error type and message
			yyjson_mut_obj_add_strcpy(doc, result_obj, "error_type",
			                          StringUtil::Lower(Exception::ExceptionTypeToString(error.Type())).c_str());
			yyjson_mut_obj_add_strcpy(doc, result_obj, "error_message", error.RawMessage().c_str());
			// add extra info
			for (auto &entry : error.ExtraInfo()) {
				yyjson_mut_obj_add_strcpy(doc, result_obj, entry.first.c_str(), entry.second.c_str());
			}

			idx_t len;
			auto data = yyjson_mut_val_write_opts(result_obj,
			                                      info.format ? JSONCommon::WRITE_PRETTY_FLAG : JSONCommon::WRITE_FLAG,
			                                      alc, reinterpret_cast<size_t *>(&len), nullptr);
			return StringVector::AddString(result, data, len);
		}
	});
}

ScalarFunctionSet JSONFunctions::GetSerializePlanFunction() {
	ScalarFunctionSet set("json_serialize_plan");

	set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::JSON(), JsonSerializePlanFunction,
	                               JsonSerializePlanBind, nullptr, nullptr, JSONFunctionLocalState::Init));

	set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BOOLEAN}, LogicalType::JSON(),
	                               JsonSerializePlanFunction, JsonSerializePlanBind, nullptr, nullptr,
	                               JSONFunctionLocalState::Init));

	set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN},
	                               LogicalType::JSON(), JsonSerializePlanFunction, JsonSerializePlanBind, nullptr,
	                               nullptr, JSONFunctionLocalState::Init));

	set.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::JSON(),
	    JsonSerializePlanFunction, JsonSerializePlanBind, nullptr, nullptr, JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN},
	    LogicalType::JSON(), JsonSerializePlanFunction, JsonSerializePlanBind, nullptr, nullptr,
	    JSONFunctionLocalState::Init));
	return set;
}

} // namespace duckdb
