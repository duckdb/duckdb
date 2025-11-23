#include "duckdb/function/scalar/system_functions.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/common/types/data_chunk.hpp"

#include "utf8proc.hpp"

namespace duckdb {

namespace {

struct ParseLogMessageData : FunctionData {
	explicit ParseLogMessageData(const LogType &log_type_p) : log_type(log_type_p) {
	}
	const LogType &log_type;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ParseLogMessageData>(log_type);
	}
	bool Equals(const FunctionData &other_p) const override {
		return log_type.name == other_p.Cast<ParseLogMessageData>().log_type.name;
	}
};

unique_ptr<FunctionData> ParseLogMessageBind(ClientContext &context, ScalarFunction &bound_function,
                                             vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 2) {
		throw BinderException("structured_log_schema: expects 1 argument", arguments[0]->alias);
	}

	if (!arguments[0]->IsFoldable()) {
		throw BinderException("structured_log_schema: argument '%s' must be constant", arguments[0]->alias);
	}

	if (arguments[0]->return_type.id() != LogicalTypeId::VARCHAR) {
		throw BinderException("structured_log_schema: 'log_type' argument must be a string");
	}

	auto type = StringValue::Get(ExpressionExecutor::EvaluateScalar(context, *arguments[0]));

	auto lookup = LogManager::Get(context).LookupLogType(type);

	if (!lookup) {
		throw InvalidInputException("structured_log_schema: '%s' not found", type);
	}

	if (!lookup->is_structured) {
		// Unstructured types we simply wrap in a struct with a single field called message
		child_list_t<LogicalType> children = {{"message", LogicalType::VARCHAR}};
		bound_function.SetReturnType(LogicalType::STRUCT(children));
	} else {
		bound_function.SetReturnType(lookup->type);
	}

	return make_uniq<ParseLogMessageData>(*lookup);
}

void ParseLogMessageFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.bind_info->Cast<ParseLogMessageData>();

	if (info.log_type.is_structured) {
		// TODO: allow more complex parsing operations than DefaultCast
		VectorOperations::DefaultCast(args.data[1], result, args.size(), true);
	} else {
		auto &struct_entries = StructVector::GetEntries(result);
		struct_entries[0]->Reference(args.data[1]);
	}
}

} // namespace

ScalarFunction ParseLogMessage::GetFunction() {
	auto fun = ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::ANY, ParseLogMessageFunction,
	                          ParseLogMessageBind, nullptr, nullptr, nullptr, LogicalType(LogicalTypeId::INVALID));
	fun.errors = FunctionErrors::CAN_THROW_RUNTIME_ERROR;
	return fun;
}

} // namespace duckdb
