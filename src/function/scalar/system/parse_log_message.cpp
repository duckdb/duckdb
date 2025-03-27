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
		throw NotImplementedException("The log type '%s', can not be parsed automatically. Please use `FROM "
		                              "duckdb_logs WHERE type='%s';` instead.",
		                              type, type);
	}

	bound_function.return_type = lookup->type;

	return make_uniq<ParseLogMessageData>(*lookup);
}

static void ParseLogMessageFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	VectorOperations::DefaultCast(args.data[1], result, args.size(), true);
}

ScalarFunction ParseLogMessage::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::ANY, ParseLogMessageFunction,
	                      ParseLogMessageBind, nullptr, nullptr, nullptr, LogicalType(LogicalTypeId::INVALID));
}

} // namespace duckdb
