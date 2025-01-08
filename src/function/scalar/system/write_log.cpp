#include "duckdb/function/scalar/system_functions.hpp"
#include "duckdb/function/scalar/system_functions.hpp"

#include "utf8proc.hpp"

namespace duckdb {

struct WriteLogBindData : FunctionData {
	//! Config
	bool disable_logging = false;
	string logger;
	LogLevel level = LogLevel::L_INFO;
	string type;

	//! Context
	optional_ptr<ClientContext> context;
	optional_ptr<DatabaseInstance> instance;

	explicit WriteLogBindData() {};
	explicit WriteLogBindData(const WriteLogBindData& other) {
		disable_logging = other.disable_logging;
		logger = other.logger;
		level = other.level;
		type = other.type;

		context = other.context;
		instance = other.instance;
	}

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<WriteLogBindData>(*this);
	}
	bool Equals(const FunctionData &other_p) const override {
		return true;
	}
};

unique_ptr<FunctionData> WriteLogBind(ClientContext &context, ScalarFunction &bound_function,
                                      vector<unique_ptr<Expression>> &arguments) {
	if (arguments.empty()) {
		throw BinderException("write_log takes at least one argument");
	}

	if (arguments[0]->return_type != LogicalType::VARCHAR) {
		throw InvalidTypeException("write_log first argument must be a VARCHAR");
	}

	// Used to replace the actual log call with a nop: useful for benchmarking
	auto result = make_uniq<WriteLogBindData>();

	for (idx_t i = 1; i < arguments.size(); i++) {
		auto &arg = arguments[i];
		if (arg->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!arg->IsFoldable()) {
			throw BinderException("write_log: arguments must be constant");
		}
		if (arg->alias == "disable_logging") {
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("write_log: 'disable_logging' argument must be a boolean");
			}
			result->disable_logging = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else if (arg->alias == "logger") {
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("write_log: 'logger' argument must be a string");
			}
			result->logger = StringValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else if (arg->alias == "level") {
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("write_log: 'level' argument must be a string");
			}
			result->level =
			    EnumUtil::FromString<LogLevel>(StringValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg)));
		} else if (arg->alias == "log_type") {
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("write_log: 'log_type' argument must be a string");
			}
			result->type = StringValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else {
			throw BinderException(StringUtil::Format("write_log: Unknown argument '%s'", arg->alias));
		}
	}

	result->context = context;

	return std::move(result);
}
static void WriteLogFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() > 1);

	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.bind_info->Cast<WriteLogBindData>();

	UnifiedVectorFormat idata;
	args.data[0].ToUnifiedFormat(args.size(), idata);

	auto input_data = UnifiedVectorFormat::GetData<string_t>(idata);

	if (!info.disable_logging) {
		for (idx_t i = 0; i < args.size(); i++) {
			// Note: use lambda to avoid copy TODO: add string_t specialization
			Logger::Info(*info.context, [&]() { return input_data[i].GetString(); });
		}
	}

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto result_data = ConstantVector::GetData<string_t>(result);
	result_data[0] = string_t();
	ConstantVector::Validity(result).Set(0, false);
}

ScalarFunctionSet WriteLogFun::GetFunctions() {
	ScalarFunctionSet set("json_serialize_plan");

	set.AddFunction(ScalarFunction("write_log", {LogicalType::VARCHAR}, LogicalType::VARCHAR, WriteLogFunction, WriteLogBind, nullptr, nullptr, nullptr, LogicalType(LogicalTypeId::INVALID), FunctionStability::VOLATILE));
	set.AddFunction(ScalarFunction("write_log", {LogicalType::VARCHAR, LogicalType::ANY}, LogicalType::VARCHAR, WriteLogFunction, WriteLogBind, nullptr, nullptr, nullptr, LogicalType(LogicalTypeId::INVALID), FunctionStability::VOLATILE));

	return set;
}

} // namespace duckdb
