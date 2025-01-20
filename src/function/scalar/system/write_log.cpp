#include "duckdb/function/scalar/system_functions.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "utf8proc.hpp"

namespace duckdb {

struct WriteLogBindData : FunctionData {
	//! Config
	bool disable_logging = false;
	string scope;
	LogLevel level = LogLevel::LOG_INFO;
	string type;

	//! Context
	optional_ptr<ClientContext> context;

	//! Output
	idx_t output_col = DConstants::INVALID_INDEX;
	LogicalType return_type;

	explicit WriteLogBindData() {};
	WriteLogBindData(const WriteLogBindData &other) {
		disable_logging = other.disable_logging;
		scope = other.scope;
		level = other.level;
		type = other.type;

		context = other.context;

		output_col = other.output_col;
		return_type = other.return_type;
	}

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<WriteLogBindData>(*this);
	}
	bool Equals(const FunctionData &other_p) const override {
		return true;
	}
};

static void ThrowIfNotConstant(const Expression &arg) {
	if (!arg.IsFoldable()) {
		throw BinderException("write_log: argument '%s' must be constant", arg.alias);
	}
}

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

	// Default return type
	bound_function.return_type = LogicalType::VARCHAR;

	for (idx_t i = 1; i < arguments.size(); i++) {
		auto &arg = arguments[i];
		if (arg->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (arg->alias == "disable_logging") {
			ThrowIfNotConstant(*arg);
			if (arg->return_type.id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("write_log: 'disable_logging' argument must be a boolean");
			}
			result->disable_logging = BooleanValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else if (arg->alias == "scope") {
			ThrowIfNotConstant(*arg);
			if (arg->return_type.id() != LogicalTypeId::VARCHAR) {
				throw BinderException("write_log: 'scope' argument must be a string");
			}
			result->scope = StringValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else if (arg->alias == "level") {
			ThrowIfNotConstant(*arg);
			if (arg->return_type.id() != LogicalTypeId::VARCHAR) {
				throw BinderException("write_log: 'level' argument must be a string");
			}
			result->level =
			    EnumUtil::FromString<LogLevel>(StringValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg)));
		} else if (arg->alias == "log_type") {
			ThrowIfNotConstant(*arg);
			if (arg->return_type.id() != LogicalTypeId::VARCHAR) {
				throw BinderException("write_log: 'log_type' argument must be a string");
			}
			result->type = StringValue::Get(ExpressionExecutor::EvaluateScalar(context, *arg));
		} else if (arg->alias == "return_value") {
			result->return_type = arg->return_type;
			result->output_col = i;
			bound_function.return_type = result->return_type;
		} else {
			throw BinderException(StringUtil::Format("write_log: Unknown argument '%s'", arg->alias));
		}
	}

	result->context = context;

	return std::move(result);
}

template <class T>
static void WriteLogValues(T &LogSource, LogLevel level, const string_t *data, const SelectionVector *sel, idx_t size,
                           const string &type) {
	if (!type.empty()) {
		for (idx_t i = 0; i < size; i++) {
			DUCKDB_LOG(LogSource, type.c_str(), level, data[sel->get_index(i)]);
		}
	} else {
		for (idx_t i = 0; i < size; i++) {
			DUCKDB_LOG(LogSource, type.c_str(), level, data[sel->get_index(i)]);
		}
	}
}

static void WriteLogFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() >= 1);

	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.bind_info->Cast<WriteLogBindData>();

	UnifiedVectorFormat idata;
	args.data[0].ToUnifiedFormat(args.size(), idata);

	auto input_data = UnifiedVectorFormat::GetData<string_t>(idata);

	if (!info.disable_logging) {
		if (info.scope.empty() || info.scope == "connection") {
			WriteLogValues<const ClientContext>(*info.context, info.level, input_data, idata.sel, args.size(),
			                                    info.type);
		} else if (info.scope == "database") {
			WriteLogValues<const DatabaseInstance>(*info.context->db, info.level, input_data, idata.sel, args.size(),
			                                       info.type);
		} else if (info.scope == "file_opener") {
			WriteLogValues<const FileOpener>(*info.context->client_data->file_opener, info.level, input_data, idata.sel,
			                                 args.size(), info.type);
		} else {
			throw InvalidInputException(
			    "write_log: 'scope' argument unknown: '%s'. Valid values are [connection, database, file_opener]",
			    info.scope);
		}
	}

	if (info.output_col != DConstants::INVALID_INDEX) {
		result.Reference(args.data[info.output_col]);
	} else {
		result.Reference(Value(LogicalType::VARCHAR));
	}
}

ScalarFunctionSet WriteLogFun::GetFunctions() {
	ScalarFunctionSet set("write_log");

	set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::ANY, WriteLogFunction, WriteLogBind, nullptr,
	                               nullptr, nullptr, LogicalType::ANY, FunctionStability::VOLATILE));

	return set;
}

} // namespace duckdb
