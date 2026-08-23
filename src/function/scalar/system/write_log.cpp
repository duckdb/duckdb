#include "duckdb/function/scalar/system_functions.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "utf8proc.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_argument_pack.hpp"

namespace duckdb {

namespace {

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

//! The value of one of the options collected by "**options", which must be a constant
Value GetOptionConstant(ClientContext &context, Expression &option, const Identifier &name) {
	if (option.HasParameter() || option.GetReturnType().id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}
	if (!option.IsFoldable()) {
		throw BinderException(option, "write_log: '%s' argument must be a constant expression", name);
	}
	return ExpressionExecutor::EvaluateScalar(context, option);
}

unique_ptr<FunctionData> WriteLogBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &bound_function = input.GetBoundFunction();
	auto &options = *input.GetArguments()[1];

	// Used to replace the actual log call with a nop: useful for benchmarking
	auto result = make_uniq<WriteLogBindData>();

	// Default return type
	bound_function.SetReturnType(LogicalType::VARCHAR);

	auto &option_types = ArgumentPack::GetTypes(options.GetReturnType());
	auto &option_args = ArgumentPack::GetPackedChildren(options);
	for (idx_t i = 0; i < option_args.size(); i++) {
		const auto &name = option_types[i].first;
		auto &arg = *option_args[i];
		if (name == "disable_logging") {
			if (arg.GetReturnType().id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("write_log: 'disable_logging' argument must be a boolean");
			}
			result->disable_logging = BooleanValue::Get(GetOptionConstant(context, arg, name));
		} else if (name == "scope") {
			if (arg.GetReturnType().id() != LogicalTypeId::VARCHAR) {
				throw BinderException("write_log: 'scope' argument must be a string");
			}
			result->scope = StringValue::Get(GetOptionConstant(context, arg, name));
		} else if (name == "level") {
			if (arg.GetReturnType().id() != LogicalTypeId::VARCHAR) {
				throw BinderException("write_log: 'level' argument must be a string");
			}
			result->level = EnumUtil::FromString<LogLevel>(StringValue::Get(GetOptionConstant(context, arg, name)));
		} else if (name == "log_type") {
			if (arg.GetReturnType().id() != LogicalTypeId::VARCHAR) {
				throw BinderException("write_log: 'log_type' argument must be a string");
			}
			result->type = StringValue::Get(GetOptionConstant(context, arg, name));
		} else if (name == "return_value") {
			result->return_type = arg.GetReturnType();
			result->output_col = i;
			bound_function.SetReturnType(result->return_type);
		} else {
			throw BinderException(StringUtil::Format("write_log: Unknown argument '%s'", name));
		}
	}

	result->context = context;

	return std::move(result);
}

template <class T>
void WriteLogValues(T &LogSource, LogLevel level, const string_t *data, const SelectionVector *sel, idx_t size,
                    const string &type) {
	for (idx_t i = 0; i < size; i++) {
		DUCKDB_LOG_INTERNAL(LogSource, type.c_str(), level, data[sel->get_index(i)]);
	}
}

void WriteLogFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);

	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.BindInfo()->Cast<WriteLogBindData>();

	UnifiedVectorFormat idata;
	args.data[0].ToUnifiedFormat(idata);

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
		result.Reference(ArgumentPack::GetInput(args.data[1])[info.output_col]);
	} else {
		ConstantVector::SetNull(result, count_t(args.size()));
	}
}

} // namespace

ScalarFunctionSet WriteLogFun::GetFunctions() {
	ScalarFunctionSet set("write_log");

	auto sig = FunctionSignature()
	               .AddParameter("string", LogicalType::VARCHAR)
	               .AddVarKeywordParameter("options", LogicalType::ANY)
	               .SetReturnType(LogicalType::ANY);
	set.AddFunction(ScalarFunction("write_log", std::move(sig))
	                    .SetFunctionCallback(WriteLogFunction)
	                    .SetBindCallback(WriteLogBind)
	                    .SetVolatile());

	return set;
}

} // namespace duckdb
