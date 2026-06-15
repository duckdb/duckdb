#include "core_functions/scalar/generic_functions.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception/parser_exception.hpp"

namespace duckdb {

namespace {
struct CurrentSettingBindData : public FunctionData {
	explicit CurrentSettingBindData(Value value_p) : value(std::move(value_p)) {
	}

	Value value;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<CurrentSettingBindData>(value);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CurrentSettingBindData>();
		return Value::NotDistinctFrom(value, other.value);
	}
};

void CurrentSettingFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<CurrentSettingBindData>();
	result.Reference(info.value, count_t(args.size()));
}

void CurrentSettingDynamic(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	UnifiedVectorFormat name_data;
	args.data[0].ToUnifiedFormat(count, name_data);
	const auto *name_ptr = UnifiedVectorFormat::GetData<string_t>(name_data);
	auto *result_ptr = FlatVector::GetDataMutable<string_t>(result);
	auto &result_validity = FlatVector::ValidityMutable(result);
	for (idx_t row = 0; row < count; row++) {
		auto n_idx = name_data.sel->get_index(row);
		if (!name_data.validity.RowIsValid(n_idx)) {
			result_validity.SetInvalid(row);
			continue;
		}
		auto key = StringUtil::Lower(name_ptr[n_idx].GetString());
		Value val;
		if (!context.TryGetCurrentSetting(key, val)) {
			Catalog::AutoloadExtensionByConfigName(context, key);
			if (!context.TryGetCurrentSetting(key, val)) {
				throw InvalidInputException("unrecognized configuration parameter \"%s\"", key);
			}
		}
		val = Settings::FormatDisplayValue(context, val);
		if (val.IsNull()) {
			result_validity.SetInvalid(row);
			continue;
		}
		result_ptr[row] = StringVector::AddString(result, val.ToString());
	}
}

unique_ptr<FunctionData> CurrentSettingBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	auto &key_child = arguments[0];
	if (key_child->GetReturnType().id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}
	if (key_child->GetReturnType().id() != LogicalTypeId::VARCHAR) {
		throw ParserException("Key name for current_setting must be of type VARCHAR");
	}
	if (!key_child->IsFoldable()) {
		bound_function.SetFunctionCallback(CurrentSettingDynamic);
		bound_function.SetReturnType(LogicalType::VARCHAR);
		return nullptr;
	}
	Value key_val = ExpressionExecutor::EvaluateScalar(context, *key_child);
	D_ASSERT(key_val.type().id() == LogicalTypeId::VARCHAR);
	if (key_val.IsNull() || StringValue::Get(key_val).empty()) {
		throw ParserException("Key name for current_setting needs to be neither NULL nor empty");
	}

	auto key = StringUtil::Lower(StringValue::Get(key_val));
	Value val;
	if (!context.TryGetCurrentSetting(key, val)) {
		auto extension_name = Catalog::AutoloadExtensionByConfigName(context, key);
		// If autoloader didn't throw, the config is now available
		context.TryGetCurrentSetting(key, val);
	}

	val = Settings::FormatDisplayValue(context, val);
	bound_function.SetReturnType(val.type());
	return make_uniq<CurrentSettingBindData>(val);
}

} // namespace

ScalarFunction CurrentSettingFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::VARCHAR}, LogicalType::ANY, CurrentSettingFunction, CurrentSettingBind);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

} // namespace duckdb
