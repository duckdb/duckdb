#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

struct GetVariableBindData : FunctionData {
	explicit GetVariableBindData(Value value_p) : value(std::move(value_p)) {
	}

	Value value;

	bool Equals(const FunctionData &other_p) const override {
		const auto &other = other_p.Cast<GetVariableBindData>();
		return Value::NotDistinctFrom(value, other.value);
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<GetVariableBindData>(value);
	}
};

static unique_ptr<FunctionData> GetVariableBind(ClientContext &context, ScalarFunction &function,
                                                vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[0]->IsFoldable()) {
		throw NotImplementedException("getvariable requires a constant input");
	}
	if (arguments[0]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	Value value;
	auto variable_name = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (!variable_name.IsNull()) {
		ClientConfig::GetConfig(context).GetUserVariable(variable_name.ToString(), value);
	}
	function.return_type = value.type();
	return make_uniq<GetVariableBindData>(std::move(value));
}

unique_ptr<Expression> BindGetVariableExpression(FunctionBindExpressionInput &input) {
	if (!input.bind_data) {
		// unknown type
		throw InternalException("input.bind_data should be set");
	}
	auto &bind_data = input.bind_data->Cast<GetVariableBindData>();
	// emit a constant expression
	return make_uniq<BoundConstantExpression>(bind_data.value);
}

void GetVariableFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction getvar("getvariable", {LogicalType::VARCHAR}, LogicalType::ANY, nullptr, GetVariableBind, nullptr);
	getvar.bind_expression = BindGetVariableExpression;
	set.AddFunction(getvar);
}

} // namespace duckdb
